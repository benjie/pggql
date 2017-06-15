const pg = require("pg");
const fs = require("fs");
const { promisify } = require("util");
const camelcase = require("lodash/camelcase");
const { GraphQLSchema, GraphQLObjectType, GraphQLString } = require("graphql");
const debug = require("debug")("pggql");

const readFile = promisify(fs.readFile);

const INTROSPECTION_PATH = `${__dirname}/res/introspection-query.sql`;

const withPgClient = async (pgConfig = process.env.DATABASE_URL, fn) => {
  if (!fn) {
    throw new Error("Nothing to do!");
  }
  let releasePgClient = () => {};
  let pgClient;
  let result;
  try {
    if (pgConfig instanceof pg.Client) {
      pgClient = pgConfig;
    } else if (pgConfig instanceof pg.Pool) {
      pgClient = await pgConfig.connect();
      releasePgClient = () => pgClient.release();
    } else if (pgConfig === undefined || typeof pgConfig === "string") {
      pgClient = new pg.Client(pgConfig);
      pgClient.on("error", e => {
        console.error("pgClient error occurred: ", e);
      });
      releasePgClient = () => promisify(pgClient.end.bind(pgClient))();
      await promisify(pgClient.connect.bind(pgClient))();
    } else {
      throw new Error("You must provide a valid PG client configuration");
    }
    result = await fn(pgClient);
  } finally {
    try {
      await releasePgClient();
    } catch (e) {
      // Failed to release, assuming success
    }
  }
  return result;
};

const defaultInflection = {
  table: camelcase,
  column: camelcase,
  singleByKeys: (typeName, keys) =>
    camelcase(`${typeName}-by-${keys.join("-and-")}`), // postsByAuthorId
};

const QueryPlugin = listener => {
  listener.on("schema", async (spec, { process, extend }) => {
    const queryType = await process(GraphQLObjectType, {
      name: "Query",
      fields: {},
    });
    return extend(spec, {
      query: queryType,
    });
  });
};

const RowByPrimaryKeyPlugin = listener => {
  listener.on(
    "objectType:fields",
    (
      spec,
      {
        inflection,
        extend,
        pg: { gqlTypeByClassId, introspectionResultsByKind },
      },
      { spec: { name } }
    ) => {
      if (name !== "Query") {
        return;
      }
      return extend(
        spec,
        introspectionResultsByKind.class.reduce((memo, result) => {
          const type = gqlTypeByClassId[result.id];
          if (type) {
            memo[inflection.table(`random-${result.name}`)] = {
              type: type,
              resolve: () => ({}),
            };
          }
          return memo;
        }, {})
      );
    }
  );
};

const ColumnsPlugin = listener => {
  listener.on("fields", typeSpec => {});
};

const PgIntrospectionPlugin = (listener, { pg: { pgConfig, schema } }) => {
  listener.on("context", (context, { extend }) => {
    if (!Array.isArray(schema)) {
      throw new Error("Argument 'schema' (array) is required");
    }
    return withPgClient(pgConfig, async pgClient => {
      // Perform introspection
      const introspectionQuery = await readFile(INTROSPECTION_PATH, "utf8");
      const { rows } = await pgClient.query(introspectionQuery, [schema]);

      const introspectionResultsByKind = rows.reduce(
        (memo, { object }) => {
          memo[object.kind].push(object);
          return memo;
        },
        {
          namespace: [],
          class: [],
          attribute: [],
          type: [],
          constraint: [],
          procedure: [],
        }
      );

      return extend(context, {
        pg: {
          introspectionResultsByKind,
          gqlTypeByClassId: {},
        },
      });
    });
  });
};

const PgTablesPlugin = listener => {
  listener.on("context", async (context, { extend, process, inflection }) => {
    await Promise.all(
      context.pg.introspectionResultsByKind.class.map(async table => {
        context.pg.gqlTypeByClassId[
          table.id
        ] = await process(GraphQLObjectType, {
          name: inflection.table(table.name),
          fields: {
            hello: {
              type: GraphQLString,
              resolve() {
                return "World";
              },
            },
          },
        });
      })
    );
  });
};

const defaultPlugins = [
  PgIntrospectionPlugin,
  PgTablesPlugin,
  QueryPlugin,
  RowByPrimaryKeyPlugin,
  ColumnsPlugin,
];

const schemaFromPg = async (
  pgConfig,
  { schema, inflection = defaultInflection, plugins = defaultPlugins }
) => {
  const options = {
    pg: {
      pgConfig,
      schema,
    },
  };

  let hookCounter = 0;

  const listener = {
    context: {},
    hooks: [],
    on(event, fn) {
      this.hooks[event] = this.hooks[event] || [];
      this.hooks[event].push(fn);
    },
    async applyHooks(event, spec, position) {
      const thisCounter = ++hookCounter;
      debug(`Hook\t${thisCounter}\t[${event}] Running...`);
      let newSpec = spec;
      if (event === "context") {
        listener.context = newSpec;
      }
      const hooks = this.hooks[event] || [];
      for (const hook of hooks) {
        const result = await hook(newSpec, listener.context, position);
        if (result) {
          newSpec = result;
        }
        if (event === "context") {
          listener.context = newSpec;
        }
      }
      debug(`Hook\t${thisCounter}\t[${event}] Complete`);
      return newSpec;
    },
  };
  for (const plugin of plugins) {
    plugin(listener, options);
  }
  await listener.applyHooks("context", {
    inflection,
    extend(obj, obj2) {
      const keysA = Object.keys(obj);
      const keysB = Object.keys(obj2);
      for (const key of keysB) {
        if (keysA.includes(key)) {
          throw new Error(`Overwriting key '${key}' is not allowed!`);
        }
      }
      return Object.assign({}, obj, obj2);
    },
    async process(Type, spec) {
      let newSpec = spec;
      if (Type === GraphQLSchema) {
        newSpec = await listener.applyHooks("schema", newSpec, {
          spec: newSpec,
        });
      } else if (Type === GraphQLObjectType) {
        newSpec = await listener.applyHooks("objectType", newSpec, {
          spec: newSpec,
        });
        newSpec = Object.assign({}, newSpec, {
          fields: await listener.applyHooks(
            "objectType:fields",
            newSpec.fields,
            {
              spec: newSpec,
              fields: newSpec.fields,
            }
          ),
        });
      }
      return new Type(newSpec);
    },
  });
  return listener.context.process(GraphQLSchema, {});
};

exports.withPgClient = withPgClient;
exports.schemaFromPg = schemaFromPg;
