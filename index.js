const pg = require("pg");
const fs = require("fs");
const { promisify } = require("util");
const camelcase = require("lodash/camelcase");
const { GraphQLSchema, GraphQLObjectType, GraphQLString } = require("graphql");

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
    (spec, { process, extend }, { spec: { name } }) => {
      if (name !== "Query") {
        return;
      }
      return extend(spec, {
        hello: {
          type: GraphQLString,
          resolve() {
            return "World";
          },
        },
      });
    }
  );
};

const ColumnsPlugin = listener => {
  listener.on("fields", ({ name }) => name === "Query", typeSpec => {});
};

const defaultPlugins = [QueryPlugin, RowByPrimaryKeyPlugin, ColumnsPlugin];

const schemaFromPg = async (
  pgConfig,
  { schema, inflection = defaultInflection, plugins = defaultPlugins }
) => {
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

    const context = {
      introspectionResultsByKind,
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
    };

    const listener = {
      hooks: [],
      on(event, fn) {
        this.hooks[event] = this.hooks[event] || [];
        this.hooks[event].push(fn);
      },
      async applyHooks(event, spec, position) {
        let newSpec = spec;
        const hooks = this.hooks[event] || [];
        for (const hook of hooks) {
          const result = await hook(newSpec, context, position);
          if (result) {
            newSpec = result;
          }
        }
        return newSpec;
      },
    };
    for (const plugin of plugins) {
      plugin(listener);
    }
    return context.process(GraphQLSchema, {});
  });
};

exports.withPgClient = withPgClient;
exports.schemaFromPg = schemaFromPg;
