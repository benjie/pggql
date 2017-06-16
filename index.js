const pg = require("pg");
const fs = require("fs");
const { promisify } = require("util");
const camelcase = require("lodash/camelcase");
const upperFirst = require("lodash/upperFirst");
const {
  GraphQLNonNull,
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLString,
  GraphQLInt,
} = require("graphql");
const debug = require("debug")("pggql");
const parseResolveInfo = require("./resolveInfo");
const pgSQLBuilder = require("./sql");

const readFile = promisify(fs.readFile);

const INTROSPECTION_PATH = `${__dirname}/res/introspection-query.sql`;

const nullableIf = (condition, Type) =>
  condition ? Type : new GraphQLNonNull(Type);

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
  table: str => upperFirst(camelcase(str)),
  field: camelcase,
  singleByKeys: (typeName, keys) =>
    camelcase(`${typeName}-by-${keys.join("-and-")}`), // postsByAuthorId
};

const QueryPlugin = listener => {
  listener.on("schema", async (spec, { buildWithHooks, extend }) => {
    const queryType = await buildWithHooks(
      GraphQLObjectType,
      {
        name: "Query",
        fields: {},
      },
      { isRootQuery: true }
    );
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
        pg: {
          gqlTypeByClassId,
          introspectionResultsByKind,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sql,
        },
      },
      { scope: { isRootQuery } }
    ) => {
      if (!isRootQuery) {
        return;
      }
      return extend(
        spec,
        introspectionResultsByKind.class.reduce((memo, table) => {
          const type = gqlTypeByClassId[table.id];
          const schema = introspectionResultsByKind.namespace.filter(
            n => n.id === table.namespaceId
          )[0];
          if (!schema) {
            console.warn(
              `Could not find the schema for table '${table.name}'; skipping`
            );
            return memo;
          }
          const sqlFullTableName = sql.identifier(schema.name, table.name);
          if (type) {
            memo[inflection.field(`random-${table.name}`)] = {
              type: type,
              async resolve(parent, args, { pgClient }, resolveInfo) {
                const { alias, fields } = parseResolveInfo(resolveInfo);
                console.dir(fields);
                const tableAlias = Symbol();
                const fragments = [];
                console.log(
                  sqlFragmentGeneratorsByClassIdAndFieldName[table.id]
                );
                for (const alias in fields) {
                  console.log(alias);
                  const spec = fields[alias];
                  const generator =
                    sqlFragmentGeneratorsByClassIdAndFieldName[table.id][
                      spec.name
                    ];
                  if (generator) {
                    const generatedFrags = generator(spec, tableAlias);
                    if (!Array.isArray(generatedFrags)) {
                      throw new Error(
                        "sqlFragmentGeneratorsByClassIdAndFieldName generators must generate arrays"
                      );
                    }
                    fragments.push(...generatedFrags);
                  }
                }
                console.dir(fragments);
                const query = sql.query`
                  select 
                    ${sql.join(
                      fragments.map(
                        ({ sqlFragment, alias }) =>
                          sql.fragment`${sqlFragment} as ${sql.identifier(
                            alias
                          )}`
                      ),
                      ", "
                    )}
                  from ${sqlFullTableName} as ${sql.identifier(
                  tableAlias
                )} order by random() limit 1;
                `;
                const { text, values } = sql.compile(query);
                console.log(text);
                const { rows: [row] } = await pgClient.query(text, values);
                return row;
              },
            };
          }
          return memo;
        }, {})
      );
    }
  );
};

const PgColumnsPlugin = listener => {
  listener.on(
    "objectType:fields",
    (
      fields,
      {
        inflection,
        extend,
        pg: {
          introspectionResultsByKind,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sql,
        },
      },
      { scope }
    ) => {
      if (
        !scope.pg ||
        !scope.pg.introspection ||
        scope.pg.introspection.kind !== "class"
      ) {
        return;
      }
      const table = scope.pg.introspection;
      return extend(
        fields,
        introspectionResultsByKind.attribute
          .filter(attr => attr.classId === table.id)
          .reduce((memo, attr) => {
            /*
            attr =
              { kind: 'attribute',
                classId: '6546809',
                num: 21,
                name: 'upstreamName',
                description: null,
                typeId: '6484393',
                isNotNull: false,
                hasDefault: false }
            */
            const fieldName = inflection.field(`${attr.name}`);
            sqlFragmentGeneratorsByClassIdAndFieldName[table.id][fieldName] = (
              resolveInfoFragment,
              tableAlias
            ) => [
              {
                alias: resolveInfoFragment.alias,
                sqlFragment: sql.identifier(tableAlias, attr.name),
              },
            ];
            memo[fieldName] = {
              type: nullableIf(!attr.isNotNull, GraphQLString),
              resolve: (data, _args, _context, resolveInfo) => {
                const { alias } = parseResolveInfo(resolveInfo, {
                  deep: false,
                });
                console.log("!!!");
                console.dir(data);
                console.log(alias);
                console.log(resolveInfo);
                return data[alias];
              },
            };
            return memo;
          }, {})
      );
    }
  );
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
          sqlFragmentGeneratorsByClassIdAndFieldName: {},
          sql: pgSQLBuilder,
        },
      });
    });
  });
};

const PgTablesPlugin = listener => {
  listener.on("context", async (context, { buildWithHooks, inflection }) => {
    await Promise.all(
      context.pg.introspectionResultsByKind.class.map(async table => {
        context.pg.sqlFragmentGeneratorsByClassIdAndFieldName[table.id] = {};
        context.pg.gqlTypeByClassId[table.id] = await buildWithHooks(
          GraphQLObjectType,
          {
            name: inflection.table(table.name),
            fields: {
              random: {
                type: GraphQLInt,
                args: {
                  sides: {
                    type: GraphQLInt,
                  },
                },
                resolve(_, { sides }) {
                  return Math.floor(Math.random() * sides) + 1;
                },
              },
            },
          },
          {
            pg: {
              introspection: table,
            },
          }
        );
      })
    );
  });
};

const defaultPlugins = [
  PgIntrospectionPlugin,
  PgTablesPlugin,
  QueryPlugin,
  RowByPrimaryKeyPlugin,
  PgColumnsPlugin,
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
    async buildWithHooks(Type, spec, scope = {}) {
      let newSpec = spec;
      if (Type === GraphQLSchema) {
        newSpec = await listener.applyHooks("schema", newSpec, {
          spec: newSpec,
          scope,
        });
      } else if (Type === GraphQLObjectType) {
        newSpec = await listener.applyHooks("objectType", newSpec, {
          spec: newSpec,
          scope,
        });
        newSpec = Object.assign({}, newSpec, {
          fields: await listener.applyHooks(
            "objectType:fields",
            newSpec.fields,
            {
              spec: newSpec,
              fields: newSpec.fields,
              scope,
            }
          ),
        });
      }
      return new Type(newSpec);
    },
  });
  return listener.context.buildWithHooks(GraphQLSchema, {});
};

exports.withPgClient = withPgClient;
exports.schemaFromPg = schemaFromPg;
