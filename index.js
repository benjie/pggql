const pg = require("pg");
const fs = require("fs");
const { promisify } = require("util");
const camelcase = require("lodash/camelcase");
const upperFirst = require("lodash/upperFirst");
const { Kind } = require("graphql/language");
const {
  GraphQLNonNull,
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLBoolean,
  GraphQLList,
  GraphQLScalarType,
  GraphQLEnumType,
} = require("graphql");
const debug = require("debug")("pggql");
const parseResolveInfo = require("./resolveInfo");
const pgSQLBuilder = require("./sql");
const GraphQLJSON = require("graphql-type-json");
const {
  GraphQLDate,
  GraphQLTime,
  GraphQLDateTime,
} = require("graphql-iso-date");

const GraphQLUUID = new GraphQLScalarType({
  name: "UUID",
  serialize: value => String(value),
  parseValue: value => String(value),
  parseLiteral: ast => {
    if (ast.kind !== Kind.STRING) {
      throw new Error("Can only parse string values");
    }
    return ast.value;
  },
});

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
  listener.on("schema", (spec, { buildWithHooks, extend }) => {
    const queryType = buildWithHooks(
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
                const tableAlias = Symbol();
                const fragments = [];
                for (const alias in fields) {
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
          gqlTypeByTypeId,
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
              type: nullableIf(
                !attr.isNotNull,
                gqlTypeByTypeId[attr.typeId] || GraphQLString
              ),
              resolve: (data, _args, _context, resolveInfo) => {
                const { alias } = parseResolveInfo(resolveInfo, {
                  deep: false,
                });
                return data[alias];
              },
            };
            return memo;
          }, {})
      );
    }
  );
};

const PgComputedColumnsPlugin = listener => {
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
          gqlTypeByTypeId,
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
      const tableType = introspectionResultsByKind.type.filter(
        type =>
          type.type === "c" &&
          type.category === "C" &&
          type.namespaceId === table.namespaceId &&
          type.classId === table.id
      )[0];
      if (!tableType) {
        throw new Error("Could not determine the type for this table");
      }
      return extend(
        fields,
        introspectionResultsByKind.procedure
          .filter(proc => proc.isStable)
          .filter(proc => proc.namespaceId === table.namespaceId)
          .filter(proc => proc.name.startsWith(`${table.name}_`))
          .filter(proc => proc.argTypeIds.length > 0)
          .filter(proc => proc.argTypeIds[0] === tableType.id)
          .reduce((memo, proc) => {
            if (proc.returnsSet) {
              // XXX: TODO!
              return memo;
            }
            /*
            proc =
              { kind: 'procedure',
                name: 'integration_webhook_secret',
                description: null,
                namespaceId: '6484381',
                isStrict: false,
                returnsSet: false,
                isStable: true,
                returnTypeId: '2950',
                argTypeIds: [ '6484569' ],
                argNames: [ 'integration' ],
                argDefaultsNum: 0 }
            */

            const fieldName = inflection.field(
              proc.name.substr(table.name.length + 1)
            );
            const schema = introspectionResultsByKind.namespace.filter(
              n => n.id === proc.namespaceId
            )[0];
            if (
              sqlFragmentGeneratorsByClassIdAndFieldName[table.id][fieldName]
            ) {
              console.warn(
                `WARNING: did not add dynamic column from function '${proc.name}' because field already exists`
              );
              return;
            }
            sqlFragmentGeneratorsByClassIdAndFieldName[table.id][fieldName] = (
              resolveInfoFragment,
              tableAlias
            ) => [
              {
                alias: resolveInfoFragment.alias,
                sqlFragment: sql.fragment`${sql.identifier(
                  schema.name,
                  proc.name
                )}(${sql.identifier(tableAlias)})`,
              },
            ];
            memo[fieldName] = {
              type: gqlTypeByTypeId[proc.returnTypeId] || GraphQLString,
              resolve: (data, _args, _context, resolveInfo) => {
                const { alias } = parseResolveInfo(resolveInfo, {
                  deep: false,
                });
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

    listener.on("context", (context, { extend }) => {
      if (!Array.isArray(schema)) {
        throw new Error("Argument 'schema' (array) is required");
      }

      return extend(context, {
        pg: {
          introspectionResultsByKind,
          gqlTypeByClassId: {},
          gqlTypeByTypeId: {},
          sqlFragmentGeneratorsByClassIdAndFieldName: {},
          sql: pgSQLBuilder,
        },
      });
    });
  });
};

const PgTablesPlugin = listener => {
  listener.on(
    "context",
    (
      context,
      {
        buildWithHooks,
        inflection,
        pg: { introspectionResultsByKind, gqlTypeByTypeId },
      }
    ) => {
      context.pg.introspectionResultsByKind.class.map(table => {
        /*
        table =
          { kind: 'class',
            id: '6484790',
            name: 'bundle',
            description: null,
            namespaceId: '6484381',
            typeId: '6484792',
            isSelectable: true,
            isInsertable: true,
            isUpdatable: true,
            isDeletable: true }
        */
        context.pg.sqlFragmentGeneratorsByClassIdAndFieldName[table.id] = {};
        context.pg.gqlTypeByClassId[table.id] = buildWithHooks(
          GraphQLObjectType,
          {
            name: inflection.table(table.name),
            fields: {},
          },
          {
            pg: {
              introspection: table,
            },
          }
        );
        const tableType = introspectionResultsByKind.type.filter(
          type =>
            type.type === "c" &&
            type.category === "C" &&
            type.namespaceId === table.namespaceId &&
            type.classId === table.id
        )[0];
        if (!tableType) {
          throw new Error("Could not determine the type for this table");
        }
        context.pg.gqlTypeByTypeId[tableType.id] =
          context.pg.gqlTypeByClassId[table.id];
      });
    }
  );
};

const PgTypesPlugin = listener => {
  listener.on(
    "context",
    (context, { buildWithHooks, inflection, pg: { gqlTypeByTypeId } }) => {
      /*
      type =
        { kind: 'type',
          id: '1021',
          name: '_float4',
          description: null,
          namespaceId: '11',
          namespaceName: 'pg_catalog',
          type: 'b',
          category: 'A',
          domainIsNotNull: false,
          arrayItemTypeId: '700',
          classId: null,
          domainBaseTypeId: null,
          enumVariants: null,
          rangeSubTypeId: null }
      */
      const pgTypeById = context.pg.introspectionResultsByKind.type.reduce(
        (memo, type) => {
          memo[type.id] = type;
          return memo;
        },
        {}
      );
      const categoryLookup = {
        B: () => GraphQLBoolean,
        N: () => GraphQLFloat,
        A: type =>
          new GraphQLList(
            new GraphQLNonNull(
              enforceGqlTypeByPgType(pgTypeById[type.arrayItemTypeId])
            )
          ),
      };
      /*
        Determined by running:

          select oid, typname, typarray, typcategory, typtype from pg_catalog.pg_type where typtype = 'b' order by oid;

        We only need to add oidLookups for types that don't have the correct fallback
      */
      const oidLookup = {
        20: GraphQLFloat, // Even though this is int8, it's too big for JS int, so cast to float (or string?).
        21: GraphQLInt,
        23: GraphQLInt,
        114: GraphQLJSON,
        3802: GraphQLJSON,
        2950: GraphQLUUID,
        1082: GraphQLDate, // date
        1114: GraphQLDateTime, // timestamp
        1184: GraphQLDateTime, // timestamptz
        1083: GraphQLTime, // time
        1266: GraphQLTime, // timetz
        // 1186 interval
      };
      const enforceGqlTypeByPgType = type => {
        // Explicit overrides
        if (!gqlTypeByTypeId[type.id]) {
          const gqlType = oidLookup[type.id];
          if (gqlType) {
            gqlTypeByTypeId[type.id] = gqlType;
          }
        }
        // Enums
        if (!gqlTypeByTypeId[type.id] && type.typtype === "e") {
          gqlTypeByTypeId[type.id] = new GraphQLEnumType({
            name: upperFirst(camelcase(`${type.name}-enum`)),
            values: type.enumVariants,
            description: type.description,
          });
        }
        // Fall back to categories
        if (!gqlTypeByTypeId[type.id]) {
          const gen = categoryLookup[type.category];
          if (gen) {
            gqlTypeByTypeId[type.id] = gen(type);
          }
        }
        // Nothing else worked; pass through as string!
        if (!gqlTypeByTypeId[type.id]) {
          gqlTypeByTypeId[type.id] = GraphQLString;
        }
        return gqlTypeByTypeId[type.id];
      };

      context.pg.introspectionResultsByKind.type
        .filter(type => true)
        .forEach(enforceGqlTypeByPgType);
    }
  );
};

const RandomFieldPlugin = async listener => {
  listener.on("objectType:fields", (fields, { extend }) => {
    return extend(fields, {
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
    });
  });
};

const defaultPlugins = [
  PgIntrospectionPlugin,
  PgTablesPlugin,
  PgTypesPlugin,
  QueryPlugin,
  RowByPrimaryKeyPlugin,
  PgColumnsPlugin,
  PgComputedColumnsPlugin,
  RandomFieldPlugin,
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
    applyHooks(event, spec, position) {
      const thisCounter = ++hookCounter;
      debug(`Hook\t${thisCounter}\t[${event}] Running...`);
      let newSpec = spec;
      if (event === "context") {
        listener.context = newSpec;
      }
      const hooks = this.hooks[event] || [];
      for (const hook of hooks) {
        const result = hook(newSpec, listener.context, position);
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
    await plugin(listener, options);
  }
  listener.applyHooks("context", {
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
    buildWithHooks(Type, spec, scope = {}) {
      let newSpec = spec;
      if (Type === GraphQLSchema) {
        newSpec = listener.applyHooks("schema", newSpec, {
          spec: newSpec,
          scope,
        });
      } else if (Type === GraphQLObjectType) {
        newSpec = listener.applyHooks("objectType", newSpec, {
          spec: newSpec,
          scope,
        });
        newSpec = Object.assign({}, newSpec, {
          fields: () =>
            listener.applyHooks("objectType:fields", newSpec.fields, {
              spec: newSpec,
              fields: newSpec.fields,
              scope,
              Self,
            }),
        });
      }
      const Self = new Type(newSpec);
      return Self;
    },
  });
  return listener.context.buildWithHooks(GraphQLSchema, {});
};

exports.withPgClient = withPgClient;
exports.schemaFromPg = schemaFromPg;
