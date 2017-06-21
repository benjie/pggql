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
const pluralize = require("pluralize");

const stringType = name =>
  new GraphQLScalarType({
    name,
    serialize: value => String(value),
    parseValue: value => String(value),
    parseLiteral: ast => {
      if (ast.kind !== Kind.STRING) {
        throw new Error("Can only parse string values");
      }
      return ast.value;
    },
  });
const Cursor = stringType("Cursor");
const GraphQLUUID = stringType("UUID");

const readFile = promisify(fs.readFile);

const INTROSPECTION_PATH = `${__dirname}/res/introspection-query.sql`;

const nullableIf = (condition, Type) =>
  condition ? Type : new GraphQLNonNull(Type);

const sqlJsonBuildObjectFromFragments = fragments => {
  const sql = pgSQLBuilder;
  return sql.fragment`
    json_build_object(
      ${sql.join(
        fragments.map(
          ({ sqlFragment, alias }) =>
            sql.fragment`${sql.literal(alias)}, ${sqlFragment}`
        ),
        ",\n"
      )}
    )`;
};

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
  edge: name => upperFirst(camelcase(`${name}-edge`)),
  connection: name => upperFirst(camelcase(`${name}-connection`)),
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

const PgRowByUniqueConstraint = listener => {
  listener.on(
    "objectType:fields",
    (
      spec,
      {
        inflection,
        extend,
        pg: {
          gqlTypeByClassId,
          gqlTypeByTypeId,
          introspectionResultsByKind,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sql,
          generateFieldFragments,
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
            const uniqueConstraints = introspectionResultsByKind.constraint
              .filter(con => con.classId === table.id)
              .filter(con => ["u", "p"].includes(con.type));
            const attributes = introspectionResultsByKind.attribute
              .filter(attr => attr.classId === table.id)
              .sort((a, b) => a.num - b.num);
            uniqueConstraints.forEach(constraint => {
              const keys = constraint.keyAttributeNums.map(
                num => attributes.filter(attr => attr.num === num)[0]
              );
              if (!keys.every(_ => _)) {
                throw new Error(
                  "Consistency error: could not find an attribute!"
                );
              }
              memo[
                inflection.field(
                  `${table.name}-by-${keys.map(key => key.name).join("-and-")}`
                )
              ] = {
                type: type,
                args: keys.reduce((memo, key) => {
                  memo[inflection.field(key.name)] = {
                    type: gqlTypeByTypeId[key.typeId],
                  };
                  return memo;
                }, {}),
                async resolve(parent, args, { pgClient }, resolveInfo) {
                  const parsedResolveInfoFragment = parseResolveInfo(
                    resolveInfo
                  );
                  const { alias, fields } = parsedResolveInfoFragment;
                  const tableAlias = Symbol();
                  const conditions = keys.map(
                    key =>
                      sql.fragment`${sql.identifier(
                        tableAlias,
                        key.name
                      )} = ${sql.value(args[inflection.field(key.name)])}`
                  );
                  const fragments = generateFieldFragments(
                    parsedResolveInfoFragment,
                    sqlFragmentGeneratorsByClassIdAndFieldName[table.id],
                    tableAlias
                  );
                  const sqlFields = sql.join(
                    fragments.map(
                      ({ sqlFragment, alias }) =>
                        sql.fragment`${sqlFragment} as ${sql.identifier(alias)}`
                    ),
                    ", "
                  );
                  const primaryKeyConstraint = introspectionResultsByKind.constraint
                    .filter(con => con.classId === table.id)
                    .filter(con => ["p"].includes(con.type))[0];
                  const attributes = introspectionResultsByKind.attribute
                    .filter(attr => attr.classId === table.id)
                    .sort((a, b) => a.num - b.num);
                  const primaryKeys =
                    primaryKeyConstraint &&
                    primaryKeyConstraint.keyAttributeNums.map(
                      num => attributes.filter(attr => attr.num === num)[0]
                    );
                  const query = sql.query`
                      select ${sqlFields}
                      from ${sqlFullTableName} as ${sql.identifier(tableAlias)} 
                      where (${sql.join(conditions, ") and (")})
                      order by ${primaryKeys
                        ? sql.join(
                            primaryKeys.map(
                              key =>
                                sql.fragment`${sql.identifier(
                                  tableAlias,
                                  key.name
                                )} asc`
                            ),
                            ", "
                          )
                        : sql.literal(1)}
                    `;
                  const { text, values } = sql.compile(query);
                  const { rows: [row] } = await pgClient.query(text, values);
                  return row;
                },
              };
            });
          }
          return memo;
        }, {})
      );
    }
  );
};

const PgAllRows = listener => {
  listener.on(
    "objectType:fields",
    (
      spec,
      {
        inflection,
        extend,
        pg: {
          gqlTypeByClassId,
          gqlTypeByTypeId,
          gqlConnectionTypeByClassId,
          introspectionResultsByKind,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sqlFragmentGeneratorsForConnectionByClassId,
          sql,
          generateFieldFragments,
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
          const connectionType = gqlConnectionTypeByClassId[table.id];
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
          if (type && connectionType) {
            memo[inflection.field(`all-${pluralize(table.name)}`)] = {
              type: connectionType,
              args: {},
              async resolve(parent, args, { pgClient }, resolveInfo) {
                const parsedResolveInfoFragment = parseResolveInfo(resolveInfo);
                const { alias, fields } = parsedResolveInfoFragment;
                const tableAlias = Symbol();
                const fragments = generateFieldFragments(
                  parsedResolveInfoFragment,
                  sqlFragmentGeneratorsForConnectionByClassId[table.id],
                  tableAlias
                );
                const sqlFields = sql.join(
                  fragments.map(
                    ({ sqlFragment, alias }) =>
                      sql.fragment`${sqlFragment} as ${sql.identifier(alias)}`
                  ),
                  ", "
                );
                const primaryKeyConstraint = introspectionResultsByKind.constraint
                  .filter(con => con.classId === table.id)
                  .filter(con => ["p"].includes(con.type))[0];
                const attributes = introspectionResultsByKind.attribute
                  .filter(attr => attr.classId === table.id)
                  .sort((a, b) => a.num - b.num);
                const primaryKeys =
                  primaryKeyConstraint &&
                  primaryKeyConstraint.keyAttributeNums.map(
                    num => attributes.filter(attr => attr.num === num)[0]
                  );
                const query = sql.query`
                    select ${sqlFields}
                    from ${sqlFullTableName} as ${sql.identifier(tableAlias)}
                    order by ${primaryKeys
                      ? sql.join(
                          primaryKeys.map(
                            key =>
                              sql.fragment`${sql.identifier(
                                tableAlias,
                                key.name
                              )} asc`
                          ),
                          ", "
                        )
                      : sql.literal(1)}
                  `;
                const { text, values } = sql.compile(query);
                const { rows } = await pgClient.query(text, values);
                return rows;
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
        !scope.pg.isRowType ||
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

const PgForwardRelationPlugin = listener => {
  listener.on(
    "objectType:fields",
    (
      fields,
      {
        inflection,
        extend,
        pg: {
          gqlTypeByTypeId,
          gqlTypeByClassId,
          introspectionResultsByKind,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sql,
          generateFieldFragments,
        },
      },
      { scope }
    ) => {
      if (
        !scope.pg ||
        !scope.pg.isRowType ||
        !scope.pg.introspection ||
        scope.pg.introspection.kind !== "class"
      ) {
        return;
      }
      const table = scope.pg.introspection;
      // This is a relation in which we (table) are local, and there's a foreign table

      const foreignKeyConstraints = introspectionResultsByKind.constraint
        .filter(con => ["f"].includes(con.type))
        .filter(con => con.classId === table.id);
      const attributes = introspectionResultsByKind.attribute
        .filter(attr => attr.classId === table.id)
        .sort((a, b) => a.num - b.num);

      return extend(
        fields,
        foreignKeyConstraints.reduce((memo, constraint) => {
          const gqlTableType = gqlTypeByClassId[constraint.classId];
          if (!gqlTableType) {
            console.warn(
              `Could not determine type for table with id ${constraint.classId}`
            );
            return memo;
          }
          const gqlForeignTableType =
            gqlTypeByClassId[constraint.foreignClassId];
          if (!gqlForeignTableType) {
            console.warn(
              `Could not determine type for foreign table with id ${constraint.foreignClassId}`
            );
            return memo;
          }
          const foreignTable = introspectionResultsByKind.class.filter(
            cls => cls.id === constraint.foreignClassId
          )[0];
          if (!foreignTable) {
            throw new Error(
              `Could not find the foreign table (constraint: ${constraint.name})`
            );
          }
          const foreignSchema = introspectionResultsByKind.namespace.filter(
            n => n.id === foreignTable.namespaceId
          )[0];
          const foreignAttributes = introspectionResultsByKind.attribute
            .filter(attr => attr.classId === constraint.foreignClassId)
            .sort((a, b) => a.num - b.num);

          const keys = constraint.keyAttributeNums.map(
            num => attributes.filter(attr => attr.num === num)[0]
          );
          const foreignKeys = constraint.foreignKeyAttributeNums.map(
            num => foreignAttributes.filter(attr => attr.num === num)[0]
          );
          if (!keys.every(_ => _) || !foreignKeys.every(_ => _)) {
            throw new Error("Could not find key columns!");
          }

          const fieldName = inflection.field(
            `${foreignTable.name}-by-${keys.map(k => k.name).join("-and-")}`
          );

          sqlFragmentGeneratorsByClassIdAndFieldName[table.id][fieldName] = (
            parsedResolveInfoFragment,
            tableAlias
          ) => {
            const foreignTableAlias = Symbol();
            const conditions = keys.map(
              (key, i) =>
                sql.fragment`${sql.identifier(
                  tableAlias,
                  key.name
                )} = ${sql.identifier(foreignTableAlias, foreignKeys[i].name)}`
            );
            const fragments = generateFieldFragments(
              parsedResolveInfoFragment,
              sqlFragmentGeneratorsByClassIdAndFieldName[foreignTable.id],
              foreignTableAlias
            );
            return [
              {
                alias: parsedResolveInfoFragment.alias,
                sqlFragment: sql.fragment`
                  (
                    select ${sqlJsonBuildObjectFromFragments(fragments)}
                    from ${sql.identifier(
                      foreignSchema.name,
                      foreignTable.name
                    )} as ${sql.identifier(foreignTableAlias)}
                    where (${sql.join(conditions, ") and (")})
                  )
                `,
              },
            ];
          };
          memo[fieldName] = {
            type: nullableIf(
              !keys.every(key => key.isNotNull),
              gqlForeignTableType
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

const PgBackwardRelationPlugin = listener => {
  listener.on(
    "objectType:fields",
    (
      fields,
      {
        inflection,
        extend,
        pg: {
          gqlTypeByTypeId,
          gqlTypeByClassId,
          introspectionResultsByKind,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sql,
          generateFieldFragments,
        },
      },
      { scope }
    ) => {
      if (
        !scope.pg ||
        !scope.pg.isRowType ||
        !scope.pg.introspection ||
        scope.pg.introspection.kind !== "class"
      ) {
        return;
      }
      // This is a relation in which WE are foreign
      const foreignTable = scope.pg.introspection;

      const foreignKeyConstraints = introspectionResultsByKind.constraint
        .filter(con => ["f"].includes(con.type))
        .filter(con => con.foreignClassId === foreignTable.id);
      const foreignAttributes = introspectionResultsByKind.attribute
        .filter(attr => attr.classId === foreignTable.id)
        .sort((a, b) => a.num - b.num);

      return extend(
        fields,
        foreignKeyConstraints.reduce((memo, constraint) => {
          const gqlTableType = gqlTypeByClassId[constraint.classId];
          if (!gqlTableType) {
            console.warn(
              `Could not determine type for table with id ${constraint.classId}`
            );
            return memo;
          }
          const gqlForeignTableType =
            gqlTypeByClassId[constraint.foreignClassId];
          if (!gqlForeignTableType) {
            console.warn(
              `Could not determine type for foreign table with id ${constraint.foreignClassId}`
            );
            return memo;
          }
          const table = introspectionResultsByKind.class.filter(
            cls => cls.id === constraint.classId
          )[0];
          if (!table) {
            throw new Error(
              `Could not find the table that referenced us (constraint: ${constraint.name})`
            );
          }
          const schema = introspectionResultsByKind.namespace.filter(
            n => n.id === table.namespaceId
          )[0];

          const attributes = introspectionResultsByKind.attribute
            .filter(attr => attr.classId === constraint.classId)
            .sort((a, b) => a.num - b.num);

          const keys = constraint.keyAttributeNums.map(
            num => attributes.filter(attr => attr.num === num)[0]
          );
          const foreignKeys = constraint.foreignKeyAttributeNums.map(
            num => foreignAttributes.filter(attr => attr.num === num)[0]
          );
          if (!keys.every(_ => _) || !foreignKeys.every(_ => _)) {
            throw new Error("Could not find key columns!");
          }

          const fieldName = inflection.field(
            `${pluralize(table.name)}-by-${keys.map(k => k.name).join("-and-")}`
          );

          sqlFragmentGeneratorsByClassIdAndFieldName[foreignTable.id][
            fieldName
          ] = (parsedResolveInfoFragment, foreignTableAlias) => {
            const tableAlias = Symbol();
            const conditions = keys.map(
              (key, i) =>
                sql.fragment`${sql.identifier(
                  tableAlias,
                  key.name
                )} = ${sql.identifier(foreignTableAlias, foreignKeys[i].name)}`
            );
            const fragments = generateFieldFragments(
              parsedResolveInfoFragment,
              sqlFragmentGeneratorsByClassIdAndFieldName[table.id],
              tableAlias
            );
            const primaryKeyConstraint = introspectionResultsByKind.constraint
              .filter(con => con.classId === table.id)
              .filter(con => ["p"].includes(con.type))[0];
            const attributes = introspectionResultsByKind.attribute
              .filter(attr => attr.classId === table.id)
              .sort((a, b) => a.num - b.num);
            const primaryKeys =
              primaryKeyConstraint &&
              primaryKeyConstraint.keyAttributeNums.map(
                num => attributes.filter(attr => attr.num === num)[0]
              );
            return [
              {
                alias: parsedResolveInfoFragment.alias,
                sqlFragment: sql.fragment`
                  (
                    select json_agg(${sqlJsonBuildObjectFromFragments(
                      fragments
                    )})
                    from ${sql.identifier(
                      schema.name,
                      table.name
                    )} as ${sql.identifier(tableAlias)}
                    where (${sql.join(conditions, ") and (")})
                    order by ${primaryKeys
                      ? sql.join(
                          primaryKeys.map(
                            key =>
                              sql.fragment`${sql.identifier(
                                tableAlias,
                                key.name
                              )} asc`
                          ),
                          ", "
                        )
                      : sql.literal(1)}
                  )
                `,
              },
            ];
          };
          memo[fieldName] = {
            type: nullableIf(
              !keys.every(key => key.isNotNull),
              new GraphQLList(new GraphQLNonNull(gqlTableType))
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
          generateFieldFragments,
        },
      },
      { scope }
    ) => {
      if (
        !scope.pg ||
        !scope.pg.isRowType ||
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

            // XXX: add args!

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
                `WARNING: did not add dynamic column '${fieldName}' from function '${proc.name}' because field already exists`
              );
              return memo;
            }

            const returnType = introspectionResultsByKind.type.filter(
              type => type.id === proc.returnTypeId
            )[0];
            const returnTypeTable = introspectionResultsByKind.class.filter(
              cls => cls.id === returnType.classId
            )[0];
            if (!returnType) {
              throw new Error(
                `Could not determine return type for function '${proc.name}'`
              );
            }

            sqlFragmentGeneratorsByClassIdAndFieldName[table.id][fieldName] = (
              parsedResolveInfoFragment,
              foreignTableAlias
            ) => {
              const sqlCall = sql.fragment`${sql.identifier(
                schema.name,
                proc.name
              )}(${sql.identifier(foreignTableAlias)})`;

              const isTable = returnType.type === "c" && returnTypeTable;

              const functionAlias = Symbol();
              const getFragments = () =>
                generateFieldFragments(
                  parsedResolveInfoFragment,
                  sqlFragmentGeneratorsByClassIdAndFieldName[
                    returnTypeTable.id
                  ],
                  functionAlias
                );
              const sqlFragment = isTable
                ? sql.query`(
                  select ${sqlJsonBuildObjectFromFragments(getFragments())}
                  from ${sqlCall} as ${sql.identifier(functionAlias)}
                )`
                : sqlCall;
              return [
                {
                  alias: parsedResolveInfoFragment.alias,
                  sqlFragment,
                },
              ];
            };
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

const PgIntrospectionPlugin = (listener, { pg: { pgConfig, schemas } }) => {
  return withPgClient(pgConfig, async pgClient => {
    // Perform introspection
    if (!Array.isArray(schemas)) {
      throw new Error("Argument 'schemas' (array) is required");
    }
    const introspectionQuery = await readFile(INTROSPECTION_PATH, "utf8");
    const { rows } = await pgClient.query(introspectionQuery, [schemas]);

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
      const sql = pgSQLBuilder;
      return extend(context, {
        pg: {
          introspectionResultsByKind,
          gqlTypeByClassId: {},
          gqlEdgeTypeByClassId: {},
          gqlConnectionTypeByClassId: {},
          gqlTypeByTypeId: {},
          sqlFragmentGeneratorsByClassIdAndFieldName: {},
          sqlFragmentGeneratorsForConnectionByClassId: {},
          sql,
          generateFieldFragments(
            parsedResolveInfoFragment,
            sqlFragmentGenerators,
            tableAlias
          ) {
            const { fields } = parsedResolveInfoFragment;
            const fragments = [];
            for (const alias in fields) {
              const spec = fields[alias];
              const generator = sqlFragmentGenerators[spec.name];
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
            return fragments;
          },
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
        pg: {
          sql,
          introspectionResultsByKind,
          gqlTypeByTypeId,
          sqlFragmentGeneratorsByClassIdAndFieldName,
          sqlFragmentGeneratorsForConnectionByClassId,
          generateFieldFragments,
        },
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
              isRowType: true,
            },
          }
        );

        const edgeFragmentGenerators = {};
        const addEdgeFragmentGenerator = (field, generator) => {
          edgeFragmentGenerators[field] = generator;
        };

        const schema = introspectionResultsByKind.namespace.filter(
          n => n.id === table.namespaceId
        )[0];
        const primaryKeyConstraint = introspectionResultsByKind.constraint
          .filter(con => con.classId === table.id)
          .filter(con => ["p"].includes(con.type))[0];
        const attributes = introspectionResultsByKind.attribute
          .filter(attr => attr.classId === table.id)
          .sort((a, b) => a.num - b.num);
        const primaryKeys =
          primaryKeyConstraint &&
          primaryKeyConstraint.keyAttributeNums.map(
            num => attributes.filter(attr => attr.num === num)[0]
          );

        addEdgeFragmentGenerator(
          "node",
          (parsedResolveInfoFragment, tableAlias) => {
            return generateFieldFragments(
              parsedResolveInfoFragment,
              sqlFragmentGeneratorsByClassIdAndFieldName[table.id],
              tableAlias
            );
          }
        );
        addEdgeFragmentGenerator(
          "cursor",
          (parsedResolveInfoFragment, tableAlias) => {
            if (!primaryKeys) {
              return [];
            }
            return [
              {
                alias: "__cursor",
                sqlFragment: sql.fragment`encode(json_build_array('pg', ${sql.literal(
                  schema.name
                )}, ${sql.literal(table.name)}, ${sql.join(
                  primaryKeys.map(attr =>
                    sql.identifier(tableAlias, attr.name)
                  ),
                  ", "
                )})::bytea, 'base64')`,
              },
            ];
          }
        );
        context.pg.gqlEdgeTypeByClassId[table.id] = buildWithHooks(
          GraphQLObjectType,
          {
            name: inflection.edge(table.name),
            fields: {
              cursor: {
                type: Cursor,
                resolve(data) {
                  return data.__cursor;
                },
              },
              node: {
                type: new GraphQLNonNull(context.pg.gqlTypeByClassId[table.id]),
                resolve(data) {
                  return data;
                },
              },
            },
          },
          {
            isEdgeType: true,
            nodeType: context.pg.gqlTypeByClassId[table.id],
            pg: {
              introspection: table,
            },
          }
        );
        const connectionFragmentGenerators = {};
        sqlFragmentGeneratorsForConnectionByClassId[
          table.id
        ] = connectionFragmentGenerators;
        const addConnectionFragmentGenerator = (field, generator) => {
          connectionFragmentGenerators[field] = generator;
        };
        addConnectionFragmentGenerator(
          "edges",
          (parsedResolveInfoFragment, tableAlias) => {
            return generateFieldFragments(
              parsedResolveInfoFragment,
              edgeFragmentGenerators,
              tableAlias
            );
          }
        );
        addConnectionFragmentGenerator(
          "nodes",
          (parsedResolveInfoFragment, tableAlias) => {
            return generateFieldFragments(
              parsedResolveInfoFragment,
              sqlFragmentGeneratorsByClassIdAndFieldName[table.id],
              tableAlias
            );
          }
        );
        context.pg.gqlConnectionTypeByClassId[table.id] = buildWithHooks(
          GraphQLObjectType,
          {
            name: inflection.connection(table.name),
            fields: {
              // XXX: pageInfo
              // XXX: totalCount
              nodes: {
                type: new GraphQLList(context.pg.gqlTypeByClassId[table.id]),
                resolve(data) {
                  return data;
                },
              },
              edges: {
                type: new GraphQLList(
                  new GraphQLNonNull(context.pg.gqlEdgeTypeByClassId[table.id])
                ),
                resolve(data) {
                  return data;
                },
              },
            },
          },
          {
            isConnectionType: true,
            edgeType: context.pg.gqlEdgeTypeByClassId[table.id],
            nodeType: context.pg.gqlTypeByClassId[table.id],
            pg: {
              introspection: table,
              addFragmentGenerator: addConnectionFragmentGenerator,
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

const PgTypesPlugin = ({ extended = true } = {}) => listener => {
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
  PgTypesPlugin(),
  QueryPlugin,
  PgRowByUniqueConstraint,
  PgAllRows,
  PgColumnsPlugin,
  PgComputedColumnsPlugin,
  RandomFieldPlugin,
  PgForwardRelationPlugin,
  PgBackwardRelationPlugin,
];

const schemaFromPg = async (
  pgConfig,
  { schemas, inflection = defaultInflection, plugins = defaultPlugins }
) => {
  const options = {
    pg: {
      pgConfig,
      schemas,
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
        const rawSpec = newSpec;
        newSpec = Object.assign({}, newSpec, {
          fields: () =>
            listener.applyHooks("objectType:fields", rawSpec.fields, {
              spec: rawSpec,
              fields: rawSpec.fields,
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

const postGraphQLPluginsFrom = options => {
  return [
    PgIntrospectionPlugin,
    PgTablesPlugin,
    PgTypesPlugin({ extended: options.dynamicJson }),
    QueryPlugin,
    PgRowByUniqueConstraint,
    PgAllRows,
    PgColumnsPlugin,
    PgComputedColumnsPlugin,
    RandomFieldPlugin,
    PgForwardRelationPlugin,
    PgBackwardRelationPlugin,
  ].filter(_ => _);
};

const postGraphQLInflection = Object.assign({}, defaultInflection, {});

const postGraphQLClassicIdsInflection = Object.assign(
  {},
  postGraphQLInflection,
  {
    field: str => (str === "id" ? "rowId" : camelcase(str)),
  }
);

const createPostGraphQLSchema = (client, schemas, options = {}) => {
  return schemaFromPg(client, {
    schemas,
    inflection: options.classicIds
      ? postGraphQLClassicIdsInflection
      : postGraphQLInflection,
    plugins: postGraphQLPluginsFrom(options),
  });
};

exports.withPgClient = withPgClient;
exports.schemaFromPg = schemaFromPg;
exports.createPostGraphQLSchema = createPostGraphQLSchema;
