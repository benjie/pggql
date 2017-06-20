const { graphql } = require("graphql");
const { withRootDb } = require("../helpers");
const { createPostGraphQLSchema } = require("../..");
const { readdirSync, readFile: rawReadFile } = require("fs");
const { promisify } = require("util");
const { resolve: resolvePath } = require("path");

const readFile = promisify(rawReadFile);

const queriesDir = `${__dirname}/../fixtures/queries`;
const queryFileNames = readdirSync(queriesDir);
let queryResults = [];

const withPgClient = withRootDb;

const kitchenSinkData = () => readFile(`${__dirname}/../kitchen-sink-data.sql`);

beforeAll(() => {
  // Get a few GraphQL schema instance that we can query.
  const gqlSchemasPromise = withPgClient(async pgClient => {
    // Different fixtures need different schemas with different configurations.
    // Make all of the different schemas with different configurations that we
    // need and wait for them to be created in parallel.
    const [normal, classicIds, dynamicJson] = await Promise.all([
      createPostGraphQLSchema(pgClient, ["a", "b", "c"]),
      createPostGraphQLSchema(pgClient, ["a", "b", "c"], { classicIds: true }),
      createPostGraphQLSchema(pgClient, ["a", "b", "c"], { dynamicJson: true }),
    ]);
    return {
      normal,
      classicIds,
      dynamicJson,
    };
  });

  // Execute all of the queries in parallel. We will not wait for them to
  // resolve or reject. The tests will do that.
  //
  // All of our queries share a single client instance.
  const queryResultsPromise = (async () => {
    // Wait for the schema to resolve. We need the schema to be introspected
    // before we can do anything else!
    const gqlSchemas = await gqlSchemasPromise;
    // Get a new Postgres client instance.
    return await withPgClient(async pgClient => {
      // Add data to the client instance we are using.
      await pgClient.query(await kitchenSinkData);
      // Run all of our queries in parallel.
      return await Promise.all(
        queryFileNames.map(async fileName => {
          // Read the query from the file system.
          const query = await new Promise((resolve, reject) => {
            readFile(
              resolvePath(queriesDir, fileName),
              "utf8",
              (error, data) => {
                if (error) reject(error);
                else resolve(data);
              }
            );
          });
          // Get the appropriate GraphQL schema for this fixture. We want to test
          // some specific fixtures against a schema configured slightly
          // differently.
          const gqlSchema = fileName === "classic-ids.graphql"
            ? gqlSchemas.classicIds
            : fileName === "dynamic-json.graphql"
              ? gqlSchemas.dynamicJson
              : gqlSchemas.normal;
          // Return the result of our GraphQL query.
          return await graphql(gqlSchema, query, null, {
            pgClient: pgClient,
          });
        })
      );
    });
  })();

  // Flatten out the query results promise.
  queryResults = queryFileNames.map(async (_, i) => {
    return await (await queryResultsPromise)[i];
  });
});

for (let i = 0; i < queryFileNames.length; i++) {
  test(queryFileNames[i], async () => {
    expect(await queryResults[i]).toMatchSnapshot();
  });
}
