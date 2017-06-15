const { schemaFromPg } = require(".");
const fs = require("fs");
const path = require("path");
const { graphql } = require("graphql");
const { /*introspectionQuery,*/ printSchema } = require("graphql/utilities");
const pg = require("pg");
const pgConnectionString = require("pg-connection-string");

(async () => {
  const pgPool = new pg.Pool(
    pgConnectionString.parse(process.env.DATABASE_URL)
  );
  const schema = await schemaFromPg(pgPool, {
    schema: ["hookhaven_data"],
  });
  console.log(printSchema(schema));
  const pgClient = await pgPool.connect();
  const result = await graphql(
    schema,
    `
    query {
      randomUser {
        id
        name
        avatarUrl
        createdAt
      }
    }
    `,
    undefined,
    {
      pgClient,
    }
  );
  await pgClient.release();
  if (result.errors) {
    console.error("ERROR introspecting schema: ", result.errors);
    process.exit(1);
  } else {
    console.dir(result);
  }
  process.exit(0);
})().catch(e => {
  console.error("ERROR!");
  console.dir(e);
});
