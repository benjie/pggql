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
      userById(id: 1) {
        rnd1: random(sides: 6)
        rnd2: random(sides: 6)
        rnd3: random(sides: 10)
        id
        name
        aliasedName: name
        avatarUrl
        createdAt
        primaryEmailId
        userEmailByPrimaryEmailIdAndId {
          id
          email
          userByUserId {
            id
            name
            userEmailByPrimaryEmailIdAndId {
              id
              email
              verified
            }

          }
        }
        ...Foo
      }
      sketchById(id: 69) {
        id
        configuration
      }
    }
    fragment Foo on User {
      id
      name
      rnd1: random(sides: 6)
      rnd4: random(sides: 20)
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
    console.dir(result, { depth: 5 });
  }
  process.exit(0);
})().catch(e => {
  console.error("ERROR!");
  console.dir(e);
});
