const { schemaFromPg } = require(".");
const fs = require("fs");
const path = require("path");
const { graphql } = require("graphql");
const { introspectionQuery, printSchema } = require("graphql/utilities");

(async () => {
  const schema = await schemaFromPg(process.env.DATABASE_URL, {
    schema: ["hookhaven_data"],
  });
  console.log(printSchema(schema));
  const result = await graphql(schema, introspectionQuery);
  if (result.errors) {
    console.error("ERROR introspecting schema: ", result.errors);
    process.exit(1);
  }
  process.exit(0);
})().catch(e => {
  console.error("ERROR!");
  console.dir(e);
});
