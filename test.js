const { typesFromPg } = require(".");
(async () => {
  console.dir(await typesFromPg());
})().catch(e => {
  console.error("ERROR!");
  console.dir(e);
});
