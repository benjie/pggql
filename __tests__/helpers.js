const pg = require("pg");
const promisify = require("util").promisify;
const readFile = promisify(require("fs").readFile);

const withDbFromUrl = async (url, fn) => {
  const client = new pg.Client(url);
  await client.connect();
  await client.query("BEGIN ISOLATION LEVEL SERIALIZABLE;");
  try {
    await fn(client);
  } finally {
    await client.query("ROLLBACK;");
    await client.end();
  }
};

const withRootDb = fn => withDbFromUrl(process.env.TEST_DATABASE_URL, fn);

let prepopulatedDBKeepalive;

const populateDatabase = async client => {
  await client.query(
    await readFile(`${__dirname}/kitchen-sink-data.sql`, "utf8")
  );
  return {};
};

const withPrepopulatedDb = async fn => {
  if (!prepopulatedDBKeepalive) {
    throw new Error("You must call setup and teardown to use this");
  }
  const { client, vars } = prepopulatedDBKeepalive;
  if (!vars) {
    throw new Error("No prepopulated vars");
  }
  let err;
  try {
    await fn(client, vars);
  } catch (e) {
    err = e;
  }
  try {
    await client.query("ROLLBACK TO SAVEPOINT pristine;");
  } catch (e) {
    err = err || e;
    console.error("ERROR ROLLING BACK", e.message); // eslint-disable-line no-console
  }
  if (err) {
    throw err;
  }
};

withPrepopulatedDb.setup = done => {
  if (prepopulatedDBKeepalive) {
    throw new Error("There's already a prepopulated DB running");
  }
  let res;
  let rej;
  prepopulatedDBKeepalive = new Promise((resolve, reject) => {
    res = resolve;
    rej = reject;
  });
  prepopulatedDBKeepalive.resolve = res;
  prepopulatedDBKeepalive.reject = rej;
  withRootDb(async client => {
    prepopulatedDBKeepalive.client = client;
    try {
      prepopulatedDBKeepalive.vars = await populateDatabase(client);
    } catch (e) {
      console.error("FAILED TO PREPOPULATE DB!", e.message); // eslint-disable-line no-console
      return done(e);
    }
    await client.query("SAVEPOINT pristine;");
    done();
    return prepopulatedDBKeepalive;
  });
};

withPrepopulatedDb.teardown = () => {
  if (!prepopulatedDBKeepalive) {
    throw new Error("Cannot tear down null!");
  }
  prepopulatedDBKeepalive.resolve(); // Release DB transaction
  prepopulatedDBKeepalive = null;
};

exports.withRootDb = withRootDb;
exports.withPrepopulatedDb = withPrepopulatedDb;
