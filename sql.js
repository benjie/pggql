// Full credit: https://raw.githubusercontent.com/postgraphql/postgraphql/master/src/postgres/utils/sql.ts
const isString = require("lodash/isString");
const lodashIsFinite = require("lodash/isFinite");

function compile(sql) {
  // Join this to generate the SQL query
  const sqlFragments = [];

  // Values hold the JavaScript values that are represented in the query
  // string by placeholders. They are eager because they were provided before
  // compile time.
  const values = [];

  // When we come accross a symbol in our identifier, we create a unique
  // alias for it that shouldn’t be in the users schema. This helps maintain
  // sanity when constructing large Sql queries with many aliases.
  let nextSymbolId = 0;
  const symbolToIdentifier = new Map();

  const items = Array.isArray(sql) ? sql : [sql];

  for (const item of items) {
    switch (item.type) {
      case "RAW":
        sqlFragments.push(item.text);
        break;
      case "IDENTIFIER":
        if (item.names.length === 0)
          throw new Error("Identifier must have a name");

        sqlFragments.push(
          item.names
            .map(name => {
              if (typeof name === "string") return escapeSqlIdentifier(name);

              // Get the correct identifier string for this symbol.
              let identifier = symbolToIdentifier.get(name);

              // If there is no identifier, create one and set it.
              if (!identifier) {
                identifier = `__local_${nextSymbolId++}__`;
                symbolToIdentifier.set(name, identifier);
              }

              // Return the identifier. Since we create it, we won’t have to
              // escape it because we know all of the characters are safe.
              return identifier;
            })
            .join(".")
        );
        break;
      case "VALUE":
        values.push(item.value);
        sqlFragments.push(`$${values.length}`);
        break;
      default:
        throw new Error(`Unexpected Sql item type '${item["type"]}'.`);
    }
  }

  const text = sqlFragments.join("");
  return {
    text,
    values,
  };
}

/**
 * A template string tag that creates a `Sql` query out of some strings and
 * some values. Use this to construct all PostgreSQL queries to avoid SQL
 * injection.
 *
 * Note that using this function, the user *must* specify if they are injecting
 * raw text. This makes a SQL injection vulnerability harder to create.
 */
const query = (strings, ...values) =>
  strings.reduce(
    (items, text, i) =>
      !values[i]
        ? [...items, { type: "RAW", text }]
        : [...items, { type: "RAW", text }, ...flatten(values[i])],
    []
  );

/**
 * Creates a Sql item for some raw Sql text. Just plain ol‘ raw Sql. This
 * method is dangerous though because it involves no escaping, so proceed
 * with caution!
 */
const raw = text => ({ type: "RAW", text });

/**
 * Creates a Sql item for a Sql identifier. A Sql identifier is anything like
 * a table, schema, or column name. An identifier may also have a namespace,
 * thus why many names are accepted.
 */
const identifier = (...names) => ({ type: "IDENTIFIER", names });

/**
 * Creates a Sql item for a value that will be included in our final query.
 * This value will be added in a way which avoids Sql injection.
 */
const value = val => ({ type: "VALUE", value: val });

/**
 * If the value is simple will inline it into the query, otherwise will defer
 * to value.
 */
const literal = val => {
  if (isString(val) && val.match(/^[a-zA-Z0-9_-]*$/)) {
    return raw(`'${val}'`);
  } else if (lodashIsFinite(val)) {
    if (Number.isInteger(val)) {
      return raw(String(val));
    } else {
      return raw(`'${val}'::float`);
    }
  } else {
    return { type: "VALUE", value: val };
  }
};

/**
 * Join some Sql items together seperated by a string. Useful when dealing
 * with lists of Sql items that doesn’t make sense as a Sql query.
 */
const join = (items, seperator = "") =>
  items.reduce(
    (currentItems, item, i) =>
      i === 0 || !seperator
        ? [...currentItems, ...flatten(item)]
        : [...currentItems, { type: "RAW", text: seperator }, ...flatten(item)],
    []
  );

/**
 * Flattens a deeply nested array.
 *
 * @private
 */
function flatten(array) {
  if (!Array.isArray(array)) return [array];

  // Type cheats ahead! Making TypeScript compile here is more important then
  // making the code statically correct.
  return array.reduce(
    (currentArray, item) =>
      Array.isArray(item)
        ? [...currentArray, ...flatten(item)]
        : [...currentArray, item],
    []
  );
}

const escapeSqlIdentifier = str => `"${str.replace(/"/g, '""')}"`;

exports.query = query;
exports.fragment = query;
exports.raw = raw;
exports.identifier = identifier;
exports.value = value;
exports.literal = literal;
exports.join = join;
exports.compile = compile;
