"use strict";

// Based on https://github.com/tjmehta/graphql-parse-fields

var assert = require("assert");
const defaults = require("lodash/defaults");

module.exports = parseFields;

/**
 * parse fields has two signatures:
 * 1)
 * @param {Object} info - graphql resolve info
 * @param {Boolean} [keepRoot] default: true
 * @return {Object} fieldTree
 * 2)
 * @param {Array} asts - ast array
 * @param {Object} [fragments] - optional fragment map
 * @param {Object} [fieldTree] - optional initial field tree
 * @return {Object} fieldTree
 */
function parseFields(/* dynamic */) {
  var tree;
  var info = arguments[0];
  var options = arguments[1];
  var fieldNodes = info && (info.fieldASTs || info.fieldNodes);
  if (fieldNodes) {
    // (info, keepRoot)
    options = options || {};
    defaults(options, {
      keepRoot: false,
      deep: true,
    });
    tree = fieldTreeFromAST(fieldNodes, info.fragments, undefined, options);
    if (!options.keepRoot) {
      var key = firstKey(tree);
      tree = tree[key];
    }
  } else {
    // (asts, fragments, fieldTree, [options])
    tree = fieldTreeFromAST.apply(this, arguments);
  }
  return tree;
}

function fieldTreeFromAST(asts, fragments, init, options) {
  init = init || {};
  fragments = fragments || {};
  asts = Array.isArray(asts) ? asts : [asts];
  return asts.reduce(function(tree, val) {
    var kind = val.kind;
    var name = val.name && val.name.value;
    var alias = val.alias ? val.alias.value : name;
    var fragment;
    if (kind === "Field") {
      if (!tree[alias]) {
        tree[alias] = {
          ast: val,
          alias,
          name,
          fields: {},
        };
        if (val.selectionSet && options.deep) {
          fieldTreeFromAST(
            val.selectionSet.selections,
            fragments,
            tree[alias].fields,
            options
          );
        } else {
          // No fields to add
        }
      }
    } else if (kind === "FragmentSpread" && options.deep) {
      fragment = fragments[name];
      assert(fragment, 'unknown fragment "' + name + '"');
      fieldTreeFromAST(
        fragment.selectionSet.selections,
        fragments,
        tree,
        options
      );
    } else if (kind === "InlineFragment" && options.deep) {
      fragment = val;
      fieldTreeFromAST(
        fragment.selectionSet.selections,
        fragments,
        tree,
        options
      );
    } // else ignore
    return tree;
  }, init);
}

function firstKey(obj) {
  for (var key in obj) {
    return key;
  }
}
