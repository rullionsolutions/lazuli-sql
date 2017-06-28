"use strict";

var Core = require("lapis-core/index.js");
var SQL = require("lazuli-sql/index.js");

/**
* To represent a SQL TABLE structure
*/
module.exports = Core.Base.clone({
    id: "Table",
    active: true,
    type: "O",
    types: {
        inner_join: "I",
        outer_join: "O",
    },
});


module.exports.defbind("manageAlias", "clone", function (spec) {
    this.alias = (spec && spec.alias) || this.alias || String.fromCharCode(65 + this.index);
    if (this.join_cond) {                       // matches '?' NOT preceded by '\'
        this.join_cond = SQL.Connection.detokenizeAlias(this.join_cond, this.alias);
    }
});


/**
* Adds a new column to this table
* @param Column specification object
* @return New column object
*/
module.exports.define("addColumn", function (col_spec) {
    if (typeof col_spec.name !== "string") {
        this.throwError("Property 'name' must be supplied");
    }
    col_spec.table = this;
    if (col_spec.sql_function) {
        col_spec.name = this.alias + "_" + col_spec.name;
    } else {
        col_spec.name = this.alias + "." + col_spec.name;
    }
    return this.query.addColumn(col_spec);
});


/**
* Outputs the SQL statement relative to this object
* @return String generated sql statement from this object
*/
module.exports.define("getSQL", function () {
    var out = "";
    if (this.type === this.types.inner_join) {
        out += " INNER JOIN ";
    } else if (this.type === this.types.outer_join) {
        out += " LEFT OUTER JOIN ";
    } else if (this.type !== "M") {
        this.throwError("invalid join type: " + this.type + " on table " + this.alias + " of query " + this.owner);
    }
    out += this.table + " AS " + this.alias;
    if (this.join_cond) {
        out += " ON " + this.join_cond;
    }
    return out;
});
