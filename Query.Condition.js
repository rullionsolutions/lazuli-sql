"use strict";

var Core = require("lapis-core/index.js");
var SQL = require("lazuli-sql/index.js");

/**
* To represent a SQL condition
*/
module.exports = Core.Base.clone({
    id: "Condition",
    active: true,
    types: {
        where_cond: "W",
        having_cond: "H",
    },
    oper_map: {
        AN: function (cond) {
            var pieces = cond.value.split("|");
            var str = "";
            var delim = "";
            var i;
            for (i = 0; i < pieces.length; i += 1) {
                if (pieces[i]) {
                    str += delim + "(" + cond.column + " LIKE " + SQL.Connection.escape("%|" + pieces[i] + "|%") + ")";
                    delim = " OR ";
                }
            }
            if (!str) {
                str = "TRUE";
            }
            return str;
        },
        AL: function (cond) {
            var pieces = cond.value.split("|");
            var str = "";
            var delim = "";
            var i;
            for (i = 0; i < pieces.length; i += 1) {
                if (pieces[i]) {
                    str += delim + "(" + cond.column + " LIKE " + SQL.Connection.escape("%|" + pieces[i] + "|%") + ")";
                    delim = " AND ";
                }
            }
            if (!str) {
                str = "TRUE";
            }
            return str;
        },
        NU: function (cond) { return cond.column + " IS NULL"; },
        NN: function (cond) { return cond.column + " IS NOT NULL"; },
        BE: function (cond) { return "  UPPER(CONVERT(" + cond.column + " USING UTF8))     LIKE UPPER(" + SQL.Connection.escape(cond.value + "%") + ")"; },
    //    BI: function(cond) { return "UPPER( " + cond.column + " LIKE UPPER( " +
    //              Connection.escape(      cond.value + "%") + ")"; },
        BN: function (cond) { return "( UPPER(CONVERT(" + cond.column + " USING UTF8)) NOT LIKE UPPER(" + SQL.Connection.escape(cond.value + "%") + ") OR " + cond.column + " IS NULL )"; },
        BT: function (cond) { return cond.column + " BETWEEN " + SQL.Connection.escape(cond.value) + " AND " + SQL.Connection.escape(cond.value2); },
        CO: function (cond) { return "  UPPER(CONVERT(" + cond.column + " USING UTF8))     LIKE UPPER(" + SQL.Connection.escape("%" + cond.value + "%") + ")"; },
    //    CI: function(cond) { return "UPPER( " + cond.column + " ) LIKE UPPER( " +
    //              Connection.escape("%" + cond.value + "%") + " )"; },
        DC: function (cond) { return "( UPPER(CONVERT(" + cond.column + " USING UTF8)) NOT LIKE UPPER(" + SQL.Connection.escape("%" + cond.value + "%") + ") OR " + cond.column + " IS NULL )"; },
        EQ: function (cond) { return "IFNULL(" + cond.column + ", '') =" + SQL.Connection.escape(cond.value); },
        NE: function (cond) { return "IFNULL(" + cond.column + ", '') <>" + SQL.Connection.escape(cond.value); },
        GT: function (cond) { return cond.column + " >" + SQL.Connection.escape(cond.value); },        // C8316 - removed IFNULL wrappers around
        GE: function (cond) { return cond.column + " >=" + SQL.Connection.escape(cond.value); },        // these conditions, as they were including
        LT: function (cond) { return cond.column + " <" + SQL.Connection.escape(cond.value); },        // blank dates in date filters
        LE: function (cond) { return cond.column + " <=" + SQL.Connection.escape(cond.value); },        // blank number should be diff from zero, too
        HA: function (cond) {
            return cond.column.replace(/\{val\}/g, SQL.Connection.escape(cond.value));
        },
        DH: function (cond) { return "NOT " + cond.column.replace(/\{val\}/g, SQL.Connection.escape(cond.value)); },
        KW: function (cond) {
            var pieces = cond.value.split("|");
            var str = "";
            var delim = "";
            var i;
            if (!cond.value) {
                return "TRUE";
            }
            pieces = cond.value.split(/[,; ]/);
            for (i = 0; i < pieces.length; i += 1) {
                str += delim + "UPPER( CONVERT( " + cond.column + " USING UTF8 ) ) LIKE UPPER( " + SQL.Connection.escape("%" + pieces[i] + "%") + " )";
                delim = " AND ";
            }
            return str;
        },
//    XX: function(cond) { return cond.column + ""   + Connection.escape(cond.value) }
    },       // oper_map
});


module.exports.defbind("validate", "clone", function (spec) {
    if (!(this.full_condition || (this.column && this.operator && typeof this.value === "string"))) {
        this.throwError("Properties 'column' and 'operator' and 'value' or 'full_condition' must be supplied");
    }
});


/**
* Outputs the SQL statement relative to this object
* @return String generated sql statement from this object
*/
module.exports.define("getSQL", function () {
    if (this.full_condition) {
        return this.full_condition;
    }
    if (this.operator && this.oper_map[this.operator]) {
        return this.oper_map[this.operator](this);
    }
    return this.column + this.operator + SQL.Connection.escape(this.value);
});


module.exports.override("remove", function () {
    this.query.conditions.splice(this.query.conditions.indexOf(this), 1);
});

