"use strict";

var Core = require("lapis-core/index.js");
var SQL = require("lazuli-sql/index.js");

/**
* To represent a SQL SELECT query, supporting multiple columns, table joins and conditions
*/
module.exports = Core.Base.clone({
    id: "Query",
    rows: 0,
    max_sort_seq: 3,
});


/**
* Initializes the main properties used by this object and it checks if the connection property
* is initialized
*/
module.exports.defbind("initialize", "clone", function () {
    if (!this.connection) {
        this.throwError("no connection");
    }
    this.tables = [];            // index 0 = alias A, etc
    this.conditions = [];
    this.columns = {};
    this.main = this.addTable({
        table: this.table,
        type: "M",
    });
    this.started = false;
    this.ended = false;
    return this;
});


/**
* Releases the statement and resultset resources (if open) and calls the finishedWithConnection
* on the connection property.
*/
module.exports.define("reset", function () {
    this.ended = true;
    this.connection.finishedWithResultSet(this.resultset);
    this.resultset = null;
});


/**
* Runs an sql query built by the getSQLStatement function and it stores the results in the
*resultset property
*/
module.exports.define("open", function () {
    var sql;
    this.reset();
    this.rows = 0;
    this.started = false;
    this.ended = false;
    sql = this.getSQLStatement();
//    this.java_conn = this.connection.getConnection();
//    this.connection.conn = this.connection.getConnection();
         // get private retained connection
    this.resultset = this.connection.executeQuery(sql);
    if (!this.get_found_rows) {
        return;
    }
    this.found_rows = this.getFoundRowsSeparateQuery();
});


/**
* Moves the resultset to the next result and checks if there are no other results.
* @return boolean true, if there are others results in the resultset
*/
module.exports.define("next", function () {
    var that = this;
    if (!this.resultset) {
        this.open();
    }
    if (!this.ended) {
        this.ended = !this.resultset.next();
        this.started = true;
    }
    if (!this.ended) {
        this.rows += 1;
        this.doColumns(function (column) {
            if (column.active && (!column.table || column.table.active)) {
                column.value = SQL.Connection.getColumnString(that.resultset, column.name);
            }
        });
    }
    return !this.ended;
});


/**
* Get the processed row count relative to the last query. This field is incremented on each call
* to the next function.
* @return number processed row count
*/
module.exports.define("getRowCount", function () {
    return this.rows;
});


/**
* Returns the total number of rows selected
*/
module.exports.define("getFoundRows", function () {
    return this.found_rows;
});


/**
* Hides the function columns, by setting their active flag to false
*/
module.exports.define("hideSQLFunctions", function () {
    this.doColumns(function (column) {
        if (column.sql_function) {
            column.active = false;
        }
    });
});


/**
* Returns the active row
* @param Transaction Object
* @return Entity row
*/
module.exports.define("getRow", function (trans) {
    return trans.getActiveRow(this.main_entity_id, this.getColumn("A._key").get());
});


// ---------------------------------------------------------------------------- Columns
/**
* Returns the query column use the name arg as index
* @param Index name
* @return The obtained column
*/
module.exports.define("getColumn", function (name) {
    return this.columns[name];
});


/**
* Adds a new column to this query
* @param Column specification object
* @return New column object
*/
module.exports.define("addColumn", function (col_spec) {
    var column;
    if (!col_spec.table && !col_spec.sql_function) {
        this.throwError("Column must specify table or sql_function");
    }
    col_spec.id = col_spec.name;
    col_spec.query = this;
    column = SQL.Query.Column.clone(col_spec);
    this.columns[col_spec.name] = column;
    return column;
});


module.exports.define("getColumnCount", function () {
    return Object.keys(this.columns).length;
});


/**
* Loops over each column passing it as arg for the input function
* @param Function to apply to each column
*/
module.exports.define("doColumns", function (funct) {
    var that = this;
    Object.keys(this.columns).forEach(function (name) {
        funct(that.columns[name]);
    });
});


// ---------------------------------------------------------------------------- Tables
/**
* It lookup the array of joined tables in this query using an index
* @param Number or char Index
* @return The obtained table
*/
module.exports.define("getTable", function (index) {
    if (typeof index === "number") {
        return this.tables[index];
    }
    return this.tables[index.charCodeAt(0) - 65];
});


/**
* Adds a new table to the tables array created using the table_spec arg
* @param Table specification
* @return The new table
*/
module.exports.define("addTable", function (table_spec) {
    var table;
    if (!table_spec.id) {
        table_spec.id = table_spec.table;
    }
    table_spec.index = this.tables.length;
    table_spec.query = this;
    table = SQL.Query.Table.clone(table_spec);
    this.tables.push(table);
    return table;
});


/**
* Returns the total number of tables
* @return Number tot tables
*/
module.exports.define("getTableCount", function () {
    return this.tables.length;
});


// ---------------------------------------------------------------------------- Conditions
/**
* It lookup the array of conditions in this query using an index
* @param Number or char Index
* @return The obtained condition
*/
module.exports.define("getCondition", function (index) {
    return this.conditions[index];
});


/**
* Adds a new condition to this query
* @param Condition specification object
* @return New condition object
*/
module.exports.define("addCondition", function (cond_spec) {
    var condition;
    cond_spec.id = "cond_" + this.conditions.length;
    cond_spec.query = this;
    if (!cond_spec.type) {
        cond_spec.type = SQL.Query.Condition.types.where_cond;
    }
    condition = SQL.Query.Condition.clone(cond_spec);
    this.conditions.push(condition);
    return condition;
});


/**
* Returns the total number of conditions
* @return Number tot conditions
*/
module.exports.define("getConditionCount", function () {
    return this.conditions.length;
});


/**
* Clears the condition array preserving the conditions with the fixed flag
*/
module.exports.define("clearConditions", function () {
    var i = 0;
    while (i < this.conditions.length) {
        if (this.conditions[i].fixed) {
            i += 1;
        } else {
            this.conditions.splice(i, 1);
        }
    }
});


/**
* Remove the sorting logic related to each column
*/
module.exports.define("sortClear", function () {
    this.doColumns(function (column) {
        column.sortRemove();
    });
});


/**
* Outputs the SQL statement relative to this object
* @return String generated sql statement from this object
*/
module.exports.define("getSQLStatement", function () {
    var out = this.getSelectClause()
        + this.getFromClause()
        + this.getWhereClause()
        + this.getGroupClause()
        + this.getHavingClause()
        + this.getOrderClause();
    if (this.limit_row_count > 0) {
        out = out + " LIMIT " + (this.limit_offset || 0) + ", " + this.limit_row_count;
    }
    return out;
});


module.exports.define("getFoundRowsSQL", function () {
    var out = "SELECT COUNT(*) FROM ( "
            + this.getSelectCountClause()
            + this.getFromClause()
            + this.getWhereClause()
            + this.getGroupClause()
            + this.getHavingClause() + ") as C";

    return out;
});


module.exports.define("getFoundRowsSeparateQuery", function () {
    var out = 0;
    var resultset = this.connection.executeQuery(this.getFoundRowsSQL());
    if (resultset.next()) {
        out = resultset.getInt(1);
    }
    resultset.close();
    return out;
});


/**
* Generates the SELECT clause part of the SQL statement when is used to count
* @return String SELECT clause part of the SQL statement
*/
module.exports.define("getSelectCountClause", function () {
    var out = "SELECT 1";
    this.doColumns(function (column) {
        if (column.active && (!column.table || column.table.active)) {
            if (column.sql_function && column.group_col) {
                out += ", ( " + SQL.Connection.detokenizeAlias(column.sql_function, (column.table ? column.table.alias : "A")) + " ) AS " + column.name;
            }
        }
    });
    return out;
});


/**
* Generates the SELECT clause part of the SQL statement
* @return String SELECT clause part of the SQL statement
*/
module.exports.define("getSelectClause", function () {
    var out = "SELECT";
    var delim = " ";

    if (this.use_query_cache === true) {
        out += " SQL_CACHE";
    } else if (this.use_query_cache === false) {
        out += " SQL_NO_CACHE";
    }

//    if (this.get_found_rows) {
//        out += " SQL_CALC_FOUND_ROWS";
//    }
    this.doColumns(function (column) {
        if (column.active && (!column.table || column.table.active)) {
            if (column.sql_function) {
                out += delim + " ( " + SQL.Connection.detokenizeAlias(column.sql_function, (column.table ? column.table.alias : "A")) + " ) AS " + column.name;
            } else {
                out += delim + column.name;
            }
            delim = ", ";
        }
    });
    if (delim === " ") {
        this.throwError("No active columns defined");
    }
    return out;
});


/**
* Generates the FROM clause part of the SQL statement
* @return String FROM clause part of the SQL statement
*/
module.exports.define("getFromClause", function () {
    var out = " FROM ";
    var i;

    for (i = 0; i < this.tables.length; i += 1) {
        if (this.tables[i].active) {
            out += this.tables[i].getSQL();
        }
    }
    return out;
});


/**
* Generates the WHERE clause part of the SQL statement
* @return String WHERE clause part of the SQL statement
*/
module.exports.define("getWhereClause", function () {
    var out = "";
    var i;
    var delim = " WHERE ";
    for (i = 0; i < this.conditions.length; i += 1) {
        if (this.conditions[i].active
                && this.conditions[i].type === SQL.Query.Condition.types.where_cond) {
            out = out + delim + "( " + this.conditions[i].getSQL() + " )";
            delim = " AND ";
        }
    }
    return out;
});


/**
* Generates the GROUP BY clause part of the SQL statement
* @return String GROUP BY clause part of the SQL statement
*/
module.exports.define("getGroupClause", function () {
    var out = "";
    var delim = " GROUP BY ";
    this.doColumns(function (column) {
        if (column.group_col) {
            out = out + delim + column.name;
            delim = ", ";
        }
    });
    return out;
});


/**
* Generates the HAVING clause part of the SQL statement
* @return String HAVING clause part of the SQL statement
*/
module.exports.define("getHavingClause", function () {
    var out = "";
    var i;
    var delim = " HAVING ";
    for (i = 0; i < this.conditions.length; i += 1) {
        if (this.conditions[i].active
                && this.conditions[i].type === SQL.Query.Condition.types.having_cond) {
            out = out + delim + "( " + this.conditions[i].getSQL() + " )";
            delim = " AND ";
        }
    }
    return out;
});


/**
* Generates the ORDER BY clause part of the SQL statement
* @return String ORDER BY clause part of the SQL statement
*/
module.exports.define("getOrderClause", function () {
    var temp = [];
    var out = "";
    var i;
    var delim = " ORDER BY ";
    this.doColumns(function (column) {
        if (typeof column.sort_seq === "number") {
            temp[column.sort_seq] = column.getOrderTerm();
        }
    });
    for (i = 0; i < temp.length; i += 1) {
        if (temp[i]) {
            out = out + delim + temp[i];
            delim = ", ";
        }
    }
    return out;
});

