/* global module, require */

"use strict";

var Test = require("lazuli-test/UnitTests.js");
var SQL = require("lazuli-sql/index.js");


module.exports = Test.clone({
    id: "UnitTestSQL",
});

module.exports.override("test", function () {
    var conn = SQL.Connection.clone({
        id: "test-connection",
    });
    var query = SQL.Query.clone({
        id: "ac_user",
        connection: conn,
        table: "ac_user",
    });
    var col;
    var col2;
    var tab1;

    this.assert(query.main.type === "M", "Query has 'main' table, with type 'M'");
    this.assert(query.main.active, "'main' table is active by default");
    this.assert(query.main.alias === "A", "'main' table has alias 'A'");
    this.assert(query.main.getSQL() === "ac_user AS A", "'main' table gives SQL 'ac_user AS A'");
    this.assert(query.getTable(0) === query.main, "Can get 'main' table with index 0");
    this.assert(query.getTable("A") === query.main, "Can get 'main' table with alias 'A'");
    this.assert(query.getTableCount() === 1, "Number of tables is 1");
    this.testCatch(query.getSQLStatement, query, null, "No active columns defined");
    col = query.main.addColumn({
        name: "name",
    });
    this.assert(col.active, "Column is active by default");
    this.assert(typeof col.get() === "undefined", "Column value is initially undefined");
    this.assert(col.name === "A.name", "Column name is 'A.name'");
    this.assert(query.getSQLStatement() === "SELECT A.name FROM ac_user AS A", "Basic SQL statement");
    while (query.next()) {
        this.debug(query.getRowCount() + " --- " + col.get());
    }
    query.reset();
    col.group_col = true;
    this.assert(query.getSQLStatement() === "SELECT A.name FROM ac_user AS A GROUP BY A.name", "SQL statement with GROUP BY");
    query.addCondition({
        column: "A.id",
        operator: "=",
        value: "batch",
    });
    this.assert(query.getSQLStatement() === "SELECT A.name FROM ac_user AS A WHERE ( A.id='batch' ) GROUP BY A.name", "SQL statement with WHERE and GROUP BY");
    this.assert(query.next() && !query.next(), "GROUPed query returns a single row");
    query.getCondition(0).type = SQL.having_cond;
    this.assert(query.getSQLStatement() === "SELECT A.name FROM ac_user AS A GROUP BY A.name HAVING ( A.id='batch' )", "SQL statement with HAVING and GROUP BY");
    col2 = query.main.addColumn({
        name: "password",
    });
    query.main.addColumn({
        name: "email",
    }).active = false;
    query.getCondition(0).active = false;
    col.group_col = false;
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A", "SQL statement with inactive column and condition");
    col2.sortTop();
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.password", "SQL statement with simple ORDER");
    col.sortTop();
    col.sortDesc();
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.name DESC, A.password", "SQL statement with compound ORDER including DESC");
    col.sortBottom();
    col.sortAsc();
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.password, A.name", "SQL statement with compound ORDER, reverted");
    tab1 = query.addTable({
        table: "ac_user",
        join_cond: "?.id=A.id",
    });
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A LEFT OUTER JOIN ac_user AS B ON B.id=A.id ORDER BY A.password, A.name", "SQL statement with OUTER JOIN");
    tab1.type = SQL.Query.Table.types.inner_join;
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A INNER JOIN ac_user AS B ON B.id=A.id ORDER BY A.password, A.name", "SQL statement with INNER JOIN");
    tab1.active = false;
    this.assert(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.password, A.name", "SQL statement with inactive table");
    query.reset();

    conn.close();
});
