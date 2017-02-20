/*jslint node: true */
/*global java */
"use strict";
var Connection   = require("../sql/Connection")
  , Query        = require("../sql/Query")
  , Log          = require("../base/Log")
  ;



module.exports.main = function (test) {
    var conn  = Connection.clone({ id: "test-connection" }),
        query = Query.clone({ id: "ac_user", connection: conn, table: "ac_user" }),
        col,
        col2,
        tab1;

    test.expect(22);

    test.equal(query.main.type, "M", "Query has 'main' table, with type 'M'");
    test.ok(query.main.active, "'main' table is active by default");
    test.equal(query.main.alias, "A", "'main' table has alias 'A'");
    test.equal(query.main.getSQL(), "ac_user AS A", "'main' table gives SQL 'ac_user AS A'");
    test.equal(query.getTable(0), query.main, "Can get 'main' table with index 0");
    test.equal(query.getTable("A"), query.main, "Can get 'main' table with alias 'A'");
    test.equal(query.getTableCount(), 1, "Number of tables is 1");
    this.testCatch(query.getSQLStatement, query, null, "No active columns defined");
    col = query.main.addColumn( { name: "name" } );
    test.ok(col.active, "Column is active by default");
    test.equal(typeof col.get(), "undefined", "Column value is initially undefined");
    test.equal(col.name, "A.name", "Column name is 'A.name'");
    test.equal(query.getSQLStatement(), "SELECT A.name FROM ac_user AS A", "Basic SQL statement");
    while (query.next()) {
        Log.debug(query.getRowCount() + " --- " + col.get());
    }
    query.reset();
    col.group_col = true;
    test.equal(query.getSQLStatement(), "SELECT A.name FROM ac_user AS A GROUP BY A.name", "SQL statement with GROUP BY");
    query.addCondition({ column: "A.id", operator: "=", value: "batch" });
    test.equal(query.getSQLStatement(), "SELECT A.name FROM ac_user AS A WHERE ( A.id='batch' ) GROUP BY A.name", "SQL statement with WHERE and GROUP BY");
    test.ok(query.next() && !query.next(), "GROUPed query returns a single row");
    query.getCondition(0).type = Condition.types.having_cond;
    test.equal(query.getSQLStatement(), "SELECT A.name FROM ac_user AS A GROUP BY A.name HAVING ( A.id='batch' )", "SQL statement with HAVING and GROUP BY");
    col2 = query.main.addColumn({ name: "password" });
    query.main.addColumn({ name: "email" }).active = false;
    query.getCondition(0).active = false;
    col.group_col = false;
    test.equal(query.getSQLStatement(), "SELECT A.name, A.password FROM ac_user AS A", "SQL statement with inactive column and condition");
    col2.sortTop();
    test.equal(query.getSQLStatement(), "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.password", "SQL statement with simple ORDER");
    col.sortTop();
    col.sortDesc();
    test.equal(query.getSQLStatement(), "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.name DESC, A.password", "SQL statement with compound ORDER including DESC");
    col.sortBottom();
    col.sortAsc();
    test.equal(query.getSQLStatement(), "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.password, A.name", "SQL statement with compound ORDER, reverted");
    tab1 = query.addTable( { table: "ac_user", join_cond: "?.id=A.id" } );
    test.equal(query.getSQLStatement(), "SELECT A.name, A.password FROM ac_user AS A LEFT OUTER JOIN ac_user AS B ON B.id=A.id ORDER BY A.password, A.name", "SQL statement with OUTER JOIN");
    tab1.type = Table.types.inner_join;
    test.equal(query.getSQLStatement(), "SELECT A.name, A.password FROM ac_user AS A INNER JOIN ac_user AS B ON B.id=A.id ORDER BY A.password, A.name", "SQL statement with INNER JOIN");
    tab1.active = false;
    test.ok(query.getSQLStatement() === "SELECT A.name, A.password FROM ac_user AS A ORDER BY A.password, A.name", "SQL statement with inactive table");
    query.reset();

    conn.close();

    test.done();
};

