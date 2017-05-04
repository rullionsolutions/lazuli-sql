/* globals Packages */

"use strict";

var Core = require("lapis-core/index.js");


module.exports = Core.Base.clone({
    id: "Connection",
    storage_engine: "INNODB",
    conn: null,                 // MySQL connection object
    // Maximum number of rows to return in a query, 0 = unlimited, defaults to 0
    max_rows: 0,
    // Whether or not each data modification statement should be immediately committed,
    // defaults false
    auto_commit: true,
    use_pool: false,            // set true inside Tomcat
    isolation_level: "READ COMMITTED",
    database_exists: true,
    execution_retries: 2,
});


/*
Objectives:
- to use Tomcat Connection Pooling if running through Tomcat
- to minimize the use of new Connections if running in the Rhino Shell
- under Tomcat, to ensure that threads don't step on each other's Connections -
    i.e. that a thread pulls a Connection off the pool and has private use of it
    until it has finished with it
- to provide a convenient means to manage Connection properties, including auto
    commit and isolation level
*/


module.exports.define("usePool", function () {
    var initContext = new Packages.javax.naming.InitialContext();
    var envContext = initContext.lookup("java:/comp/env");
    this.data_source = envContext.lookup("jdbc/database");
    this.use_pool = true;
});


module.exports.define("shared", module.exports.clone({
    id: "shared",
    shared_connection: true,
}));


module.exports.define("getQueryConnection", function (id) {
    return this.use_pool ? this.clone({ id: id + "_" + Core.Format.getRandomNumber(1000), }) : this.shared;
});


module.exports.define("getTransConnection", function (id) {
    var new_conn = this.shared_trans || this.clone({
        id: id,
        auto_commit: false,
        isolation_level: "READ COMMITTED",
        execution_retries: 0,
    });
    // new_conn.trans_conn_hashcode = new_conn.getConnection().conn.hashcode();
    if (!this.use_pool && !this.shared_trans) {
        this.shared_trans = new_conn;
    }
    return new_conn;
});


module.exports.define("getUncommittedConnection", function (id) {
    var new_conn = this.shared_uncomm || this.clone({
        id: id,
        isolation_level: "READ UNCOMMITTED",
    });
    // new_conn.trans_conn_hashcode = new_conn.getConnection().conn.hashcode();
    if (!this.use_pool && !this.shared_uncomm) {
        this.shared_uncomm = new_conn;
    }
    return new_conn;
});


/**
* Get a new or a retained connection with the db
* @return connection object
*/
module.exports.define("getConnection", function () {
    var stat;
    if (this === module.exports) {
        this.throwError("must NOT use archetype Connection object");
    }
    if (!this.driver) {
        this.driver = Packages.java.lang.Class.forName("com.mysql.jdbc.Driver").newInstance();
    }
    if (this.conn && this.conn.isClosed()) {
        this.trace("Existing connection closed: " + this.conn.hashCode());
        this.conn = null;
    }
    if (!this.conn) {
        this.conn = this.getNewJDBCConnection();
        this.debug("new connection: " + this + ", Java conn: " + this.conn + ", Java hashCode: " + this.conn.hashCode() +
            ", auto_commit: " + this.auto_commit + ", url: " + this.url + ", data_source: " + this.data_source + ", isolation_level: " + this.isolation_level);
        if (this.isolation_level) {
            stat = this.conn.createStatement(Packages.java.sql.ResultSet.TYPE_FORWARD_ONLY,
                Packages.java.sql.ResultSet.CONCUR_READ_ONLY);
            stat.executeUpdate("SET TRANSACTION ISOLATION LEVEL " + this.isolation_level);
        }
    }
    return this.conn;
});


module.exports.define("getConnectionID", function () {
    var resultset;
    try {
        resultset = this.executeQuery("SELECT CONNECTION_ID()");
        resultset.next();
        return this.getColumnString(resultset, 1);
    } finally {
        this.finishedWithResultSet(resultset);
    }
});


module.exports.define("getNewJDBCConnection", function () {
    var conn;
    if (this.use_pool) {
        conn = this.getNewJDBCConnectionFromPool();
    } else {
        conn = this.getNewJDBCConnectionDirect();
    }
    if (this.database && this.database_exists) {
        conn.setCatalog(this.database);
    }
    if (typeof this.auto_commit === "boolean") {
        conn.setAutoCommit(this.auto_commit);        // this call tests MySQL connection
    }
    return conn;
});


module.exports.define("getNewJDBCConnectionFromPool", function () {
    return this.data_source.getConnection();
});


module.exports.define("getNewJDBCConnectionDirect", function () {
    this.url = "jdbc:mysql://" + this.rdbms_host + ":" + this.rdbms_port;
    return Packages.java.sql.DriverManager.getConnection(this.url, this.rdbms_user,
        this.rdbms_pswd);
});


// Makes and returns a NEW statement object within this Connection object
/**
* Makes and returns a NEW statement object within this Connection object
* @param query_timeout number
* @return statement object
*/
module.exports.define("getStatement", function (query_timeout) {
    var stat = this.getConnection().createStatement(Packages.java.sql.ResultSet.TYPE_FORWARD_ONLY,
        Packages.java.sql.ResultSet.CONCUR_READ_ONLY);

    if (typeof this.max_rows === "number") {
        stat.setMaxRows(this.max_rows);
    }
    if (typeof query_timeout === "number") {
        stat.setQueryTimeout(query_timeout);
    }
    return stat;
});


/**
* Makes and returns a NEW statement object within this Connection object using the input sql
* @param sql string
* @return statement object
*/
module.exports.define("prepareStatement", function (sql) {
    return this.getConnection().prepareStatement(sql);
});


/**
* Executes the input sql query
* @param sql string, query_timeout number, on_exception function
* @return resultset object
*/
module.exports.define("executeQuery", function (sql, query_timeout, on_exception) {
    var statement;
    this.debug(sql);
    try {
        statement = this.getStatement(query_timeout);
        // if (this.shared_connection) {
        //     java.lang.Thread.sleep(2 * 1000);
        // }
        return statement.executeQuery(sql);
    } catch (e) {
        this.addSQLState(e, sql);
        if (on_exception) {
            on_exception(e);
            return null;
        }
        this.error("executeQuery error: " + e.toString());
        this.close();
        throw e;
    }
});


/**
* Does the same as executeQuery but instead of returning resultset object it returns the number
* of rows updated.
* @param sql string, query_timeout number, on_exception function
* @return rows_affected number
*/
module.exports.define("executeUpdate", function (sql, query_timeout, on_exception) {
    var statement;
    var rows_affected;

    this.debug(sql);
    try {
        statement = this.getStatement(query_timeout);
        rows_affected = statement.executeUpdate(sql);
        statement.close();
        if (this.auto_commit) {
            this.finishedWithConnection();
        }
        return rows_affected;
    } catch (e) {
        this.addSQLState(e, sql);
        if (on_exception) {
            on_exception(e);
            return null;
        }

        this.error("executeUpdate error: " + e.toString());
        this.finishedWithConnection();
        if (statement) {
            statement.close();
        }
        throw e;
    }
});


module.exports.define("addSQLState", function (e, sql) {
    e.lock_wait_timeout = (e.toString().indexOf("Lock wait timeout exceeded") > -1);
    e.sql = sql;
    if (e.javaException && typeof e.javaException.getSQLState === "function") {
        e.sql_state = String(e.javaException.getSQLState());
        e.sql_connection_failure = (e.sql_state === "08S01" || e.sql_state === "40001" || e.sql_state === "08003");
    }
    if (e.sql_state === "3D000" || e.sql_state === "42000") {
        module.exports.database_exists = false;
    }

    this.debug("SQL error: " + e.toString() + ", sql_state: " + e.sql_state +
        ", lock_wait_timeout: " + e.lock_wait_timeout + ", sql_connection_failure: " + e.sql_connection_failure);
});


/**
* Closes the input resultset and it calls the finishedWithConnection passing the statment related
* to the resultset
* if present and the shared_connection property is set to false (avoid connection reuse)
* @param resulset object
*/
module.exports.define("finishedWithResultSet", function (resultset) {
    var stat;
    try {
        if (resultset) {
            stat = resultset.getStatement();
            if (stat) {
                stat.close();
                if (!this.shared_connection && this.auto_commit) {
                    this.finishedWithConnection();
                }
            }
            resultset.close();
        }
    } catch (e) {
        this.report(e);
    }
});


/**
* Closes the input prepared_statement
* @param prepared_statement object
*/
module.exports.define("finishedWithPreparedStatement", function (prepared_statement) {
    try {
        if (prepared_statement) {
            this.finishedWithConnection(prepared_statement.getConnection());
            prepared_statement.close();
        }
    } catch (e) {
        this.report(e);
    }
});


/**
* Calls the close function on the connection object passed as input or present as property of
* this object
* @param connection object
*/
module.exports.define("finishedWithConnection", function () {
    this.trace("finishedWithConnection(): conn? " + this.conn + ", isClosed()? " + (this.conn && this.conn.isClosed()) +
        ", use_pool? " + this.use_pool + ", shared? " + this.shared_connection);
    if (this.conn && !this.conn.isClosed() && (this.use_pool || !this.shared_connection)) {
        this.close();
    }
});


/**
* Closes the connection and removes it from the connections_in_use object using the hashcode
* returned by the hashcode function
*/
module.exports.define("close", function () {
    if (!this.conn) {
        return;
    }
    if (!this.conn.isClosed()) {
        try {
            this.conn.close();
        } catch (e) {
            this.report(e);
        }
    }
    this.conn = null;
});


module.exports.define("getAutoIncrement", function () {
    var resultset;
    try {
        resultset = this.executeQuery("SELECT LAST_INSERT_ID()");
        resultset.next();
        return String(resultset.getInt(1));
//        this.populateFromKey(id);
    } finally {
        this.finishedWithResultSet(resultset);
    }
});


module.exports.define("fetchRowObject", function (resultset) {
    var meta_data;
    var row_obj = {};
    var i;
    var column_count;
    var column_name;

    if (resultset) {
        meta_data = resultset.getMetaData();
        column_count = meta_data.getColumnCount();
        for (i = 1; i < (column_count + 1); i += 1) {
            column_name = meta_data.getColumnLabel(i);
            row_obj[column_name] = this.getColumnString(resultset, i);
        }
    }
    return row_obj;
});


module.exports.define("getColumnString", function (resultset, column_id) {
    var str = "";
    var bytes = resultset.getBytes(column_id);
    if (bytes) {
        str = String(new Packages.java.lang.String(bytes, "UTF-8"));
    }
    return str;
});


module.exports.define("escape", function (sql, max_length) {
    if (!sql) {
        return "null";
    }
    if (typeof sql !== "string") {
        sql = String(sql);
    }
    if (typeof max_length === "number" && max_length > -1 && sql.length > max_length) {        // do this BEFORE substitutions
        sql = sql.substr(0, max_length);
    }
    sql = sql.replace(/'/g, "''").replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/\r/g, "");
    return "'" + sql + "'";
});


module.exports.define("detokenizeAlias", function (sql_function, alias) {
// This DOESN'T currently support escaping a ? with a prefixed \ - it wasn't working
//    var out = sql_function.replace(/\?/g, alias).replace(new RegExp("\\\\" + alias), "?");
        // replace ? with alias unless prefixed with \

//    Log.info("Connection.detokenizeAlias: " + sql_function + ", " + alias + " -> " + out);
//    return sql_function.replace(/\?/g, alias);

    // CL - Allows for SQL function based URL fields
    // CL (Again) - I should have used x.field.URL.url_pattern but I think this code still stands
    if (sql_function.match(/\?\./g)) {    // Match ?.
        return sql_function.replace(/\?\./g, alias + ".");
    }
    if (sql_function.match(/\?_/g)) {    // Match ?_
        return sql_function.replace(/\?_/g, alias + "_");
    }
    // Return unchanged
    return sql_function;
});

// ------------------------------------------------------------------------------ Utility Functions
/**
* Read and execute a SQL input file, line by line
* @param String path of input file
*/
module.exports.define("loadSQLFile", function (file) {
    var reader;
    var line;
    var sql = "";

    this.info("Loading sql file: " + file);
    try {
        reader = new Packages.java.io.BufferedReader(
            new Packages.java.io.InputStreamReader(
            new Packages.java.io.FileInputStream(file)));
        line = reader.readLine();
        while (line) {
            line = Core.Format.trim(line);
            sql += line;
            if (sql.match(/;$/)) {
                this.executeUpdate(sql);
                sql = "";
            } else {
                sql += "\n";
            }
            line = reader.readLine();
        }
        reader.close();
    } catch (e) {
        this.error(e.toString());
        return e + "\n";
    }
    return 1;
});


module.exports.define("composeMySQLCommand", function (options) {
    var out = "mysql " +
        " --user=" + this.rdbms_user +
        " --password=" + this.rdbms_pswd +
        " --host=" + this.rdbms_host +
        " --port=" + this.rdbms_port;

    options = options || {};
    out += (options.batch !== false ? " --batch" : "");
    out += " " + (options.database || this.database);
    return out;
});


module.exports.define("composeMySQLDumpCommand", function (options) {
    var out = "mysqldump" +
        " -u " + this.rdbms_user +
        " -p" + this.rdbms_pswd +
        " -h " + this.rdbms_host +
        " -P" + this.rdbms_port;
    var i;

    options = options || {};
    out += " --skip-comments --skip-opt --quick --add-drop-table --max_allowed_packet=50M --extended-insert";
    // out += " --skip-comments --skip-opt --add-drop-table --extended-insert";
    out += (options.where_clause ? " --where=" + options.where_clause : "");
    out += (options.output_file ? " --result-file=" + options.output_file : "");
    for (i = 0; options.ignore_tables && i < options.ignore_tables.length; i += 1) {
        out += " --ignore-table=" + options.ignore_tables[i];
    }
    out += " " + (options.database || this.database);
    out += (options.tables ? " " + options.tables : "");
    return out;
});
