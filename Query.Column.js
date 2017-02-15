"use strict";

var Core = require("lapis-core/index.js");

/**
* To represent a SQL column stucture
*/
module.exports = Core.Base.clone({
    id: "Column",
    active: true,
});


/**
* To get the Object representing the column
*/
module.exports.define("get", function () {
    return this.value;
});


/**
* To get a number value from one field of the column, defaulted to def_val arg if blank
* @param def_val to use as default value if blank
* @return Number value
*/
module.exports.define("getNumber", function (def_val) {
    var number_val = parseFloat(this.get());
    if (isNaN(number_val) && def_val !== undefined) {
        number_val = def_val;
    }
    return number_val;
});


/**
* To get a date object from one field of the column, defaulted to def_val arg if blank
* @param def_val to use as default value if blank
* @return Date object
*/
module.exports.define("getDate", function (def_val) {
    var date = Date.parse(this.get());
    if (!this.get()) {
        date = def_val || null;
    }
    return date;
});


/**
* To select the current column to sort up the table
*/
module.exports.define("sortTop", function () {
    var max_sort_seq;
    var this_column;
    if (this.sort_seq === 0) {
        return;
    }
    this.sort_seq = 0;
    max_sort_seq = this.query.max_sort_seq;
    this_column = this;
    this.query.doColumns(function (column) {
        if (column !== this_column && typeof column.sort_seq === "number") {
            column.sort_seq += 1;
            if (column.sort_seq > max_sort_seq) {
                delete column.sort_seq;
            }
        }
    });
});


/**
* To select the current column to sort bottom the table
*/
module.exports.define("sortBottom", function () {
    var max_curr_sort_seq = -1;
    this.query.doColumns(function (column) {
        if (typeof column.sort_seq === "number" && column.sort_seq > max_curr_sort_seq) {
            max_curr_sort_seq = column.sort_seq;
        }
    });
    if (max_curr_sort_seq < this.query.max_sort_seq) {
        this.sort_seq = max_curr_sort_seq + 1;
    }
});


/**
* To set the column ordering as ascending
*/
module.exports.define("sortAsc", function () {
    this.sort_desc = false;
});


/**
* To set the column ordering as descending
*/
module.exports.define("sortDesc", function () {
    this.sort_desc = true;
});


/**
* To remove the column ordering
*/
module.exports.define("sortRemove", function () {
    this.query.doColumns(function (column) {
        if (typeof column.sort_seq === "number" && column.sort_seq > this.sort_seq) {
            column.sort_seq -= 1;
        }
    });
    delete this.sort_seq;
    delete this.sort_desc;
});


/**
* To get the corresponding sort sql term to make the sql statement.
*/
module.exports.define("getOrderTerm", function () {
    return (this.order_term || this.name) + (this.sort_desc ? " DESC" : "");
});
