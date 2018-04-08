/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const Cursor = require('./Cursor').Cursor;
const SqlFieldsCursor = require('./Cursor').SqlFieldsCursor;
const ArgumentChecker = require('./internal/ArgumentChecker');
const BinaryWriter = require('./internal/BinaryWriter');
const BinaryUtils = require('./internal/BinaryUtils');

const PAGE_SIZE_DEFAULT = 1024;

/**
 * ???
 *
 * @hideconstructor
 */
class Query {

    /**
     * ???
     *
     * @param {boolean} local - ???
     *
     * @return {Query} - the same instance of the Query.
     */
    setLocal(local) {
        this._local = local;
        return this;
    }

    /**
     * ???
     *
     * @param {integer} pageSize - ???
     *
     * @return {Query} - the same instance of the Query.
     */
    setPageSize(pageSize) {
        this._pageSize = pageSize;
        return this;
    }

    /** Private methods */

    /**
     * @ignore
     */
    constructor(operation) {
        this._operation = operation;
        this._local = false;
        this._pageSize = PAGE_SIZE_DEFAULT;
    }
}

/**
 * ???
 */
class SqlQuery extends Query {

    /**
     * Public constructor.
     *
     * @param {string} type - ???
     * @param {string} sql - ???
     *
     * @return {SqlQuery} - new SqlQuery instance.
     */
    constructor(type, sql) {
        super(BinaryUtils.OPERATION.QUERY_SQL);
        this.setType(type);
        this.setSql(sql);
        this._args = null;
        this._argTypes = null;
        this._distributedJoins = false;
        this._replicatedOnly = false;
        this._timeout = 0;
    }

    setType(type) {
        if (this instanceof SqlFieldsQuery) {
            ArgumentChecker.invalidArgument(type, 'type', SqlFieldsQuery);
        }
        else {
            ArgumentChecker.notNull(type, 'type');
        }
        this._type = type;
        return this;
    }

    /**
     * ???
     *
     * @param {string} sql - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setSql(sql) {
        ArgumentChecker.notNull(sql, 'sql');
        this._sql = sql;
        return this;
    }

    /**
     * ???
     *
     * @param {...*} args - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setArgs(...args) {
        this._args = args;
        return this;
    }

    /**
     * ???
     *
     * @param {...ObjectType.PRIMITIVE_TYPE | CompositeType} argTypes - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setArgTypes(...argTypes) {
        this._argTypes = argTypes;
        return this;
    }

    /**
     * ???
     *
     * @param {boolean} distributedJoins - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setDistributedJoins(distributedJoins) {
        this._distributedJoins = distributedJoins;
        return this;
    }

    /**
     * ???
     *
     * @param {boolean} replicatedOnly - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setReplicatedOnly(replicatedOnly) {
        this._replicatedOnly = replicatedOnly;
        return this;
    }

    /**
     * ???
     *
     * @param {number} timeout - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setTimeout(timeout) {
        this._timeout = timeout;
        return this;
    }

    /** Private methods */

    /**
     * @ignore
     */
    _write(buffer) {
        BinaryWriter.writeString(buffer, this._type);
        BinaryWriter.writeString(buffer, this._sql);
        this._writeArgs(buffer);
        buffer.writeBoolean(this._distributedJoins);
        buffer.writeBoolean(this._local);
        buffer.writeBoolean(this._replicatedOnly);
        buffer.writeInteger(this._pageSize);
        buffer.writeLong(this._timeout);
    }

    /**
     * @ignore
     */
    _writeArgs(buffer) {
        const argsLength = this._args ? this._args.length : 0;
        buffer.writeInteger(argsLength);
        if (argsLength > 0) {
            let argType;
            for (let i = 0; i < argsLength; i++) {
                argType = this._argTypes && i < this._argTypes.length ? this._argTypes[i] : null;
                BinaryWriter.writeObject(buffer, this._args[i], argType);
            }
        }
    }

    /**
     * @ignore
     */
    _getCursor(socket, payload, keyType = null, valueType = null) {
        const cursor = new Cursor(socket, BinaryUtils.OPERATION.QUERY_SQL_CURSOR_GET_PAGE, keyType, valueType);
        cursor._read(payload);
        return cursor;
    }
}

/**
 * ???
 * @typedef SqlFieldsQuery.STATEMENT_TYPE
 * @enum
 * @readonly
 * @property ANY 0
 * @property SELECT 1
 * @property UPDATE 2
 */
const STATEMENT_TYPE = Object.freeze({
    ANY : 0,
    SELECT : 1,
    UPDATE : 2
});


/**
 * ???
 */
class SqlFieldsQuery extends SqlQuery {

    /**
     * Public constructor.
     *
     * @param {string} sql - ???
     *
     * @return {SqlFieldsQuery} - new SqlFieldsQuery instance.
     */
    constructor(sql) {
        super(null, sql);
        this._operation = BinaryUtils.OPERATION.QUERY_SQL_FIELDS;
        this._schema = null;
        this._maxRows = -1;
        this._statementType = SqlFieldsQuery.STATEMENT_TYPE.ANY;
        this._enforceJoinOrder = false;
        this._collocated = false;
        this._lazy = false;
        this._includeFieldNames = false;
        this._fieldTypes = null;
    }

    static get STATEMENT_TYPE() {
        return STATEMENT_TYPE;
    }

    /**
     * ???
     *
     * @param {string} schema - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setSchema(schema) {
        this._schema = schema;
        return this;
    }

    /**
     * ???
     *
     * @param {integer} maxRows - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setMaxRows(maxRows) {
        this._maxRows = maxRows;
        return this;
    }

    /**
     * ???
     *
     * @param {SqlFieldsQuery.STATEMENT_TYPE} type - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setStatementType(type) {
        this._statementType = type;
        return this;
    }

    /**
     * ???
     *
     * @param {boolean} enforceJoinOrder - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setEnforceJoinOrder(enforceJoinOrder) {
        this._enforceJoinOrder = enforceJoinOrder;
        return this;
    }

    /**
     * ???
     *
     * @param {boolean} collocated - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setCollocated(collocated) {
        this._collocated = collocated;
        return this;
    }

    /**
     * ???
     *
     * @param {boolean} lazy - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setLazy(lazy) {
        this._lazy = lazy;
        return this;
    }

    /**
     * ???
     *
     * @param {boolean} includeFieldNames - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setIncludeFieldNames(includeFieldNames) {
        this._includeFieldNames = includeFieldNames;
        return this;
    }

    /**
     * ???
     *
     * @param {...ObjectType.PRIMITIVE_TYPE | CompositeType} fieldTypes - ???
     *
     * @return {SqlFieldsQuery} - the same instance of the SqlFieldsQuery.
     */
    setFieldTypes(...fieldTypes) {
        this._fieldTypes = fieldTypes;
        return this;
    }

    /** Private methods */

    /**
     * @ignore
     */
    _write(buffer) {
        BinaryWriter.writeString(buffer, this._schema);
        buffer.writeInteger(this._pageSize);
        buffer.writeInteger(this._maxRows);
        BinaryWriter.writeString(buffer, this._sql);
        this._writeArgs(buffer)
        buffer.writeByte(this._statementType);
        buffer.writeBoolean(this._distributedJoins);
        buffer.writeBoolean(this._local);
        buffer.writeBoolean(this._replicatedOnly);
        buffer.writeBoolean(this._enforceJoinOrder);
        buffer.writeBoolean(this._collocated);
        buffer.writeBoolean(this._lazy);
        buffer.writeLong(this._timeout);
        buffer.writeBoolean(this._includeFieldNames);
    }

    /**
     * @ignore
     */
    _getCursor(socket, payload, keyType = null, valueType = null) {
        const cursor = new SqlFieldsCursor(socket, this._fieldTypes);
        cursor._read(payload, true, this._includeFieldNames);
        return cursor;
    }
}

module.exports.SqlQuery = SqlQuery;
module.exports.SqlFieldsQuery = SqlFieldsQuery;
