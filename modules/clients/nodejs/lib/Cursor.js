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

const Errors = require('./Errors');
const CacheEntry = require('./CacheClient').CacheEntry;
const BinaryUtils = require('./internal/BinaryUtils');
const BinaryReader = require('./internal/BinaryReader');
const BinaryWriter = require('./internal/BinaryWriter');

/**
 * ???
 *
 * @hideconstructor
 */
class Cursor {

    /**
     * ???
     *
     * @return {boolean} - ???
     */
    hasNext() {
        if (this._value) {
            return true;
        }
        return this._hasNext;
    }

    /**
     * ???
     *
     * @async
     *
     * @raturn {Promise<Array<*>>} -
     */
    async getNext() {
        let value = null;
        if (!this._value && this._hasNext) {
            await this._getNext();
        }
        value = this._value;
        this._value = null;
        return value;
    }

    /**
     * ???
     *
     * @async
     *
     * @raturn {Promise<Array<*>>} -
     */
    async getAll() {
        let result = new Array();
        while (this.hasNext()) {
            result = result.concat(await this.getNext());
        }
        return result;
    }

    /**
     * ???
     *
     * @async
     */
    async close() {
        // Close cursor only if the server has more pages: the server closes cursor automatically on last page
        if (this._id && this._hasNext) {
            await this._socket.send(
                BinaryUtils.OPERATION.RESOURCE_CLOSE,
                (payload) => {
                    this._write(payload);
                });
        }
    }

    /** Private methods */

    /**
     * @ignore
     */
    constructor(socket, operation, keyType = null, valueType = null) {
        this._socket = socket;
        this._operation = operation;
        this._keyType = keyType;
        this._valueType = valueType;
        this._id = null;
        this._hasNext = false;
        this._value = null;
    }

    /**
     * @ignore
     */
    async _getNext() {
        this._hasNext = false;
        this._value = null;
        await this._socket.send(
            this._operation,
            (payload) => {
                this._write(payload);
            },
            (payload) => {
                this._read(payload);
            });
    }

    /**
     * @ignore
     */
    _write(buffer) {
        buffer.writeLong(this._id);
    }


    /**
     * @ignore
     */
    _read(buffer) {
        const id = buffer.readLong();
        if (this._id) {
            if (!this._id.equals(id)) {
                throw Errors.IgniteClientError.internalError();
            }
        }
        else {
            this._id = id;
        }
        this._rowCount = buffer.readInteger();
        this._value = new Array(this._rowCount);
        for (let i = 0; i < this._rowCount; i++) {
            this._value[i] = new CacheEntry(
                BinaryReader.readObject(buffer, this._keyType),
                BinaryReader.readObject(buffer, this._valueType));
        }
        this._hasNext = buffer.readBoolean();
    }
}

/**
 * ???
 *
 * @hideconstructor
 */
class SqlFieldsCursor extends Cursor {

    /**
     * ???
     *
     * @return {Array<string>} - ???
     */
    getFieldNames() {
        return this._fieldNames;
    }

    /** Private methods */

    /**
     * @ignore
     */
    constructor(socket, fieldTypes) {
        super(socket, BinaryUtils.OPERATION.QUERY_SQL_FIELDS_CURSOR_GET_PAGE);
        this._fieldTypes = fieldTypes;
        this._fieldNames = null;
    }

    /**
     * @ignore
     */
    _read(buffer, initial = false, includeFieldNames = false) {
        if (initial) {
            this._id = buffer.readLong();
            this._fieldCount = buffer.readInteger();
            if (includeFieldNames) {
                this._fieldNames = new Array(this._fieldCount);
                for (let i = 0; i < this._fieldCount; i++) {
                    this._fieldNames[i] = BinaryReader.readObject(buffer);
                }
            }
        }
        this._rowCount = buffer.readInteger();
        this._value = new Array(this._rowCount);
        let values;
        for (let i = 0; i < this._rowCount; i++) {
            values = new Array(this._fieldCount);
            for (let j = 0; j < this._fieldCount; j++) {
                const fieldType = this._fieldTypes && j < this._fieldTypes.length ? this._fieldTypes[j] : null;
                values[j] = BinaryReader.readObject(buffer, fieldType);
            }
            this._value[i] = values;
        }
        this._hasNext = buffer.readBoolean();
    }
}

module.exports.Cursor = Cursor;
module.exports.SqlFieldsCursor = SqlFieldsCursor;
