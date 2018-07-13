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

const ComplexObjectType = require('./ObjectType').ComplexObjectType;
const ObjectArrayType = require('./ObjectType').ObjectArrayType;
const BinaryUtils = require('./internal/BinaryUtils');
const BinaryReader = require('./internal/BinaryReader');
const BinaryWriter = require('./internal/BinaryWriter');
const ArgumentChecker = require('./internal/ArgumentChecker');
const Errors = require('./Errors');

/**
 * Class representing Cache Key part of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class CacheKeyConfiguration {

    /**
     * Public constructor.
     *
     * @param {string} [typeName=null]
     * @param {string} [affinityKeyFieldName=null]
     *
     * @return {CacheKeyConfiguration} - new CacheKeyConfiguration instance.
     */
    constructor(typeName = null, affinityKeyFieldName = null) {
        this._typeName = typeName;
        this._affinityKeyFieldName = affinityKeyFieldName;
    }

    /**
     *
     *
     * @param {string} typeName
     *
     * @return {CacheKeyConfiguration} - the same instance of the CacheKeyConfiguration.
     */
    setTypeName(typeName) {
        this._typeName = typeName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getTypeName() {
        return this._typeName;
    }

    /**
     *
     *
     * @param {string} affinityKeyFieldName
     *
     * @return {CacheKeyConfiguration} - the same instance of the CacheKeyConfiguration.
     */
    setAffinityKeyFieldName(affinityKeyFieldName) {
        this._affinityKeyFieldName = affinityKeyFieldName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getAffinityKeyFieldName() {
        return this._affinityKeyFieldName;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(buffer) {
        await BinaryWriter.writeString(buffer, this._typeName);
        await BinaryWriter.writeString(buffer, this._affinityKeyFieldName);
    }

    /**
     * @ignore
     */
    async _read(buffer) {
        this._typeName = await BinaryReader.readObject(buffer);
        this._affinityKeyFieldName = await BinaryReader.readObject(buffer);
    }
}

/**
 * Class representing one Query Entity element of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class QueryEntity {

    /**
     * Public constructor.
     *
     * @return {QueryEntity} - new QueryEntity instance.
     */
    constructor() {
        this._keyTypeName = null;
        this._valueTypeName = null;
        this._tableName = null;
        this._keyFieldName = null;
        this._valueFieldName = null;
        this._fields = null;
        this._aliases = null;
        this._indexes = null;
    }

    /**
     *
     *
     * @param {string} keyTypeName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setKeyTypeName(keyTypeName) {
        this._keyTypeName = keyTypeName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getKeyTypeName() {
        return this._keyTypeName;
    }

    /**
     *
     *
     * @param {string} valueTypeName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setValueTypeName(valueTypeName) {
        this._valueTypeName = valueTypeName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getValueTypeName() {
        return this._valueTypeName;
    }

    /**
     *
     *
     * @param {string} tableName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setTableName(tableName) {
        this._tableName = tableName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getTableName() {
        return this._tableName;
    }

    /**
     *
     *
     * @param {string} keyFieldName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setKeyFieldName(keyFieldName) {
        this._keyFieldName = keyFieldName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getKeyFieldName() {
        return this._keyFieldName;
    }

    /**
     *
     *
     * @param {string} valueFieldName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setValueFieldName(valueFieldName) {
        this._valueFieldName = valueFieldName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getValueFieldName() {
        return this._valueFieldName;
    }

    /**
     * 
     *
     * @param {Array<QueryField>} fields
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setFields(fields) {
        this._fields = fields;
        return this;
    }

    /**
     * 
     *
     * @return {Array<QueryField>}
     */
    getFields() {
        return this._fields;
    }

    /**
     * 
     *
     * @param {Map<string, string>} aliases
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setAliases(aliases) {
        this._aliases = aliases;
        return this;
    }

    /**
     * 
     *
     * @return {Map<string, string>}
     */
    getAliases() {
        return this._aliases;
    }

    /**
     * 
     *
     * @param {Array<QueryIndex>} indexes
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setIndexes(indexes) {
        this._indexes = indexes;
        return this;
    }

    /**
     * 
     *
     * @return {Array<QueryIndex>}
     */
    getIndexes() {
        return this._indexes;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(buffer) {
        await BinaryWriter.writeString(buffer, this._keyTypeName);
        await BinaryWriter.writeString(buffer, this._valueTypeName);
        await BinaryWriter.writeString(buffer, this._tableName);
        await BinaryWriter.writeString(buffer, this._keyFieldName);
        await BinaryWriter.writeString(buffer, this._valueFieldName);
        await this._writeSubEntities(buffer, this._fields);
        await this._writeAliases(buffer);
        await this._writeSubEntities(buffer, this._indexes);
    }

    /**
     * @ignore
     */
    async _writeAliases(buffer) {
        const length = this._aliases ? this._aliases.size : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let [key, value] of this._aliases.entries()) {
                await BinaryWriter.writeString(buffer, key);
                await BinaryWriter.writeString(buffer, value);
            }
        }
    }

    /**
     * @ignore
     */
    async _writeSubEntities(buffer, entities) {
        const length = entities ? entities.length : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let entity of entities) {
                await entity._write(buffer);
            }
        }
    }

    /**
     * @ignore
     */
    async _read(buffer) {
        this._keyTypeName = await BinaryReader.readObject(buffer);
        this._valueTypeName = await BinaryReader.readObject(buffer);
        this._tableName = await BinaryReader.readObject(buffer);
        this._keyFieldName = await BinaryReader.readObject(buffer);
        this._valueFieldName = await BinaryReader.readObject(buffer);
        this._fields = await this._readSubEntities(buffer, QueryField);
        await this._readAliases(buffer);
        this._indexes = await this._readSubEntities(buffer, QueryIndex);
    }

    /**
     * @ignore
     */
    async _readSubEntities(buffer, objectConstructor) {
        const length = buffer.readInteger(buffer);
        const result = new Array(length);
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                res = new objectConstructor();
                await res._read(buffer);
                result[i] = res;
            }
        }
        return result;
    }

    /**
     * @ignore
     */
    async _readAliases(buffer) {
        const length = buffer.readInteger(buffer);
        this._aliases = new Map();
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                this._aliases.set(await BinaryReader.readObject(buffer), await BinaryReader.readObject(buffer));
            }
        }
    }
}

/**
 * Class representing one Query Field element of {@link QueryEntity} of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class QueryField {

    /**
     * Public constructor.
     *
     * @param {string} [name=null]
     * @param {string} [typeName=null]
     *
     * @return {QueryField} - new QueryField instance.
     */
    constructor(name = null, typeName = null) {
        this._name = name;
        this._typeName = typeName;
        this._isKeyField = false;
        this._isNotNull = false;
        this._defaultValue = undefined;
        this._precision = -1;
        this._scale = -1;
        this._valueType = null;
        this._buffer = null;
        this._index = null;
    }

    /**
     *
     *
     * @param {string} name
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setName(name) {
        this._name = name;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getName() {
        return this._name;
    }

    /**
     *
     *
     * @param {string} typeName
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setTypeName(typeName) {
        this._typeName = typeName;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getTypeName() {
        return this._typeName;
    }

    /**
     *
     *
     * @param {boolean} isKeyField
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setIsKeyField(isKeyField) {
        this._isKeyField = isKeyField;
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getIsKeyField() {
        return this._isKeyField;
    }

    /**
     *
     *
     * @param {boolean} isNotNull
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setIsNotNull(isNotNull) {
        this._isNotNull = isNotNull;
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getIsNotNull() {
        return this._isNotNull;
    }

    /**
     *
     *
     * @param {*} defaultValue
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [valueType=null] - type of the default value:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setDefaultValue(defaultValue, valueType = null) {
        this._defaultValue = defaultValue;
        this._valueType = valueType;
        return this;
    }

    /**
     *
     *
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [valueType=null] - type of the default value:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @async
     *
     * @return {*}
     */
    async getDefaultValue(valueType = null) {
        if (this._defaultValue === undefined) {
            if (this._buffer) {
                const position = this._buffer.position;
                this._buffer.position = this._index;
                const result = await BinaryReader.readObject(this._buffer, valueType);
                this._buffer.position = position;
                return result;
            }
            else {
                return null;
            }
        }
        else {
            return this._defaultValue;
        }
    }

    /**
     *
     *
     * @param {number} precision
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setPrecision(precision) {
        ArgumentChecker.isInteger(precision, 'precision');
        this._precision = precision;
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getPrecision() {
        return this._precision;
    }

    /**
     *
     *
     * @param {number} scale
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setScale(scale) {
        ArgumentChecker.isInteger(scale, 'scale');
        this._scale = scale;
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getScale() {
        return this._scale;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(buffer) {
        await BinaryWriter.writeString(buffer, this._name);
        await BinaryWriter.writeString(buffer, this._typeName);
        buffer.writeBoolean(this._isKeyField);
        buffer.writeBoolean(this._isNotNull);
        await BinaryWriter.writeObject(buffer, this._defaultValue ? this._defaultValue : null, this._valueType);
        buffer.writeInteger(this._precision);
        buffer.writeInteger(this._scale);
    }

    /**
     * @ignore
     */
    async _read(buffer) {
        this._name = await BinaryReader.readObject(buffer);
        this._typeName = await BinaryReader.readObject(buffer);
        this._isKeyField = buffer.readBoolean();
        this._isNotNull = buffer.readBoolean();
        this._defaultValue = undefined;
        this._buffer = buffer;
        this._index = buffer.position;
        await BinaryReader.readObject(buffer);
        this._precision = buffer.readInteger();
        this._scale = buffer.readInteger();
    }
}

/**
 * 
 * @typedef QueryIndex.INDEX_TYPE
 * @enum
 * @readonly
 * @property SORTED 0
 * @property FULLTEXT 1
 * @property GEOSPATIAL 2
 */
 const INDEX_TYPE = Object.freeze({
    SORTED : 0,
    FULLTEXT : 1,
    GEOSPATIAL : 2
});

/**
 * Class representing one Query Index element of {@link QueryEntity} of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class QueryIndex {
    
    /**
     * Public constructor.
     *
     * @param {string} [name=null]
     * @param {string} [typeName=QueryIndex.INDEX_TYPE.SORTED]
     *
     * @return {QueryIndex} - new QueryIndex instance.
     */
    constructor(name = null, type = QueryIndex.INDEX_TYPE.SORTED) {
        this._name = name;
        this.setType(type);
        this._inlineSize = -1;
        this._fields = null;
    }

    static get INDEX_TYPE() {
        return INDEX_TYPE;
    }

    /**
     *
     *
     * @param {string} name
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setName(name) {
        this._name = name;
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getName() {
        return this._name;
    }

    /**
     *
     *
     * @param {QueryIndex.INDEX_TYPE} type
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     *
     * @throws {IgniteClientError} if error.
     */
    setType(type) {
        ArgumentChecker.hasValueFrom(type, 'type', false, QueryIndex.INDEX_TYPE);
        this._type = type;
        return this;
    }

    /**
     *
     *
     * @return {QueryIndex.INDEX_TYPE}
     */
    getType() {
        return this._type;
    }

    /**
     *
     *
     * @param {number} inlineSize
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setInlineSize(inlineSize) {
        this._inlineSize = inlineSize;
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
     getInlineSize() {
        return this._inlineSize;
    }

    /**
     * 
     *
     * @param {Map<string, boolean>} fields
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setFields(fields) {
        this._fields = fields;
        return this;
    }

    /**
     * 
     *
     * @return {Map<string, boolean>}
     */
    getFields() {
        return this._fields;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(buffer) {
        await BinaryWriter.writeString(buffer, this._name);
        buffer.writeByte(this._type);
        buffer.writeInteger(this._inlineSize);
        // write fields
        const length = this._fields ? this._fields.size : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let [key, value] of this._fields.entries()) {
                await BinaryWriter.writeString(buffer, key);
                buffer.writeBoolean(value);
            }
        }
    }

    /**
     * @ignore
     */
    async _read(buffer) {
        this._name = await BinaryReader.readObject(buffer);
        this._type = buffer.readByte();
        this._inlineSize = buffer.readInteger();
        // read fields
        const length = buffer.readInteger(buffer);
        this._fields = new Map();
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                this._fields.set(await BinaryReader.readObject(buffer), buffer.readBoolean());
            }
        }
    }
}

const PROP_NAME = 0;
const PROP_CACHE_MODE = 1;
const PROP_ATOMICITY_MODE = 2;
const PROP_BACKUPS = 3;
const PROP_WRITE_SYNCHRONIZATION_MODE = 4;
const PROP_COPY_ON_READ = 5;
const PROP_READ_FROM_BACKUP = 6;
const PROP_DATA_REGION_NAME = 100;
const PROP_IS_ONHEAP_CACHE_ENABLED = 101;
const PROP_QUERY_ENTITY = 200;
const PROP_QUERY_PARALLELISM = 201;
const PROP_QUERY_DETAIL_METRICS_SIZE = 202;
const PROP_SQL_SCHEMA = 203;
const PROP_SQL_INDEX_INLINE_MAX_SIZE = 204;
const PROP_SQL_ESCAPE_ALL = 205;
const PROP_MAX_QUERY_ITERATORS = 206;
const PROP_REBALANCE_MODE = 300;
const PROP_REBALANCE_DELAY = 301;
const PROP_REBALANCE_TIMEOUT = 302;
const PROP_REBALANCE_BATCH_SIZE = 303;
const PROP_REBALANCE_BATCHES_PREFETCH_COUNT = 304;
const PROP_REBALANCE_ORDER = 305;
const PROP_REBALANCE_THROTTLE = 306;
const PROP_GROUP_NAME = 400;
const PROP_CACHE_KEY_CONFIGURATION = 401;
const PROP_DEFAULT_LOCK_TIMEOUT = 402;
const PROP_MAX_CONCURRENT_ASYNC_OPS = 403;
const PROP_PARTITION_LOSS_POLICY = 404;
const PROP_EAGER_TTL = 405;
const PROP_STATISTICS_ENABLED = 406;

const PROP_TYPES = Object.freeze({
    [PROP_NAME] : BinaryUtils.TYPE_CODE.STRING,
    [PROP_CACHE_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_ATOMICITY_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_BACKUPS] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_WRITE_SYNCHRONIZATION_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_COPY_ON_READ] : BinaryUtils.TYPE_CODE.BOOLEAN,
    [PROP_READ_FROM_BACKUP] : BinaryUtils.TYPE_CODE.BOOLEAN,
    [PROP_DATA_REGION_NAME] : BinaryUtils.TYPE_CODE.STRING,
    [PROP_IS_ONHEAP_CACHE_ENABLED] : BinaryUtils.TYPE_CODE.BOOLEAN,
    [PROP_QUERY_ENTITY] : new ObjectArrayType(new ComplexObjectType(new QueryEntity())),
    [PROP_QUERY_PARALLELISM] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_QUERY_DETAIL_METRICS_SIZE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_SQL_SCHEMA] : BinaryUtils.TYPE_CODE.STRING,
    [PROP_SQL_INDEX_INLINE_MAX_SIZE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_SQL_ESCAPE_ALL] : BinaryUtils.TYPE_CODE.BOOLEAN,
    [PROP_MAX_QUERY_ITERATORS] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_REBALANCE_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_REBALANCE_DELAY] : BinaryUtils.TYPE_CODE.LONG,
    [PROP_REBALANCE_TIMEOUT] : BinaryUtils.TYPE_CODE.LONG,
    [PROP_REBALANCE_BATCH_SIZE] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_REBALANCE_BATCHES_PREFETCH_COUNT] : BinaryUtils.TYPE_CODE.LONG,
    [PROP_REBALANCE_ORDER] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_REBALANCE_THROTTLE] : BinaryUtils.TYPE_CODE.LONG,
    [PROP_GROUP_NAME] : BinaryUtils.TYPE_CODE.STRING,
    [PROP_CACHE_KEY_CONFIGURATION] : new ObjectArrayType(new ComplexObjectType(new CacheKeyConfiguration())),
    [PROP_DEFAULT_LOCK_TIMEOUT] : BinaryUtils.TYPE_CODE.LONG,
    [PROP_MAX_CONCURRENT_ASYNC_OPS] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_PARTITION_LOSS_POLICY] : BinaryUtils.TYPE_CODE.INTEGER,
    [PROP_EAGER_TTL] : BinaryUtils.TYPE_CODE.BOOLEAN,
    [PROP_STATISTICS_ENABLED] : BinaryUtils.TYPE_CODE.BOOLEAN
});

/**
 * 
 * @typedef CacheConfiguration.CACHE_ATOMICITY_MODE
 * @enum
 * @readonly
 * @property TRANSACTIONAL 0
 * @property ATOMIC 1
 */
const CACHE_ATOMICITY_MODE = Object.freeze({
    TRANSACTIONAL : 0,
    ATOMIC : 1
});

/**
 * 
 * @typedef CacheConfiguration.CACHE_MODE
 * @enum
 * @readonly
 * @property LOCAL 0
 * @property REPLICATED 1
 * @property PARTITIONED 2
 */
const CACHE_MODE = Object.freeze({
    LOCAL : 0,
    REPLICATED : 1,
    PARTITIONED : 2
});

/**
 * 
 * @typedef CacheConfiguration.PARTITION_LOSS_POLICY
 * @enum
 * @readonly
 * @property READ_ONLY_SAFE 0
 * @property READ_ONLY_ALL 1
 * @property READ_WRITE_SAFE 2
 * @property READ_WRITE_ALL 3
 * @property IGNORE 4
 */
const PARTITION_LOSS_POLICY = Object.freeze({
    READ_ONLY_SAFE : 0,
    READ_ONLY_ALL : 1,
    READ_WRITE_SAFE : 2,
    READ_WRITE_ALL : 3,
    IGNORE : 4
});

/**
 * 
 * @typedef CacheConfiguration.REABALANCE_MODE
 * @enum
 * @readonly
 * @property SYNC 0
 * @property ASYNC 1
 * @property NONE 2
 */
const REABALANCE_MODE = Object.freeze({
    SYNC : 0,
    ASYNC : 1,
    NONE : 2
});

/**
 * 
 * @typedef CacheConfiguration.WRITE_SYNCHRONIZATION_MODE
 * @enum
 * @readonly
 * @property FULL_SYNC 0
 * @property FULL_ASYNC 1
 * @property PRIMARY_SYNC 2
 */
const WRITE_SYNCHRONIZATION_MODE = Object.freeze({
    FULL_SYNC : 0,
    FULL_ASYNC : 1,
    PRIMARY_SYNC : 2
});

/**
 * Class representing Ignite cache configuration on a server.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class CacheConfiguration {

    /**
     * Public constructor.
     *
     * @return {CacheConfiguration} - new CacheConfiguration instance.
     */
    constructor() {
        this._properties = new Map();
    }

    static get CACHE_ATOMICITY_MODE() {
        return CACHE_ATOMICITY_MODE;
    }

    static get CACHE_MODE() {
        return CACHE_MODE;
    }

    static get PARTITION_LOSS_POLICY() {
        return PARTITION_LOSS_POLICY;
    }

    static get REABALANCE_MODE() {
        return REABALANCE_MODE;
    }

    static get WRITE_SYNCHRONIZATION_MODE() {
        return WRITE_SYNCHRONIZATION_MODE;
    }

    /**
     *
     *
     * @param {CacheConfiguration.CACHE_ATOMICITY_MODE} atomicityMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setAtomicityMode(atomicityMode) {
        ArgumentChecker.hasValueFrom(atomicityMode, 'atomicityMode', false, CACHE_ATOMICITY_MODE);
        this._properties.set(PROP_ATOMICITY_MODE, atomicityMode);
        return this;
    }

    /**
     *
     *
     * @return {CacheConfiguration.CACHE_ATOMICITY_MODE}
     */
    getAtomicityMode() {
        return this._properties.get(PROP_ATOMICITY_MODE);
    }

    /**
     *
     *
     * @param {number} backups
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setBackups(backups) {
        this._properties.set(PROP_BACKUPS, backups);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getBackups() {
        return this._properties.get(PROP_BACKUPS);
    }

    /**
     *
     *
     * @param {CacheConfiguration.CACHE_MODE} cacheMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setCacheMode(cacheMode) {
        ArgumentChecker.hasValueFrom(cacheMode, 'cacheMode', false, CACHE_MODE);
        this._properties.set(PROP_CACHE_MODE, cacheMode);
        return this;
    }

    /**
     *
     *
     * @return {CacheConfiguration.CACHE_MODE}
     */
    getCacheMode() {
        return this._properties.get(PROP_CACHE_MODE);
    }

    /**
     *
     *
     * @param {boolean} copyOnRead
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setCopyOnRead(copyOnRead) {
        this._properties.set(PROP_COPY_ON_READ, copyOnRead);
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getCopyOnRead() {
        return this._properties.get(PROP_COPY_ON_READ);
    }

    /**
     *
     *
     * @param {string} dataRegionName
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setDataRegionName(dataRegionName) {
        this._properties.set(PROP_DATA_REGION_NAME, dataRegionName);
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getDataRegionName() {
        return this._properties.get(PROP_DATA_REGION_NAME);
    }

    /**
     *
     *
     * @param {boolean} eagerTtl
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setEagerTtl(eagerTtl) {
        this._properties.set(PROP_EAGER_TTL, eagerTtl);
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getEagerTtl() {
        return this._properties.get(PROP_EAGER_TTL);
    }

    /**
     *
     *
     * @param {boolean} statisticsEnabled
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setStatisticsEnabled(statisticsEnabled) {
        this._properties.set(PROP_STATISTICS_ENABLED, statisticsEnabled);
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getStatisticsEnabled() {
        return this._properties.get(PROP_STATISTICS_ENABLED);
    }

    /**
     *
     *
     * @param {string} groupName
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setGroupName(groupName) {
        this._properties.set(PROP_GROUP_NAME, groupName);
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getGroupName() {
        return this._properties.get(PROP_GROUP_NAME);
    }

    /**
     *
     *
     * @param {number} lockTimeout
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setDefaultLockTimeout(lockTimeout) {
        this._properties.set(PROP_DEFAULT_LOCK_TIMEOUT, lockTimeout);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getDefaultLockTimeout() {
        return this._properties.get(PROP_DEFAULT_LOCK_TIMEOUT);
    }

    /**
     *
     *
     * @param {number} maxConcurrentAsyncOperations
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setMaxConcurrentAsyncOperations(maxConcurrentAsyncOperations) {
        this._properties.set(PROP_MAX_CONCURRENT_ASYNC_OPS, maxConcurrentAsyncOperations);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getMaxConcurrentAsyncOperations() {
        return this._properties.get(PROP_MAX_CONCURRENT_ASYNC_OPS);
    }

    /**
     *
     *
     * @param {number} maxQueryIterators
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setMaxQueryIterators(maxQueryIterators) {
        this._properties.set(PROP_MAX_QUERY_ITERATORS, maxQueryIterators);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getMaxQueryIterators() {
        return this._properties.get(PROP_MAX_QUERY_ITERATORS);
    }

    /**
     *
     *
     * @param {boolean} isOnheapCacheEnabled
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setIsOnheapCacheEnabled(isOnheapCacheEnabled) {
        this._properties.set(PROP_IS_ONHEAP_CACHE_ENABLED, isOnheapCacheEnabled);
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getIsOnheapCacheEnabled() {
        return this._properties.get(PROP_IS_ONHEAP_CACHE_ENABLED);
    }

    /**
     *
     *
     * @param {CacheConfiguration.PARTITION_LOSS_POLICY} partitionLossPolicy
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setPartitionLossPolicy(partitionLossPolicy) {
        ArgumentChecker.hasValueFrom(partitionLossPolicy, 'partitionLossPolicy', false, PARTITION_LOSS_POLICY);
        this._properties.set(PROP_PARTITION_LOSS_POLICY, partitionLossPolicy);
        return this;
    }

    /**
     *
     *
     * @return {CacheConfiguration.PARTITION_LOSS_POLICY}
     */
    getPartitionLossPolicy() {
        return this._properties.get(PROP_PARTITION_LOSS_POLICY);
    }

    /**
     *
     *
     * @param {number} queryDetailMetricsSize
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setQueryDetailMetricsSize(queryDetailMetricsSize) {
        this._properties.set(PROP_QUERY_DETAIL_METRICS_SIZE, queryDetailMetricsSize);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getQueryDetailMetricsSize() {
        return this._properties.get(PROP_QUERY_DETAIL_METRICS_SIZE);
    }

    /**
     *
     *
     * @param {number} queryParallelism
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setQueryParallelism(queryParallelism) {
        this._properties.set(PROP_QUERY_PARALLELISM, queryParallelism);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getQueryParallelism() {
        return this._properties.get(PROP_QUERY_PARALLELISM);
    }

    /**
     *
     *
     * @param {boolean} readFromBackup
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setReadFromBackup(readFromBackup) {
        this._properties.set(PROP_READ_FROM_BACKUP, readFromBackup);
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getReadFromBackup() {
        return this._properties.get(PROP_READ_FROM_BACKUP);
    }

    /**
     *
     *
     * @param {number} rebalanceBatchSize
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceBatchSize(rebalanceBatchSize) {
        this._properties.set(PROP_REBALANCE_BATCH_SIZE, rebalanceBatchSize);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getRebalanceBatchSize() {
        return this._properties.get(PROP_REBALANCE_BATCH_SIZE);
    }

    /**
     *
     *
     * @param {number} rebalanceBatchesPrefetchCount
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceBatchesPrefetchCount(rebalanceBatchesPrefetchCount) {
        this._properties.set(PROP_REBALANCE_BATCHES_PREFETCH_COUNT, rebalanceBatchesPrefetchCount);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getRebalanceBatchesPrefetchCount() {
        return this._properties.get(PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
    }

    /**
     *
     *
     * @param {number} rebalanceDelay
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceDelay(rebalanceDelay) {
        this._properties.set(PROP_REBALANCE_DELAY, rebalanceDelay);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getRebalanceDelay() {
        return this._properties.get(PROP_REBALANCE_DELAY);
    }

    /**
     *
     *
     * @param {CacheConfiguration.REABALANCE_MODE} rebalanceMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setRebalanceMode(rebalanceMode) {
        ArgumentChecker.hasValueFrom(rebalanceMode, 'rebalanceMode', false, REABALANCE_MODE);
        this._properties.set(PROP_REBALANCE_MODE, rebalanceMode);
        return this;
    }

    /**
     *
     *
     * @return {CacheConfiguration.REABALANCE_MODE}
     */
    getRebalanceMode() {
        return this._properties.get(PROP_REBALANCE_MODE);
    }

    /**
     *
     *
     * @param {number} rebalanceOrder
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceOrder(rebalanceOrder) {
        this._properties.set(PROP_REBALANCE_ORDER, rebalanceOrder);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getRebalanceOrder() {
        return this._properties.get(PROP_REBALANCE_ORDER);
    }

    /**
     *
     *
     * @param {number} rebalanceThrottle
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceThrottle(rebalanceThrottle) {
        this._properties.set(PROP_REBALANCE_THROTTLE, rebalanceThrottle);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getRebalanceThrottle() {
        return this._properties.get(PROP_REBALANCE_THROTTLE);
    }

    /**
     *
     *
     * @param {number} rebalanceTimeout
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceTimeout(rebalanceTimeout) {
        this._properties.set(PROP_REBALANCE_TIMEOUT, rebalanceTimeout);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getRebalanceTimeout() {
        return this._properties.get(PROP_REBALANCE_TIMEOUT);
    }

    /**
     *
     *
     * @param {boolean} sqlEscapeAll
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlEscapeAll(sqlEscapeAll) {
        this._properties.set(PROP_SQL_ESCAPE_ALL, sqlEscapeAll);
        return this;
    }

    /**
     *
     *
     * @return {boolean}
     */
    getSqlEscapeAll() {
        return this._properties.get(PROP_SQL_ESCAPE_ALL);
    }

    /**
     *
     *
     * @param {number} sqlIndexInlineMaxSize
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlIndexInlineMaxSize(sqlIndexInlineMaxSize) {
        this._properties.set(PROP_SQL_INDEX_INLINE_MAX_SIZE, sqlIndexInlineMaxSize);
        return this;
    }

    /**
     *
     *
     * @return {number}
     */
    getSqlIndexInlineMaxSize() {
        return this._properties.get(PROP_SQL_INDEX_INLINE_MAX_SIZE);
    }

    /**
     *
     *
     * @param {string} sqlSchema
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlSchema(sqlSchema) {
        this._properties.set(PROP_SQL_SCHEMA, sqlSchema);
        return this;
    }

    /**
     *
     *
     * @return {string}
     */
    getSqlSchema() {
        return this._properties.get(PROP_SQL_SCHEMA);
    }

    /**
     *
     *
     * @param {CacheConfiguration.WRITE_SYNCHRONIZATION_MODE} writeSynchronizationMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setWriteSynchronizationMode(writeSynchronizationMode) {
        ArgumentChecker.hasValueFrom(writeSynchronizationMode, 'writeSynchronizationMode', false, WRITE_SYNCHRONIZATION_MODE);
        this._properties.set(PROP_WRITE_SYNCHRONIZATION_MODE, writeSynchronizationMode);
        return this;
    }

    /**
     *
     *
     * @return {CacheConfiguration.WRITE_SYNCHRONIZATION_MODE}
     */
    getWriteSynchronizationMode() {
        return this._properties.get(PROP_WRITE_SYNCHRONIZATION_MODE);
    }

    /**
     * 
     *
     * @param {...CacheKeyConfiguration} keyConfigurations
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setKeyConfigurations(...keyConfigurations) {
        ArgumentChecker.hasType(keyConfigurations, 'keyConfigurations', true, CacheKeyConfiguration);
        this._properties.set(PROP_CACHE_KEY_CONFIGURATION, keyConfigurations);
        return this;
    }

    /**
     * 
     *
     * @return {Array<CacheKeyConfiguration>}
     */
    getKeyConfigurations() {
        return this._properties.get(PROP_CACHE_KEY_CONFIGURATION);
    }

    /**
     * 
     *
     * @param {...QueryEntity} queryEntities
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setQueryEntities(...queryEntities) {
        ArgumentChecker.hasType(queryEntities, 'queryEntities', true, QueryEntity);
        this._properties.set(PROP_QUERY_ENTITY, queryEntities);
        return this;
    }

    /**
     * 
     *
     * @return {Array<QueryEntity>}
     */
    getQueryEntities() {
        return this._properties.get(PROP_QUERY_ENTITY);
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(buffer, name) {
        this._properties.set(PROP_NAME, name);

        const startPos = buffer.position;
        buffer.position = buffer.position +
            BinaryUtils.getSize(BinaryUtils.TYPE_CODE.INTEGER) +
            BinaryUtils.getSize(BinaryUtils.TYPE_CODE.SHORT);

        for (let [propertyCode, property] of this._properties) {
            await this._writeProperty(buffer, propertyCode, property);
        }

        const length = buffer.position - startPos;
        buffer.position = startPos;

        buffer.writeInteger(length);
        buffer.writeShort(this._properties.size);
    }

    /**
     * @ignore
     */
    async _writeProperty(buffer, propertyCode, property) {
        buffer.writeShort(propertyCode);
        const propertyType = PROP_TYPES[propertyCode];
        switch (BinaryUtils.getTypeCode(propertyType)) {
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                await BinaryWriter.writeObject(buffer, property, propertyType, false);
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                await BinaryWriter.writeObject(buffer, property, propertyType);
                return;
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                const length = property ? property.length : 0;
                buffer.writeInteger(length);
                for (let prop of property) {
                    await prop._write(buffer);
                }
                return;
            default:
                throw Errors.IgniteClientError.internalError();
        }
    }

    /**
     * @ignore
     */
    async _read(buffer) {
        // length
        buffer.readInteger();
        await this._readProperty(buffer, PROP_ATOMICITY_MODE);
        await this._readProperty(buffer, PROP_BACKUPS);
        await this._readProperty(buffer, PROP_CACHE_MODE);
        await this._readProperty(buffer, PROP_COPY_ON_READ);
        await this._readProperty(buffer, PROP_DATA_REGION_NAME);
        await this._readProperty(buffer, PROP_EAGER_TTL);
        await this._readProperty(buffer, PROP_STATISTICS_ENABLED);
        await this._readProperty(buffer, PROP_GROUP_NAME);
        await this._readProperty(buffer, PROP_DEFAULT_LOCK_TIMEOUT);
        await this._readProperty(buffer, PROP_MAX_CONCURRENT_ASYNC_OPS);
        await this._readProperty(buffer, PROP_MAX_QUERY_ITERATORS);
        await this._readProperty(buffer, PROP_NAME);
        await this._readProperty(buffer, PROP_IS_ONHEAP_CACHE_ENABLED);
        await this._readProperty(buffer, PROP_PARTITION_LOSS_POLICY);
        await this._readProperty(buffer, PROP_QUERY_DETAIL_METRICS_SIZE);
        await this._readProperty(buffer, PROP_QUERY_PARALLELISM);
        await this._readProperty(buffer, PROP_READ_FROM_BACKUP);
        await this._readProperty(buffer, PROP_REBALANCE_BATCH_SIZE);
        await this._readProperty(buffer, PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
        await this._readProperty(buffer, PROP_REBALANCE_DELAY);
        await this._readProperty(buffer, PROP_REBALANCE_MODE);
        await this._readProperty(buffer, PROP_REBALANCE_ORDER);
        await this._readProperty(buffer, PROP_REBALANCE_THROTTLE);
        await this._readProperty(buffer, PROP_REBALANCE_TIMEOUT);
        await this._readProperty(buffer, PROP_SQL_ESCAPE_ALL);
        await this._readProperty(buffer, PROP_SQL_INDEX_INLINE_MAX_SIZE);
        await this._readProperty(buffer, PROP_SQL_SCHEMA);
        await this._readProperty(buffer, PROP_WRITE_SYNCHRONIZATION_MODE);
        await this._readProperty(buffer, PROP_CACHE_KEY_CONFIGURATION);
        await this._readProperty(buffer, PROP_QUERY_ENTITY);
    }

    /**
     * @ignore
     */
    async _readProperty(buffer, propertyCode) {
        const propertyType = PROP_TYPES[propertyCode];
        switch (BinaryUtils.getTypeCode(propertyType)) {
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                this._properties.set(propertyCode, await BinaryReader._readTypedObject(buffer, propertyType));
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                this._properties.set(propertyCode, await BinaryReader.readObject(buffer, propertyType));
                return;
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                const length = buffer.readInteger();
                if (length > 0) {
                    const properties = new Array(length);
                    for (let i = 0; i < length; i++) {
                        const property = new propertyType._elementType._objectConstructor();
                        await property._read(buffer);
                        properties[i] = property;
                    }
                    this._properties.set(propertyCode, properties);
                }
                return;
            default:
                throw Errors.IgniteClientError.internalError();
        }
    }
}

module.exports = CacheConfiguration;
module.exports.QueryEntity = QueryEntity;
module.exports.QueryField = QueryField;
module.exports.QueryIndex = QueryIndex;
module.exports.CacheKeyConfiguration = CacheKeyConfiguration;
