/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
const BinaryCommunicator = require('./internal/BinaryCommunicator');
const ArgumentChecker = require('./internal/ArgumentChecker');
const Errors = require('./Errors');

/**
 * Class representing Cache Key part of GridGain {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class CacheKeyConfiguration {

    /**
     * Public constructor.
     *
     * @param {string} [typeName=null] - Type name for which affinity field name is being defined.
     * @param {string} [affinityKeyFieldName=null] - Affinity key field name.
     *
     * @return {CacheKeyConfiguration} - new CacheKeyConfiguration instance.
     */
    constructor(typeName = null, affinityKeyFieldName = null) {
        this._typeName = typeName;
        this._affinityKeyFieldName = affinityKeyFieldName;
    }

    /**
     * Sets type name for which affinity field name is being defined.
     *
     * @param {string} typeName - Type name for which affinity field name is being defined.
     *
     * @return {CacheKeyConfiguration} - the same instance of the CacheKeyConfiguration.
     */
    setTypeName(typeName) {
        this._typeName = typeName;
        return this;
    }

    /**
     * Gets type name for which affinity field name is being defined.
     *
     * @return {string} - Type name for which affinity field name is being defined.
     */
    getTypeName() {
        return this._typeName;
    }

    /**
     * Sets affinity key field name.
     *
     * @param {string} affinityKeyFieldName - Affinity key field name.
     *
     * @return {CacheKeyConfiguration} - the same instance of the CacheKeyConfiguration.
     */
    setAffinityKeyFieldName(affinityKeyFieldName) {
        this._affinityKeyFieldName = affinityKeyFieldName;
        return this;
    }

    /**
     * Gets affinity key field name.
     *
     * @return {string} - Affinity key field name.
     */
    getAffinityKeyFieldName() {
        return this._affinityKeyFieldName;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(communicator, buffer) {
        BinaryCommunicator.writeString(buffer, this._typeName);
        BinaryCommunicator.writeString(buffer, this._affinityKeyFieldName);
    }

    /**
     * @ignore
     */
    async _read(communicator, buffer) {
        this._typeName = BinaryCommunicator.readString(buffer);
        this._affinityKeyFieldName = BinaryCommunicator.readString(buffer);
    }
}

/**
 * Class representing one Query Entity element of GridGain {@link CacheConfiguration}.
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
     * Sets key type name for this query entity.
     *
     * @param {string} keyTypeName - Key type name.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setKeyTypeName(keyTypeName) {
        this._keyTypeName = keyTypeName;
        return this;
    }

    /**
     * Gets key type name for this query entity.
     *
     * @return {string} - Key type name.
     */
    getKeyTypeName() {
        return this._keyTypeName;
    }

    /**
     * Sets value type name for this query entity.
     *
     * @param {string} valueTypeName - Value type name.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setValueTypeName(valueTypeName) {
        this._valueTypeName = valueTypeName;
        return this;
    }

    /**
     * Gets value type name for this query entity.
     *
     * @return {string} - Value type name.
     */
    getValueTypeName() {
        return this._valueTypeName;
    }

    /**
     * Sets table name for this query entity.
     *
     * @param {string} tableName - Table name.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setTableName(tableName) {
        this._tableName = tableName;
        return this;
    }

    /**
     * Gets table name for this query entity.
     *
     * @return {string} - Table name.
     */
    getTableName() {
        return this._tableName;
    }

    /**
     * Sets key field name.
     *
     * @param {string} keyFieldName - Key name.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setKeyFieldName(keyFieldName) {
        this._keyFieldName = keyFieldName;
        return this;
    }

    /**
     * Gets key field name.
     *
     * @return {string} - Key name.
     */
    getKeyFieldName() {
        return this._keyFieldName;
    }

    /**
     * Sets value field name.
     *
     * @param {string} valueFieldName - Value name.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setValueFieldName(valueFieldName) {
        this._valueFieldName = valueFieldName;
        return this;
    }

    /**
     * Gets value field name.
     *
     * @return {string} - Value name.
     */
    getValueFieldName() {
        return this._valueFieldName;
    }

    /**
     * Sets query fields for this query pair. The order of the fields is important as it
     * defines the order of columns returned by the 'select *' queries.
     *
     * @param {Array<QueryField>} fields - Array of query fields.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setFields(fields) {
        this._fields = fields;
        return this;
    }

    /**
     * Gets query fields for this query pair. The order of the fields is
     * defines the order of columns returned by the 'select *' queries.
     *
     * @return {Array<QueryField>} - Array of query fields.
     */
    getFields() {
        return this._fields;
    }

    /**
     * Sets mapping from a full property name in dot notation to an alias that will be
     * used as SQL column name. Example: {"parent.name" -> "parentName"}.
     *
     * @param {Map<string, string>} aliases - Aliases map.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setAliases(aliases) {
        this._aliases = aliases;
        return this;
    }

    /**
     * Gets aliases map.
     *
     * @return {Map<string, string>} - Aliases map.
     */
    getAliases() {
        return this._aliases;
    }

    /**
     * Sets a collection of index entities.
     *
     * @param {Array<QueryIndex>} indexes - Collection (array) of index entities.
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setIndexes(indexes) {
        this._indexes = indexes;
        return this;
    }

    /**
     * Gets a collection of index entities.
     *
     * @return {Array<QueryIndex>} - Collection (array) of index entities.
     */
    getIndexes() {
        return this._indexes;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(communicator, buffer) {
        BinaryCommunicator.writeString(buffer, this._keyTypeName);
        BinaryCommunicator.writeString(buffer, this._valueTypeName);
        BinaryCommunicator.writeString(buffer, this._tableName);
        BinaryCommunicator.writeString(buffer, this._keyFieldName);
        BinaryCommunicator.writeString(buffer, this._valueFieldName);
        await this._writeSubEntities(communicator, buffer, this._fields);
        await this._writeAliases(communicator, buffer);
        await this._writeSubEntities(communicator, buffer, this._indexes);
    }

    /**
     * @ignore
     */
    async _writeAliases(communicator, buffer) {
        const length = this._aliases ? this._aliases.size : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let [key, value] of this._aliases.entries()) {
                BinaryCommunicator.writeString(buffer, key);
                BinaryCommunicator.writeString(buffer, value);
            }
        }
    }

    /**
     * @ignore
     */
    async _writeSubEntities(communicator, buffer, entities) {
        const length = entities ? entities.length : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let entity of entities) {
                await entity._write(communicator, buffer);
            }
        }
    }

    /**
     * @ignore
     */
    async _read(communicator, buffer) {
        this._keyTypeName = await communicator.readObject(buffer);
        this._valueTypeName = await communicator.readObject(buffer);
        this._tableName = await communicator.readObject(buffer);
        this._keyFieldName = await communicator.readObject(buffer);
        this._valueFieldName = await communicator.readObject(buffer);
        this._fields = await this._readSubEntities(communicator, buffer, QueryField);
        await this._readAliases(communicator, buffer);
        this._indexes = await this._readSubEntities(communicator, buffer, QueryIndex);
    }

    /**
     * @ignore
     */
    async _readSubEntities(communicator, buffer, objectConstructor) {
        const length = buffer.readInteger(buffer);
        const result = new Array(length);
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                res = new objectConstructor();
                await res._read(communicator, buffer);
                result[i] = res;
            }
        }
        return result;
    }

    /**
     * @ignore
     */
    async _readAliases(communicator, buffer) {
        const length = buffer.readInteger(buffer);
        this._aliases = new Map();
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                this._aliases.set(await communicator.readObject(buffer), await communicator.readObject(buffer));
            }
        }
    }
}

/**
 * Class representing one Query Field element of {@link QueryEntity} of GridGain {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class QueryField {

    /**
     * Public constructor.
     *
     * @param {string} [name=null] - Query field name.
     * @param {string} [typeName=null] - Query field type name.
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
        this._communicator = null;
        this._buffer = null;
        this._index = null;
    }

    /**
     * Sets query field name.
     *
     * @param {string} name - Query field name.
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setName(name) {
        this._name = name;
        return this;
    }

    /**
     * Gets query field name.
     *
     * @return {string} - Query field name.
     */
    getName() {
        return this._name;
    }

    /**
     * Sets query field type name.
     *
     * @param {string} typeName - Query field type name.
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setTypeName(typeName) {
        this._typeName = typeName;
        return this;
    }

    /**
     * Gets query field type name.
     *
     * @return {string} - Query field type name.
     */
    getTypeName() {
        return this._typeName;
    }

    /**
     * Sets if it is a key query field or not.
     *
     * @param {boolean} isKeyField - True to make this query field a key field. False otherwise.
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setIsKeyField(isKeyField) {
        this._isKeyField = isKeyField;
        return this;
    }

    /**
     * Gets if it is a key query field or not.
     *
     * @return {boolean} - True if this query field is a key field. False otherwise.
     */
    getIsKeyField() {
        return this._isKeyField;
    }

    /**
     * Sets if this query field must be checked for null.
     *
     * @param {boolean} isNotNull - True if this query field must be checked for null. False otherwise.
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setIsNotNull(isNotNull) {
        this._isNotNull = isNotNull;
        return this;
    }

    /**
     * Gets if this query field must be checked for null.
     *
     * @return {boolean} - True if this query field must be checked for null. False otherwise.
     */
    getIsNotNull() {
        return this._isNotNull;
    }

    /**
     * Sets query field default value.
     *
     * @param {*} defaultValue - Query field default value.
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
     * Gets query field default value.
     *
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [valueType=null] - type of the default value:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @async
     *
     * @return {*} - Query field default value.
     */
    async getDefaultValue(valueType = null) {
        if (this._defaultValue === undefined) {
            if (this._buffer) {
                const position = this._buffer.position;
                this._buffer.position = this._index;
                const result = await this._communicator.readObject(this._buffer, valueType);
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
     * Sets query field precision.
     *
     * @param {number} precision - Query field precision.
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setPrecision(precision) {
        ArgumentChecker.isInteger(precision, 'precision');
        this._precision = precision;
        return this;
    }

    /**
     * Gets query field precision.
     *
     * @return {number} - Query field precision.
     */
    getPrecision() {
        return this._precision;
    }

    /**
     * Sets query field scale.
     *
     * @param {number} scale - Query field scale.
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setScale(scale) {
        ArgumentChecker.isInteger(scale, 'scale');
        this._scale = scale;
        return this;
    }

    /**
     * Gets query field scale.
     *
     * @return {number} - Query field scale.
     */
    getScale() {
        return this._scale;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(communicator, buffer) {
        BinaryCommunicator.writeString(buffer, this._name);
        BinaryCommunicator.writeString(buffer, this._typeName);
        buffer.writeBoolean(this._isKeyField);
        buffer.writeBoolean(this._isNotNull);
        await communicator.writeObject(buffer, this._defaultValue ? this._defaultValue : null, this._valueType);
        buffer.writeInteger(this._precision);
        buffer.writeInteger(this._scale);
    }

    /**
     * @ignore
     */
    async _read(communicator, buffer) {
        this._name = await communicator.readObject(buffer);
        this._typeName = await communicator.readObject(buffer);
        this._isKeyField = buffer.readBoolean();
        this._isNotNull = buffer.readBoolean();
        this._defaultValue = undefined;
        this._communicator = communicator;
        this._buffer = buffer;
        this._index = buffer.position;
        await communicator.readObject(buffer);
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
 * Class representing one Query Index element of {@link QueryEntity} of GridGain {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class QueryIndex {

    /**
     * Public constructor.
     *
     * @param {string} [name=null] - Query index name.
     * @param {string} [typeName=QueryIndex.INDEX_TYPE.SORTED] - Query index type name.
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
     * Sets query index name. Will be automatically set if not provided by a user.
     *
     * @param {string} name - Query index name.
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setName(name) {
        this._name = name;
        return this;
    }

    /**
     * Gets query index name.
     *
     * @return {string} - Query index name.
     */
    getName() {
        return this._name;
    }

    /**
     * Sets query index type.
     *
     * @param {QueryIndex.INDEX_TYPE} type - Query index type.
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
     * Gets query index type.
     *
     * @return {QueryIndex.INDEX_TYPE} - Query index type.
     */
    getType() {
        return this._type;
    }

    /**
     * Sets index inline size in bytes. When enabled, a part of the indexed value is placed directly
     * to the index pages, thus minimizing data page accesses and increasing query performance. Allowed values:
     *   - -1 (default) - determine inline size automatically (see below)
     *   - 0 - index inline is disabled (not recommended)
     *   - positive value - fixed index inline
     *
     * When set to -1, Ignite will try to detect inline size automatically. It will be no more than
     * CacheConfiguration.getSqlIndexInlineMaxSize(). Index inline will be enabled for all fixed-length types,
     * but will not be enabled for String.
     *
     * @param {number} inlineSize - Index inline size in bytes.
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setInlineSize(inlineSize) {
        this._inlineSize = inlineSize;
        return this;
    }

    /**
     * Gets index inline size in bytes.
     *
     * @return {number} - Index inline size in bytes.
     */
    getInlineSize() {
        return this._inlineSize;
    }

    /**
     * Sets fields included in the index.
     *
     * @param {Map<string, boolean>} fields - Map of the index fields.
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setFields(fields) {
        this._fields = fields;
        return this;
    }

    /**
     * Gets fields included in the index.
     *
     * @return {Map<string, boolean>} - Map of the index fields.
     */
    getFields() {
        return this._fields;
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(communicator, buffer) {
        BinaryCommunicator.writeString(buffer, this._name);
        buffer.writeByte(this._type);
        buffer.writeInteger(this._inlineSize);
        // write fields
        const length = this._fields ? this._fields.size : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let [key, value] of this._fields.entries()) {
                BinaryCommunicator.writeString(buffer, key);
                buffer.writeBoolean(value);
            }
        }
    }

    /**
     * @ignore
     */
    async _read(communicator, buffer) {
        this._name = await communicator.readObject(buffer);
        this._type = buffer.readByte();
        this._inlineSize = buffer.readInteger();
        // read fields
        const length = buffer.readInteger(buffer);
        this._fields = new Map();
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                this._fields.set(await communicator.readObject(buffer), buffer.readBoolean());
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
 * Class representing GridGain cache configuration on a server.
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
     * Sets cache atomicity mode.
     *
     * @param {CacheConfiguration.CACHE_ATOMICITY_MODE} atomicityMode - Cache atomicity mode.
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
     * Gets cache atomicity mode.
     *
     * @return {CacheConfiguration.CACHE_ATOMICITY_MODE} - Cache atomicity mode.
     */
    getAtomicityMode() {
        return this._properties.get(PROP_ATOMICITY_MODE);
    }

    /**
     * Sets number of nodes used to back up single partition for {@link CacheConfiguration.CACHE_MODE}.PARTITIONED cache.
     *
     * @param {number} backups - Number of backup nodes for one partition.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setBackups(backups) {
        this._properties.set(PROP_BACKUPS, backups);
        return this;
    }

    /**
     * Gets number of nodes used to back up single partition for {@link CacheConfiguration.CACHE_MODE}.PARTITIONED cache.
     *
     * @return {number} - Number of backup nodes for one partition.
     */
    getBackups() {
        return this._properties.get(PROP_BACKUPS);
    }

    /**
     * Sets caching mode.
     *
     * @param {CacheConfiguration.CACHE_MODE} cacheMode - Caching mode.
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
     * Gets caching mode.
     *
     * @return {CacheConfiguration.CACHE_MODE} - Caching mode.
     */
    getCacheMode() {
        return this._properties.get(PROP_CACHE_MODE);
    }

    /**
     * Sets the flag indicating whether a copy of the value stored in the on-heap cache
     * should be created for a cache operation return the value. Also, if this flag
     * is set, copies are created for values passed to CacheInterceptor and to CacheEntryProcessor.
     * If the on-heap cache is disabled then this flag is of no use.
     *
     * @param {boolean} copyOnRead - Copy on read flag.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setCopyOnRead(copyOnRead) {
        this._properties.set(PROP_COPY_ON_READ, copyOnRead);
        return this;
    }

    /**
     * Gets copy on read flag.
     *
     * @return {boolean} - Copy on read flag.
     */
    getCopyOnRead() {
        return this._properties.get(PROP_COPY_ON_READ);
    }

    /**
     * Sets a name of DataRegionConfiguration for this cache.
     *
     * @param {string} dataRegionName - DataRegionConfiguration name. Can be null
     *  (default DataRegionConfiguration will be used) but should not be empty.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setDataRegionName(dataRegionName) {
        this._properties.set(PROP_DATA_REGION_NAME, dataRegionName);
        return this;
    }

    /**
     * Gets the name of DataRegionConfiguration for this cache.
     *
     * @return {string} - DataRegionConfiguration name.
     */
    getDataRegionName() {
        return this._properties.get(PROP_DATA_REGION_NAME);
    }

    /**
     * Sets eager ttl flag. If there is at least one cache configured with this flag set to true,
     * GridGain will create a single thread to clean up expired entries in background.
     * When flag is set to false, expired entries will be removed on next entry access.
     *
     * @param {boolean} eagerTtl - True if GridGain should eagerly remove expired cache entries.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setEagerTtl(eagerTtl) {
        this._properties.set(PROP_EAGER_TTL, eagerTtl);
        return this;
    }

    /**
     * Gets eager ttl flag. If there is at least one cache configured with this flag set to true,
     * GridGain will create a single thread to clean up expired entries in background.
     * When flag is set to false, expired entries will be removed on next entry access.
     *
     * @return {boolean} - Flag indicating whether GridGain will eagerly remove expired entries.
     */
    getEagerTtl() {
        return this._properties.get(PROP_EAGER_TTL);
    }

    /**
     * Enables or disables statistics for this cache.
     *
     * @param {boolean} statisticsEnabled - True to enable, false to disable.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setStatisticsEnabled(statisticsEnabled) {
        this._properties.set(PROP_STATISTICS_ENABLED, statisticsEnabled);
        return this;
    }

    /**
     * Gets if statistics are enabled for this cache.
     *
     * @return {boolean} - True if enabled, false if disabled.
     */
    getStatisticsEnabled() {
        return this._properties.get(PROP_STATISTICS_ENABLED);
    }

    /**
     * Sets the cache group name. Caches with the same group name share single underlying 'physical' cache
     * (partition set), but are logically isolated. Grouping caches reduces overall overhead, since
     * internal data structures are shared.
     *
     * @param {string} groupName - Cache group name.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setGroupName(groupName) {
        this._properties.set(PROP_GROUP_NAME, groupName);
        return this;
    }

    /**
     * Gets the cache group name.
     *
     * @return {string} - Cache group name.
     */
    getGroupName() {
        return this._properties.get(PROP_GROUP_NAME);
    }

    /**
     * Sets default lock timeout in milliseconds.
     *
     * @param {number} lockTimeout - Default lock timeout.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setDefaultLockTimeout(lockTimeout) {
        this._properties.set(PROP_DEFAULT_LOCK_TIMEOUT, lockTimeout);
        return this;
    }

    /**
     * Gets default lock acquisition timeout.
     *
     * @return {number} - Default lock timeout.
     */
    getDefaultLockTimeout() {
        return this._properties.get(PROP_DEFAULT_LOCK_TIMEOUT);
    }

    /**
     * Sets maximum number of allowed concurrent asynchronous operations. 0 - the number of concurrent asynchronous
     * operations is unlimited.
     *
     * @param {number} maxConcurrentAsyncOperations - Maximum number of concurrent asynchronous operations.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setMaxConcurrentAsyncOperations(maxConcurrentAsyncOperations) {
        this._properties.set(PROP_MAX_CONCURRENT_ASYNC_OPS, maxConcurrentAsyncOperations);
        return this;
    }

    /**
     * Gets maximum number of allowed concurrent asynchronous operations.
     * If 0 returned then number of concurrent asynchronous operations is unlimited.
     *
     * @return {number} - Maximum number of concurrent asynchronous operations or 0 if unlimited.
     */
    getMaxConcurrentAsyncOperations() {
        return this._properties.get(PROP_MAX_CONCURRENT_ASYNC_OPS);
    }

    /**
     * Sets maximum number of query iterators that can be stored. Iterators are stored to support query
     * pagination when each page of data is sent to user's node only on demand. Increase this property
     * if you are running and processing lots of queries in parallel.
     *
     * @param {number} maxQueryIterators - Maximum number of query iterators that can be stored.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setMaxQueryIterators(maxQueryIterators) {
        this._properties.set(PROP_MAX_QUERY_ITERATORS, maxQueryIterators);
        return this;
    }

    /**
     * Gets maximum number of query iterators that can be stored.
     *
     * @return {number} - Maximum number of query iterators that can be stored.
     */
    getMaxQueryIterators() {
        return this._properties.get(PROP_MAX_QUERY_ITERATORS);
    }

    /**
     * Enables/disables on-heap cache for the off-heap based page memory.
     *
     * @param {boolean} isOnheapCacheEnabled - On-heap cache enabled flag.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setIsOnheapCacheEnabled(isOnheapCacheEnabled) {
        this._properties.set(PROP_IS_ONHEAP_CACHE_ENABLED, isOnheapCacheEnabled);
        return this;
    }

    /**
     * Checks if the on-heap cache is enabled for the off-heap based page memory.
     *
     * @return {boolean} - On-heap cache enabled flag.
     */
    getIsOnheapCacheEnabled() {
        return this._properties.get(PROP_IS_ONHEAP_CACHE_ENABLED);
    }

    /**
     * Sets partition loss policy. This policy defines how Ignite will react to a situation when
     * all nodes for some partition leave the cluster.
     *
     * @param {CacheConfiguration.PARTITION_LOSS_POLICY} partitionLossPolicy - Partition loss policy.
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
     * Gets partition loss policy. This policy defines how Ignite will react to a situation when
     * all nodes for some partition leave the cluster.
     *
     * @return {CacheConfiguration.PARTITION_LOSS_POLICY} - Partition loss policy.
     */
    getPartitionLossPolicy() {
        return this._properties.get(PROP_PARTITION_LOSS_POLICY);
    }

    /**
     * Sets size of queries detail metrics that will be stored in memory for monitoring purposes.
     * If 0, then history will not be collected. Note, larger number may lead to higher memory consumption.
     *
     * @param {number} queryDetailMetricsSize - Maximum number of latest queries metrics that will be stored in memory.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setQueryDetailMetricsSize(queryDetailMetricsSize) {
        this._properties.set(PROP_QUERY_DETAIL_METRICS_SIZE, queryDetailMetricsSize);
        return this;
    }

    /**
     * Gets size of queries detail metrics that will be stored in memory for monitoring purposes.
     * If 0, then history will not be collected. Note, larger number may lead to higher memory consumption.
     *
     * @return {number} - Maximum number of query metrics that will be stored in memory.
     */
    getQueryDetailMetricsSize() {
        return this._properties.get(PROP_QUERY_DETAIL_METRICS_SIZE);
    }

    /**
     * Defines a hint to query execution engine on desired degree of parallelism within a single node.
     * Query executor may or may not use this hint depending on estimated query costs.
     * Query executor may define certain restrictions on parallelism depending on query type and/or cache type.
     *
     * @param {number} queryParallelism - Query parallelism.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setQueryParallelism(queryParallelism) {
        this._properties.set(PROP_QUERY_PARALLELISM, queryParallelism);
        return this;
    }

    /**
     * Gets query parallelism parameter which is a hint to query execution engine on desired degree of
     * parallelism within a single node.
     *
     * @return {number} - Query parallelism.
     */
    getQueryParallelism() {
        return this._properties.get(PROP_QUERY_PARALLELISM);
    }

    /**
     * Sets read from backup flag.
     *
     * @param {boolean} readFromBackup - True to allow reads from backups. False - data always
     * should be read from primary node and never from backup.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setReadFromBackup(readFromBackup) {
        this._properties.set(PROP_READ_FROM_BACKUP, readFromBackup);
        return this;
    }

    /**
     * Gets flag indicating whether data can be read from backup.
     *
     * @return {boolean} - true if data can be read from backup node or false if data always
     *  should be read from primary node and never from backup.
     */
    getReadFromBackup() {
        return this._properties.get(PROP_READ_FROM_BACKUP);
    }

    /**
     * Sets rebalance batch size (to be loaded within a single rebalance message). Rebalancing algorithm will split
     * total data set on every node into multiple batches prior to sending data.
     *
     * @param {number} rebalanceBatchSize - Rebalance batch size (size in bytes of a single rebalance message).
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceBatchSize(rebalanceBatchSize) {
        this._properties.set(PROP_REBALANCE_BATCH_SIZE, rebalanceBatchSize);
        return this;
    }

    /**
     * Gets size (in number bytes) to be loaded within a single rebalance message.
     *
     * @return {number} - Size in bytes of a single rebalance message.
     */
    getRebalanceBatchSize() {
        return this._properties.get(PROP_REBALANCE_BATCH_SIZE);
    }

    /**
     * To gain better rebalancing performance supplier node can provide more than one batch at rebalancing start
     * and provide one new to each next demand request. Sets number of batches generated by supply node at
     * rebalancing start. Minimum is 1.
     *
     * @param {number} rebalanceBatchesPrefetchCount - Batches count.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceBatchesPrefetchCount(rebalanceBatchesPrefetchCount) {
        this._properties.set(PROP_REBALANCE_BATCHES_PREFETCH_COUNT, rebalanceBatchesPrefetchCount);
        return this;
    }

    /**
     * To gain better rebalancing performance supplier node can provide more than one batch at rebalancing start
     * and provide one new to each next demand request. Gets number of batches generated by supply node at
     * rebalancing start. Minimum is 1.
     *
     * @return {number} - Batches count.
     */
    getRebalanceBatchesPrefetchCount() {
        return this._properties.get(PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
    }

    /**
     * Sets delay in milliseconds upon a node joining or leaving topology (or crash) after which rebalancing should be
     * started automatically. Rebalancing should be delayed if you plan to restart nodes after they leave topology,
     * or if you plan to start multiple nodes at once or one after another and don't want to repartition and rebalance
     * until all nodes are started.
     *
     * @param {number} rebalanceDelay - Rebalance delay to set. 0 to start rebalancing immediately,
     *  -1 to start rebalancing manually, or positive value to specify delay in milliseconds after which rebalancing
     *  should start automatically.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceDelay(rebalanceDelay) {
        this._properties.set(PROP_REBALANCE_DELAY, rebalanceDelay);
        return this;
    }

    /**
     * Gets rebalance delay.
     *
     * @return {number} - Rebalance delay.
     */
    getRebalanceDelay() {
        return this._properties.get(PROP_REBALANCE_DELAY);
    }

    /**
     * Sets rebalance mode for distributed cache.
     *
     * @param {CacheConfiguration.REABALANCE_MODE} rebalanceMode - Rebalance mode.
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
     * Gets rebalance mode for distributed cache.
     *
     * @return {CacheConfiguration.REABALANCE_MODE} - Rebalance mode.
     */
    getRebalanceMode() {
        return this._properties.get(PROP_REBALANCE_MODE);
    }

    /**
     * Sets cache rebalance order. Rebalance order can be set to non-zero value for caches with SYNC or
     * ASYNC rebalance modes only. If cache rebalance order is positive, rebalancing for this cache will be started
     * only when rebalancing for all caches with smaller rebalance order will be completed.
     *
     * @param {number} rebalanceOrder - Cache rebalance order.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceOrder(rebalanceOrder) {
        this._properties.set(PROP_REBALANCE_ORDER, rebalanceOrder);
        return this;
    }

    /**
     * Gets cache rebalance order.
     *
     * @return {number} - Cache rebalance order.
     */
    getRebalanceOrder() {
        return this._properties.get(PROP_REBALANCE_ORDER);
    }

    /**
     * Sets time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network. This parameter
     * helps tune the amount of time to wait between rebalance messages to make sure that rebalancing process does not
     * have any negative performance impact.
     *
     * @param {number} rebalanceThrottle - Time in millis to wait between rebalance messages, 0 to disable throttling.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceThrottle(rebalanceThrottle) {
        this._properties.set(PROP_REBALANCE_THROTTLE, rebalanceThrottle);
        return this;
    }

    /**
     * Gets time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.
     *
     * @return {number} - Time in millis to wait between rebalance messages, 0 - throttling disabled.
     */
    getRebalanceThrottle() {
        return this._properties.get(PROP_REBALANCE_THROTTLE);
    }

    /**
     * Sets rebalance timeout (ms).
     *
     * @param {number} rebalanceTimeout - Rebalance timeout (ms).
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceTimeout(rebalanceTimeout) {
        this._properties.set(PROP_REBALANCE_TIMEOUT, rebalanceTimeout);
        return this;
    }

    /**
     * Gets rebalance timeout (ms).
     *
     * @return {number} - Rebalance timeout (ms).
     */
    getRebalanceTimeout() {
        return this._properties.get(PROP_REBALANCE_TIMEOUT);
    }

    /**
     * Sets sqlEscapeAll flag. If true all the SQL table and field names will be escaped with double quotes like
     * ("tableName"."fieldsName"). This enforces case sensitivity for field names and also allows having special
     * characters in table and field names.
     *
     * @param {boolean} sqlEscapeAll - Flag value.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlEscapeAll(sqlEscapeAll) {
        this._properties.set(PROP_SQL_ESCAPE_ALL, sqlEscapeAll);
        return this;
    }

    /**
     * Gets sqlEscapeAll flag.
     *
     * @return {boolean} - Flag value.
     */
    getSqlEscapeAll() {
        return this._properties.get(PROP_SQL_ESCAPE_ALL);
    }

    /**
     * Sets maximum inline size for sql indexes.
     *
     * @param {number} sqlIndexInlineMaxSize - Maximum payload size for offheap indexes.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlIndexInlineMaxSize(sqlIndexInlineMaxSize) {
        this._properties.set(PROP_SQL_INDEX_INLINE_MAX_SIZE, sqlIndexInlineMaxSize);
        return this;
    }

    /**
     * Gets maximum inline size for sql indexes.
     *
     * @return {number} - Maximum payload size for offheap indexes.
     */
    getSqlIndexInlineMaxSize() {
        return this._properties.get(PROP_SQL_INDEX_INLINE_MAX_SIZE);
    }

    /**
     * Sets sql schema to be used for current cache. This name will correspond to SQL ANSI-99 standard. Nonquoted
     * identifiers are not case sensitive. Quoted identifiers are case sensitive. Be aware of using the same string
     * in case sensitive and case insensitive manner simultaneously, since behaviour for such case is not specified.
     * When sqlSchema is not specified, quoted cacheName is used instead. sqlSchema could not be an empty string.
     * Has to be "\"\"" (quoted empty string) instead.
     *
     * @param {string} sqlSchema - Schema name for current cache according to SQL ANSI-99. Should not be null.
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlSchema(sqlSchema) {
        this._properties.set(PROP_SQL_SCHEMA, sqlSchema);
        return this;
    }

    /**
     * Gets custom name of the sql schema. If custom sql schema is not set then undefined will be returned and quoted
     * case sensitive name will be used as sql schema.
     *
     * @return {string} - Schema name for current cache according to SQL ANSI-99. Could be undefined.
     */
    getSqlSchema() {
        return this._properties.get(PROP_SQL_SCHEMA);
    }

    /**
     * Sets write synchronization mode. Default synchronization mode is
     * {@link CacheConfiguration.WRITE_SYNCHRONIZATION_MODE}.PRIMARY_SYNC.
     *
     * @param {CacheConfiguration.WRITE_SYNCHRONIZATION_MODE} writeSynchronizationMode - Write synchronization mode
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
     * Gets write synchronization mode. This mode controls whether the main caller should wait for update on
     * other nodes to complete or not.
     *
     * @return {CacheConfiguration.WRITE_SYNCHRONIZATION_MODE} - Write synchronization mode.
     */
    getWriteSynchronizationMode() {
        return this._properties.get(PROP_WRITE_SYNCHRONIZATION_MODE);
    }

    /**
     * Sets cache key configurations.
     *
     * @param {...CacheKeyConfiguration} keyConfigurations - Cache key configurations.
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
     * Gets cache key configurations.
     *
     * @return {Array<CacheKeyConfiguration>} - Array of cache key configurations.
     */
    getKeyConfigurations() {
        return this._properties.get(PROP_CACHE_KEY_CONFIGURATION);
    }

    /**
     * Sets query entities configuration.
     *
     * @param {...QueryEntity} queryEntities - Query entities configuration.
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
     * Gets a collection (array) of configured query entities.
     *
     * @return {Array<QueryEntity>} - Array of query entities configurations.
     */
    getQueryEntities() {
        return this._properties.get(PROP_QUERY_ENTITY);
    }

    /** Private methods */

    /**
     * @ignore
     */
    async _write(communicator, buffer, name) {
        this._properties.set(PROP_NAME, name);

        const startPos = buffer.position;
        buffer.position = buffer.position +
            BinaryUtils.getSize(BinaryUtils.TYPE_CODE.INTEGER) +
            BinaryUtils.getSize(BinaryUtils.TYPE_CODE.SHORT);

        for (let [propertyCode, property] of this._properties) {
            await this._writeProperty(communicator, buffer, propertyCode, property);
        }

        const length = buffer.position - startPos;
        buffer.position = startPos;

        buffer.writeInteger(length);
        buffer.writeShort(this._properties.size);
    }

    /**
     * @ignore
     */
    async _writeProperty(communicator, buffer, propertyCode, property) {
        buffer.writeShort(propertyCode);
        const propertyType = PROP_TYPES[propertyCode];
        switch (BinaryUtils.getTypeCode(propertyType)) {
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                await communicator.writeObject(buffer, property, propertyType, false);
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                await communicator.writeObject(buffer, property, propertyType);
                return;
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                const length = property ? property.length : 0;
                buffer.writeInteger(length);
                for (let prop of property) {
                    await prop._write(communicator, buffer);
                }
                return;
            default:
                throw Errors.IgniteClientError.internalError();
        }
    }

    /**
     * @ignore
     */
    async _read(communicator, buffer) {
        // length
        buffer.readInteger();
        await this._readProperty(communicator, buffer, PROP_ATOMICITY_MODE);
        await this._readProperty(communicator, buffer, PROP_BACKUPS);
        await this._readProperty(communicator, buffer, PROP_CACHE_MODE);
        await this._readProperty(communicator, buffer, PROP_COPY_ON_READ);
        await this._readProperty(communicator, buffer, PROP_DATA_REGION_NAME);
        await this._readProperty(communicator, buffer, PROP_EAGER_TTL);
        await this._readProperty(communicator, buffer, PROP_STATISTICS_ENABLED);
        await this._readProperty(communicator, buffer, PROP_GROUP_NAME);
        await this._readProperty(communicator, buffer, PROP_DEFAULT_LOCK_TIMEOUT);
        await this._readProperty(communicator, buffer, PROP_MAX_CONCURRENT_ASYNC_OPS);
        await this._readProperty(communicator, buffer, PROP_MAX_QUERY_ITERATORS);
        await this._readProperty(communicator, buffer, PROP_NAME);
        await this._readProperty(communicator, buffer, PROP_IS_ONHEAP_CACHE_ENABLED);
        await this._readProperty(communicator, buffer, PROP_PARTITION_LOSS_POLICY);
        await this._readProperty(communicator, buffer, PROP_QUERY_DETAIL_METRICS_SIZE);
        await this._readProperty(communicator, buffer, PROP_QUERY_PARALLELISM);
        await this._readProperty(communicator, buffer, PROP_READ_FROM_BACKUP);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_BATCH_SIZE);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_DELAY);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_MODE);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_ORDER);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_THROTTLE);
        await this._readProperty(communicator, buffer, PROP_REBALANCE_TIMEOUT);
        await this._readProperty(communicator, buffer, PROP_SQL_ESCAPE_ALL);
        await this._readProperty(communicator, buffer, PROP_SQL_INDEX_INLINE_MAX_SIZE);
        await this._readProperty(communicator, buffer, PROP_SQL_SCHEMA);
        await this._readProperty(communicator, buffer, PROP_WRITE_SYNCHRONIZATION_MODE);
        await this._readProperty(communicator, buffer, PROP_CACHE_KEY_CONFIGURATION);
        await this._readProperty(communicator, buffer, PROP_QUERY_ENTITY);
    }

    /**
     * @ignore
     */
    async _readProperty(communicator, buffer, propertyCode) {
        const propertyType = PROP_TYPES[propertyCode];
        switch (BinaryUtils.getTypeCode(propertyType)) {
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                this._properties.set(propertyCode, await communicator._readTypedObject(buffer, propertyType));
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                this._properties.set(propertyCode, await communicator.readObject(buffer, propertyType));
                return;
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                const length = buffer.readInteger();
                if (length > 0) {
                    const properties = new Array(length);
                    for (let i = 0; i < length; i++) {
                        const property = new propertyType._elementType._objectConstructor();
                        await property._read(communicator, buffer);
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
