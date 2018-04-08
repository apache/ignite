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


class CacheKeyConfiguration {
    constructor() {
        this._typeName = null;
        this._affinityKeyFieldName = null;
    }

    setTypeName(typeName) {
        this._typeName = typeName;
        return this;
    }

    getTypeName() {
        return this._typeName;
    }

    setAffinityKeyFieldName(affinityKeyFieldName) {
        this._affinityKeyFieldName = affinityKeyFieldName;
        return this;
    }

    getAffinityKeyFieldName() {
        return this._affinityKeyFieldName;
    }

    /** Private methods */

    _write(buffer) {
        BinaryWriter.writeString(buffer, this._typeName);
        BinaryWriter.writeString(buffer, this._affinityKeyFieldName);
    }

    _read(buffer) {
        this._typeName = BinaryReader.readString(buffer);
        this._affinityKeyFieldName = BinaryReader.readString(buffer);
    }
}

class QueryEntity {

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

    setKeyTypeName(keyTypeName) {
        this._keyTypeName = keyTypeName;
        return this;
    }

    setValueTypeName(valueTypeName) {
        this._valueTypeName = valueTypeName;
        return this;
    }

    setTableName(tableName) {
        this._tableName = tableName;
        return this;
    }

    setKeyFieldName(keyFieldName) {
        this._keyFieldName = keyFieldName;
        return this;
    }

    setValueFieldName(valueFieldName) {
        this._valueFieldName = valueFieldName;
        return this;
    }

    /**
     * ???
     *
     * @param {Array<QueryField>} fields - ???
     */
    setFields(fields) {
        this._fields = fields;
        return this;
    }

    /**
     * ???
     *
     * @param {Map<String, String>} aliases - ???
     */
    setAliases(aliases) {
        this._aliases = aliases;
        return this;
    }

    setIndexes(indexes) {
        this._indexes = indexes;
        return this;
    }

    /** Private methods */

    _write(buffer) {
        BinaryWriter.writeString(buffer, this._keyTypeName);
        BinaryWriter.writeString(buffer, this._valueTypeName);
        BinaryWriter.writeString(buffer, this._tableName);
        BinaryWriter.writeString(buffer, this._keyFieldName);
        BinaryWriter.writeString(buffer, this._valueFieldName);
        this._writeSubEntities(buffer, this._fields);
        this._writeAliases(buffer);
        this._writeSubEntities(buffer, this._indexes);
    }

    _writeAliases(buffer) {
        const length = this._aliases ? this._aliases.size : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            this._aliases.forEach((value, key) => {
                BinaryWriter.writeString(buffer, key);
                BinaryWriter.writeString(buffer, value);
            });
        }
    }

    _writeSubEntities(buffer, entities) {
        const length = entities ? entities.length : 0;
        buffer.writeInteger(length);
        if (length > 0) {
            for (let entity of entities) {
                entity._write(buffer);
            }
        }
    }

    _read(buffer) {
        this._keyTypeName = BinaryReader.readString(buffer);
        this._valueTypeName = BinaryReader.readString(buffer);
        this._tableName = BinaryReader.readString(buffer);
        this._keyFieldName = BinaryReader.readString(buffer);
        this._valueFieldName = BinaryReader.readString(buffer);
        this._fields = this._readSubEntities(buffer, QueryField);
        this._readAliases(buffer);
        this._indexes = this._readSubEntities(buffer/*, ???!!!*/);
    }

    _readSubEntities(buffer, objectConstructor) {
        const length = buffer.readInteger(buffer);
        const result = new Array(length);
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                res = new objectConstructor();
                res._read(buffer);
                result[i] = res;
            }
        }
        return result;
    }

    _readAliases(buffer) {
        const length = buffer.readInteger(buffer);
        this._aliases = new Map();
        if (length > 0) {
            let res;
            for (let i = 0; i < length; i++) {
                this._aliases.set(BinaryReader.readString(buffer), BinaryReader.readString(buffer));
            }
        }
    }
}

class QueryField {
    constructor(name, typeName) {
        this._name = name;
        this._typeName = typeName;
        this._isKeyField = false;
        this._isNotNull = false;
        this._defaultValue = null;
        this._valueType = null;
    }

    setIsKeyField(isKeyField) {
        this._isKeyField = isKeyField;
        return this;
    }

    setIsNotNull(isNotNull) {
        this._isNotNull = isNotNull;
        return this;
    }

    setDefaultValue(defaultValue, valueType = null) {
        this._defaultValue = defaultValue;
        this._valueType = valueType;
    }

    /** Private methods */

    _write(buffer) {
        BinaryWriter.writeString(buffer, this._name);
        BinaryWriter.writeString(buffer, this._typeName);
        buffer.writeBoolean(this._isKeyField);
        buffer.writeBoolean(this._isNotNull);
        BinaryWriter.writeObject(buffer, this._defaultValue, this._valueType);
    }

    _read(buffer) {
        this._name = BinaryReader.readString(buffer);
        this._typeName = BinaryReader.readString(buffer);
        this._isKeyField = buffer.readBoolean();
        this._isNotNull = buffer.readBoolean();
        this._defaultValue = BinaryReader.readObject(buffer, this._valueType);
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

const CACHE_ATOMICITY_MODE = Object.freeze({
    TRANSACTIONAL : 0,
    ATOMIC : 1
});

const CACHE_MODE = Object.freeze({
    LOCAL : 0,
    REPLICATED : 1,
    PARTITIONED : 2
});

const PARTITION_LOSS_POLICY = Object.freeze({
    READ_ONLY_SAFE : 0,
    READ_ONLY_ALL : 1,
    READ_WRITE_SAFE : 2,
    READ_WRITE_ALL : 3,
    IGNORE : 4
});

const REABALANCE_MODE = Object.freeze({
    SYNC : 0,
    ASYNC : 1,
    NONE : 2
});

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

    setAtomicityMode(atomicityMode) {
        ArgumentChecker.hasValueFrom(atomicityMode, 'atomicityMode', false, CACHE_ATOMICITY_MODE);
        this._properties.set(PROP_ATOMICITY_MODE, atomicityMode);
        return this;
    }

    getAtomicityMode() {
        return this._properties.get(PROP_ATOMICITY_MODE);
    }

    setBackups(backups) {
        this._properties.set(PROP_BACKUPS, backups);
        return this;
    }

    getBackups() {
        return this._properties.get(PROP_BACKUPS);
    }

    setCacheMode(cacheMode) {
        ArgumentChecker.hasValueFrom(cacheMode, 'cacheMode', false, CACHE_MODE);
        this._properties.set(PROP_CACHE_MODE, cacheMode);
        return this;
    }

    getCacheMode() {
        return this._properties.get(PROP_CACHE_MODE);
    }

    setCopyOnRead(copyOnRead) {
        this._properties.set(PROP_COPY_ON_READ, copyOnRead);
        return this;
    }

    getCopyOnRead() {
        return this._properties.get(PROP_COPY_ON_READ);
    }

    setDataRegionName(dataRegionName) {
        this._properties.set(PROP_DATA_REGION_NAME, dataRegionName);
        return this;
    }

    getDataRegionName() {
        return this._properties.get(PROP_DATA_REGION_NAME);
    }

    setEagerTtl(eagerTtl) {
        this._properties.set(PROP_EAGER_TTL, eagerTtl);
        return this;
    }

    getEagerTtl() {
        return this._properties.get(PROP_EAGER_TTL);
    }

    setStatisticsEnabled(statisticsEnabled) {
        this._properties.set(PROP_STATISTICS_ENABLED, statisticsEnabled);
        return this;
    }

    getStatisticsEnabled() {
        return this._properties.get(PROP_STATISTICS_ENABLED);
    }

    setGroupName(groupName) {
        this._properties.set(PROP_GROUP_NAME, groupName);
        return this;
    }

    getGroupName() {
        return this._properties.get(PROP_GROUP_NAME);
    }

    setDefaultLockTimeout(lockTimeout) {
        this._properties.set(PROP_DEFAULT_LOCK_TIMEOUT, lockTimeout);
        return this;
    }

    getDefaultLockTimeout() {
        return this._properties.get(PROP_DEFAULT_LOCK_TIMEOUT);
    }

    setMaxConcurrentAsyncOperations(maxConcurrentAsyncOperations) {
        this._properties.set(PROP_MAX_CONCURRENT_ASYNC_OPS, maxConcurrentAsyncOperations);
        return this;
    }

    getMaxConcurrentAsyncOperations() {
        return this._properties.get(PROP_MAX_CONCURRENT_ASYNC_OPS);
    }

    setMaxQueryIterators(maxQueryIterators) {
        this._properties.set(PROP_MAX_QUERY_ITERATORS, maxQueryIterators);
        return this;
    }

    getMaxQueryIterators() {
        return this._properties.get(PROP_MAX_QUERY_ITERATORS);
    }

    setIsOnheapCacheEnabled(isOnheapCacheEnabled) {
        this._properties.set(PROP_IS_ONHEAP_CACHE_ENABLED, isOnheapCacheEnabled);
        return this;
    }

    getIsOnheapCacheEnabled() {
        return this._properties.get(PROP_IS_ONHEAP_CACHE_ENABLED);
    }

    setPartitionLossPolicy(partitionLossPolicy) {
        ArgumentChecker.hasValueFrom(partitionLossPolicy, 'partitionLossPolicy', false, PARTITION_LOSS_POLICY);
        this._properties.set(PROP_PARTITION_LOSS_POLICY, partitionLossPolicy);
        return this;
    }

    getPartitionLossPolicy() {
        return this._properties.get(PROP_PARTITION_LOSS_POLICY);
    }

    setQueryDetailMetricsSize(queryDetailMetricsSize) {
        this._properties.set(PROP_QUERY_DETAIL_METRICS_SIZE, queryDetailMetricsSize);
        return this;
    }

    getQueryDetailMetricsSize() {
        return this._properties.get(PROP_QUERY_DETAIL_METRICS_SIZE);
    }

    setQueryParallelism(queryParallelism) {
        this._properties.set(PROP_QUERY_PARALLELISM, queryParallelism);
        return this;
    }

    getQueryParallelism() {
        return this._properties.get(PROP_QUERY_PARALLELISM);
    }

    setReadFromBackup(readFromBackup) {
        this._properties.set(PROP_READ_FROM_BACKUP, readFromBackup);
        return this;
    }

    getReadFromBackup() {
        return this._properties.get(PROP_READ_FROM_BACKUP);
    }

    setRebalanceBatchSize(rebalanceBatchSize) {
        this._properties.set(PROP_REBALANCE_BATCH_SIZE, rebalanceBatchSize);
        return this;
    }

    getRebalanceBatchSize() {
        return this._properties.get(PROP_REBALANCE_BATCH_SIZE);
    }

    setRebalanceBatchesPrefetchCount(rebalanceBatchesPrefetchCount) {
        this._properties.set(PROP_REBALANCE_BATCHES_PREFETCH_COUNT, rebalanceBatchesPrefetchCount);
        return this;
    }

    getRebalanceBatchesPrefetchCount() {
        return this._properties.get(PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
    }

    setRebalanceDelay(rebalanceDelay) {
        this._properties.set(PROP_REBALANCE_DELAY, rebalanceDelay);
        return this;
    }

    getRebalanceDelay() {
        return this._properties.get(PROP_REBALANCE_DELAY);
    }

    setRebalanceMode(rebalanceMode) {
        ArgumentChecker.hasValueFrom(rebalanceMode, 'rebalanceMode', false, REABALANCE_MODE);
        this._properties.set(PROP_REBALANCE_MODE, rebalanceMode);
        return this;
    }

    getRebalanceMode() {
        return this._properties.get(PROP_REBALANCE_MODE);
    }

    setRebalanceOrder(rebalanceOrder) {
        this._properties.set(PROP_REBALANCE_ORDER, rebalanceOrder);
        return this;
    }

    getRebalanceOrder() {
        return this._properties.get(PROP_REBALANCE_ORDER);
    }

    setRebalanceThrottle(rebalanceThrottle) {
        this._properties.set(PROP_REBALANCE_THROTTLE, rebalanceThrottle);
        return this;
    }

    getRebalanceThrottle() {
        return this._properties.get(PROP_REBALANCE_THROTTLE);
    }

    setRebalanceTimeout(rebalanceTimeout) {
        this._properties.set(PROP_REBALANCE_TIMEOUT, rebalanceTimeout);
        return this;
    }

    getRebalanceTimeout() {
        return this._properties.get(PROP_REBALANCE_TIMEOUT);
    }

    setSqlEscapeAll(sqlEscapeAll) {
        this._properties.set(PROP_SQL_ESCAPE_ALL, sqlEscapeAll);
        return this;
    }

    getSqlEscapeAll() {
        return this._properties.get(PROP_SQL_ESCAPE_ALL);
    }

    setSqlIndexInlineMaxSize(sqlIndexInlineMaxSize) {
        this._properties.set(PROP_SQL_INDEX_INLINE_MAX_SIZE, sqlIndexInlineMaxSize);
        return this;
    }

    getSqlIndexInlineMaxSize() {
        return this._properties.get(PROP_SQL_INDEX_INLINE_MAX_SIZE);
    }

    setSqlSchema(sqlSchema) {
        this._properties.set(PROP_SQL_SCHEMA, sqlSchema);
        return this;
    }

    getSqlSchema() {
        return this._properties.get(PROP_SQL_SCHEMA);
    }

    setWriteSynchronizationMode(writeSynchronizationMode) {
        ArgumentChecker.hasValueFrom(writeSynchronizationMode, 'writeSynchronizationMode', false, WRITE_SYNCHRONIZATION_MODE);
        this._properties.set(PROP_WRITE_SYNCHRONIZATION_MODE, writeSynchronizationMode);
        return this;
    }

    getWriteSynchronizationMode() {
        return this._properties.get(PROP_WRITE_SYNCHRONIZATION_MODE);
    }

    setKeyConfigurations(...keyConfigurations) {
        ArgumentChecker.hasType(keyConfigurations, 'keyConfigurations', true, CacheKeyConfiguration);
        this._properties.set(PROP_CACHE_KEY_CONFIGURATION, keyConfigurations);
        return this;
    }

    getKeyConfigurations() {
        return this._properties.get(PROP_CACHE_KEY_CONFIGURATION);
    }

    setQueryEntities(...queryEntities) {
        ArgumentChecker.hasType(queryEntities, 'queryEntities', true, QueryEntity);
        this._properties.set(PROP_QUERY_ENTITY, queryEntities);
        return this;
    }

    getQueryEntities() {
        return this._properties.get(PROP_QUERY_ENTITY);
    }

    /** Private methods */

    /**
     * @ignore
     */
    _write(buffer, name) {
        this._properties.set(PROP_NAME, name);

        const startPos = buffer.position;
        buffer.position = buffer.position +
            BinaryUtils.getSize(BinaryUtils.TYPE_CODE.INTEGER) +
            BinaryUtils.getSize(BinaryUtils.TYPE_CODE.SHORT);

        for (let [propertyCode, property] of this._properties) {
            this._writeProperty(buffer, propertyCode, property);
        }

        const length = buffer.position - startPos;
        buffer.position = startPos;

        buffer.writeInteger(length);
        buffer.writeShort(this._properties.size);
    }

    /**
     * @ignore
     */
    _writeProperty(buffer, propertyCode, property) {
        buffer.writeShort(propertyCode);
        const propertyType = PROP_TYPES[propertyCode];
        switch (BinaryUtils.getTypeCode(propertyType)) {
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                BinaryWriter.writeObject(buffer, property, propertyType, false);
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                BinaryWriter.writeObject(buffer, property, propertyType);
                return;
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                const length = property ? property.length : 0;
                buffer.writeInteger(length);
                for (let prop of property) {
                    prop._write(buffer);
                }
                return;
            default:
                throw Errors.IgniteClientError.internalError();
        }
    }

    /**
     * @ignore
     */
    _read(buffer) {
        // length
        buffer.readInteger();
        this._readProperty(buffer, PROP_ATOMICITY_MODE);
        this._readProperty(buffer, PROP_BACKUPS);
        this._readProperty(buffer, PROP_CACHE_MODE);
        this._readProperty(buffer, PROP_COPY_ON_READ);
        this._readProperty(buffer, PROP_DATA_REGION_NAME);
        this._readProperty(buffer, PROP_EAGER_TTL);
        this._readProperty(buffer, PROP_STATISTICS_ENABLED);
        this._readProperty(buffer, PROP_GROUP_NAME);
        this._readProperty(buffer, PROP_DEFAULT_LOCK_TIMEOUT);
        this._readProperty(buffer, PROP_MAX_CONCURRENT_ASYNC_OPS);
        this._readProperty(buffer, PROP_MAX_QUERY_ITERATORS);
        this._readProperty(buffer, PROP_NAME);
        this._readProperty(buffer, PROP_IS_ONHEAP_CACHE_ENABLED);
        this._readProperty(buffer, PROP_PARTITION_LOSS_POLICY);
        this._readProperty(buffer, PROP_QUERY_DETAIL_METRICS_SIZE);
        this._readProperty(buffer, PROP_QUERY_PARALLELISM);
        this._readProperty(buffer, PROP_READ_FROM_BACKUP);
        this._readProperty(buffer, PROP_REBALANCE_BATCH_SIZE);
        this._readProperty(buffer, PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
        this._readProperty(buffer, PROP_REBALANCE_DELAY);
        this._readProperty(buffer, PROP_REBALANCE_MODE);
        this._readProperty(buffer, PROP_REBALANCE_ORDER);
        this._readProperty(buffer, PROP_REBALANCE_THROTTLE);
        this._readProperty(buffer, PROP_REBALANCE_TIMEOUT);
        this._readProperty(buffer, PROP_SQL_ESCAPE_ALL);
        this._readProperty(buffer, PROP_SQL_INDEX_INLINE_MAX_SIZE);
        this._readProperty(buffer, PROP_SQL_SCHEMA);
        this._readProperty(buffer, PROP_WRITE_SYNCHRONIZATION_MODE);
        this._readProperty(buffer, PROP_CACHE_KEY_CONFIGURATION);
        this._readProperty(buffer, PROP_QUERY_ENTITY);
    }

    /**
     * @ignore
     */
    _readProperty(buffer, propertyCode) {
        const propertyType = PROP_TYPES[propertyCode];
        switch (BinaryUtils.getTypeCode(propertyType)) {
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                this._properties.set(propertyCode, BinaryReader._readTypedObject(buffer, propertyType));
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                this._properties.set(propertyCode, BinaryReader.readObject(buffer, propertyType));
                return;
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                const length = buffer.readInteger();
                if (length > 0) {
                    const properties = new Array(length);
                    for (let i = 0; i < length; i++) {
                        const property = new propertyType._elementType._objectConstructor();
                        property._read(buffer);
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
