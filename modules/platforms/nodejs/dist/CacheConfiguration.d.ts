/**
 * Class representing Cache Key part of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
export declare class CacheKeyConfiguration {
    private _typeName;
    private _affinityKeyFieldName;
    /**
     * Public constructor.
     *
     * @param {string} [typeName=null]
     * @param {string} [affinityKeyFieldName=null]
     *
     * @return {CacheKeyConfiguration} - new CacheKeyConfiguration instance.
     */
    constructor(typeName?: string, affinityKeyFieldName?: string);
    /**
     *
     *
     * @param {string} typeName
     *
     * @return {CacheKeyConfiguration} - the same instance of the CacheKeyConfiguration.
     */
    setTypeName(typeName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getTypeName(): string;
    /**
     *
     *
     * @param {string} affinityKeyFieldName
     *
     * @return {CacheKeyConfiguration} - the same instance of the CacheKeyConfiguration.
     */
    setAffinityKeyFieldName(affinityKeyFieldName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getAffinityKeyFieldName(): string;
    /** Private methods */
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _read(communicator: any, buffer: any): Promise<void>;
}
/**
 * Class representing one Query Entity element of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
export declare class QueryEntity {
    private _keyTypeName;
    private _valueTypeName;
    private _tableName;
    private _keyFieldName;
    private _valueFieldName;
    private _fields;
    private _aliases;
    private _indexes;
    /**
     * Public constructor.
     *
     * @return {QueryEntity} - new QueryEntity instance.
     */
    constructor();
    /**
     *
     *
     * @param {string} keyTypeName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setKeyTypeName(keyTypeName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getKeyTypeName(): any;
    /**
     *
     *
     * @param {string} valueTypeName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setValueTypeName(valueTypeName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getValueTypeName(): any;
    /**
     *
     *
     * @param {string} tableName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setTableName(tableName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getTableName(): any;
    /**
     *
     *
     * @param {string} keyFieldName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setKeyFieldName(keyFieldName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getKeyFieldName(): any;
    /**
     *
     *
     * @param {string} valueFieldName
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setValueFieldName(valueFieldName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getValueFieldName(): any;
    /**
     *
     *
     * @param {Array<QueryField>} fields
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setFields(fields: any): this;
    /**
     *
     *
     * @return {Array<QueryField>}
     */
    getFields(): any;
    /**
     *
     *
     * @param {Map<string, string>} aliases
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setAliases(aliases: any): this;
    /**
     *
     *
     * @return {Map<string, string>}
     */
    getAliases(): any;
    /**
     *
     *
     * @param {Array<QueryIndex>} indexes
     *
     * @return {QueryEntity} - the same instance of the QueryEntity.
     */
    setIndexes(indexes: any): this;
    /**
     *
     *
     * @return {Array<QueryIndex>}
     */
    getIndexes(): any;
    /** Private methods */
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _writeAliases(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _writeSubEntities(communicator: any, buffer: any, entities: any): Promise<void>;
    /**
     * @ignore
     */
    _read(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _readSubEntities(communicator: any, buffer: any, objectConstructor: any): Promise<any[]>;
    /**
     * @ignore
     */
    _readAliases(communicator: any, buffer: any): Promise<void>;
}
/**
 * Class representing one Query Field element of {@link QueryEntity} of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
export declare class QueryField {
    private _name;
    private _typeName;
    private _isKeyField;
    private _isNotNull;
    private _defaultValue;
    private _precision;
    private _scale;
    private _valueType;
    private _communicator;
    private _buffer;
    private _index;
    /**
     * Public constructor.
     *
     * @param {string} [name=null]
     * @param {string} [typeName=null]
     *
     * @return {QueryField} - new QueryField instance.
     */
    constructor(name?: any, typeName?: any);
    /**
     *
     *
     * @param {string} name
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setName(name: any): this;
    /**
     *
     *
     * @return {string}
     */
    getName(): any;
    /**
     *
     *
     * @param {string} typeName
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setTypeName(typeName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getTypeName(): any;
    /**
     *
     *
     * @param {boolean} isKeyField
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setIsKeyField(isKeyField: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getIsKeyField(): any;
    /**
     *
     *
     * @param {boolean} isNotNull
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setIsNotNull(isNotNull: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getIsNotNull(): any;
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
    setDefaultValue(defaultValue: any, valueType?: any): this;
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
    getDefaultValue(valueType?: any): Promise<any>;
    /**
     *
     *
     * @param {number} precision
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setPrecision(precision: any): this;
    /**
     *
     *
     * @return {number}
     */
    getPrecision(): any;
    /**
     *
     *
     * @param {number} scale
     *
     * @return {QueryField} - the same instance of the QueryField.
     */
    setScale(scale: any): this;
    /**
     *
     *
     * @return {number}
     */
    getScale(): any;
    /** Private methods */
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _read(communicator: any, buffer: any): Promise<void>;
}
/**
 * Class representing one Query Index element of {@link QueryEntity} of Ignite {@link CacheConfiguration}.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
export declare class QueryIndex {
    private _name;
    private _inlineSize;
    private _fields;
    private _type;
    /**
     * Public constructor.
     *
     * @param {string} [name=null]
     * @param {string} [typeName=QueryIndex.INDEX_TYPE.SORTED]
     *
     * @return {QueryIndex} - new QueryIndex instance.
     */
    constructor(name?: any, type?: number);
    static readonly INDEX_TYPE: Readonly<{
        SORTED: number;
        FULLTEXT: number;
        GEOSPATIAL: number;
    }>;
    /**
     *
     *
     * @param {string} name
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setName(name: any): this;
    /**
     *
     *
     * @return {string}
     */
    getName(): any;
    /**
     *
     *
     * @param {QueryIndex.INDEX_TYPE} type
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     *
     * @throws {IgniteClientError} if error.
     */
    setType(type: any): this;
    /**
     *
     *
     * @return {QueryIndex.INDEX_TYPE}
     */
    getType(): any;
    /**
     *
     *
     * @param {number} inlineSize
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setInlineSize(inlineSize: any): this;
    /**
     *
     *
     * @return {number}
     */
    getInlineSize(): number;
    /**
     *
     *
     * @param {Map<string, boolean>} fields
     *
     * @return {QueryIndex} - the same instance of the QueryIndex.
     */
    setFields(fields: any): this;
    /**
     *
     *
     * @return {Map<string, boolean>}
     */
    getFields(): any;
    /** Private methods */
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _read(communicator: any, buffer: any): Promise<void>;
}
/**
 * Class representing Ignite cache configuration on a server.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
export declare class CacheConfiguration {
    private _properties;
    /**
     * Public constructor.
     *
     * @return {CacheConfiguration} - new CacheConfiguration instance.
     */
    constructor();
    static readonly CACHE_ATOMICITY_MODE: Readonly<{
        TRANSACTIONAL: number;
        ATOMIC: number;
    }>;
    static readonly CACHE_MODE: Readonly<{
        LOCAL: number;
        REPLICATED: number;
        PARTITIONED: number;
    }>;
    static readonly PARTITION_LOSS_POLICY: Readonly<{
        READ_ONLY_SAFE: number;
        READ_ONLY_ALL: number;
        READ_WRITE_SAFE: number;
        READ_WRITE_ALL: number;
        IGNORE: number;
    }>;
    static readonly REABALANCE_MODE: Readonly<{
        SYNC: number;
        ASYNC: number;
        NONE: number;
    }>;
    static readonly WRITE_SYNCHRONIZATION_MODE: Readonly<{
        FULL_SYNC: number;
        FULL_ASYNC: number;
        PRIMARY_SYNC: number;
    }>;
    /**
     *
     *
     * @param {CacheConfiguration.CACHE_ATOMICITY_MODE} atomicityMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setAtomicityMode(atomicityMode: any): this;
    /**
     *
     *
     * @return {CacheConfiguration.CACHE_ATOMICITY_MODE}
     */
    getAtomicityMode(): any;
    /**
     *
     *
     * @param {number} backups
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setBackups(backups: any): this;
    /**
     *
     *
     * @return {number}
     */
    getBackups(): any;
    /**
     *
     *
     * @param {CacheConfiguration.CACHE_MODE} cacheMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setCacheMode(cacheMode: any): this;
    /**
     *
     *
     * @return {CacheConfiguration.CACHE_MODE}
     */
    getCacheMode(): any;
    /**
     *
     *
     * @param {boolean} copyOnRead
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setCopyOnRead(copyOnRead: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getCopyOnRead(): any;
    /**
     *
     *
     * @param {string} dataRegionName
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setDataRegionName(dataRegionName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getDataRegionName(): any;
    /**
     *
     *
     * @param {boolean} eagerTtl
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setEagerTtl(eagerTtl: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getEagerTtl(): any;
    /**
     *
     *
     * @param {boolean} statisticsEnabled
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setStatisticsEnabled(statisticsEnabled: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getStatisticsEnabled(): any;
    /**
     *
     *
     * @param {string} groupName
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setGroupName(groupName: any): this;
    /**
     *
     *
     * @return {string}
     */
    getGroupName(): any;
    /**
     *
     *
     * @param {number} lockTimeout
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setDefaultLockTimeout(lockTimeout: any): this;
    /**
     *
     *
     * @return {number}
     */
    getDefaultLockTimeout(): any;
    /**
     *
     *
     * @param {number} maxConcurrentAsyncOperations
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setMaxConcurrentAsyncOperations(maxConcurrentAsyncOperations: any): this;
    /**
     *
     *
     * @return {number}
     */
    getMaxConcurrentAsyncOperations(): any;
    /**
     *
     *
     * @param {number} maxQueryIterators
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setMaxQueryIterators(maxQueryIterators: any): this;
    /**
     *
     *
     * @return {number}
     */
    getMaxQueryIterators(): any;
    /**
     *
     *
     * @param {boolean} isOnheapCacheEnabled
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setIsOnheapCacheEnabled(isOnheapCacheEnabled: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getIsOnheapCacheEnabled(): any;
    /**
     *
     *
     * @param {CacheConfiguration.PARTITION_LOSS_POLICY} partitionLossPolicy
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setPartitionLossPolicy(partitionLossPolicy: any): this;
    /**
     *
     *
     * @return {CacheConfiguration.PARTITION_LOSS_POLICY}
     */
    getPartitionLossPolicy(): any;
    /**
     *
     *
     * @param {number} queryDetailMetricsSize
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setQueryDetailMetricsSize(queryDetailMetricsSize: any): this;
    /**
     *
     *
     * @return {number}
     */
    getQueryDetailMetricsSize(): any;
    /**
     *
     *
     * @param {number} queryParallelism
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setQueryParallelism(queryParallelism: any): this;
    /**
     *
     *
     * @return {number}
     */
    getQueryParallelism(): any;
    /**
     *
     *
     * @param {boolean} readFromBackup
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setReadFromBackup(readFromBackup: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getReadFromBackup(): any;
    /**
     *
     *
     * @param {number} rebalanceBatchSize
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceBatchSize(rebalanceBatchSize: any): this;
    /**
     *
     *
     * @return {number}
     */
    getRebalanceBatchSize(): any;
    /**
     *
     *
     * @param {number} rebalanceBatchesPrefetchCount
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceBatchesPrefetchCount(rebalanceBatchesPrefetchCount: any): this;
    /**
     *
     *
     * @return {number}
     */
    getRebalanceBatchesPrefetchCount(): any;
    /**
     *
     *
     * @param {number} rebalanceDelay
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceDelay(rebalanceDelay: any): this;
    /**
     *
     *
     * @return {number}
     */
    getRebalanceDelay(): any;
    /**
     *
     *
     * @param {CacheConfiguration.REABALANCE_MODE} rebalanceMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setRebalanceMode(rebalanceMode: any): this;
    /**
     *
     *
     * @return {CacheConfiguration.REABALANCE_MODE}
     */
    getRebalanceMode(): any;
    /**
     *
     *
     * @param {number} rebalanceOrder
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceOrder(rebalanceOrder: any): this;
    /**
     *
     *
     * @return {number}
     */
    getRebalanceOrder(): any;
    /**
     *
     *
     * @param {number} rebalanceThrottle
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceThrottle(rebalanceThrottle: any): this;
    /**
     *
     *
     * @return {number}
     */
    getRebalanceThrottle(): any;
    /**
     *
     *
     * @param {number} rebalanceTimeout
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setRebalanceTimeout(rebalanceTimeout: any): this;
    /**
     *
     *
     * @return {number}
     */
    getRebalanceTimeout(): any;
    /**
     *
     *
     * @param {boolean} sqlEscapeAll
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlEscapeAll(sqlEscapeAll: any): this;
    /**
     *
     *
     * @return {boolean}
     */
    getSqlEscapeAll(): any;
    /**
     *
     *
     * @param {number} sqlIndexInlineMaxSize
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlIndexInlineMaxSize(sqlIndexInlineMaxSize: any): this;
    /**
     *
     *
     * @return {number}
     */
    getSqlIndexInlineMaxSize(): any;
    /**
     *
     *
     * @param {string} sqlSchema
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     */
    setSqlSchema(sqlSchema: any): this;
    /**
     *
     *
     * @return {string}
     */
    getSqlSchema(): any;
    /**
     *
     *
     * @param {CacheConfiguration.WRITE_SYNCHRONIZATION_MODE} writeSynchronizationMode
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setWriteSynchronizationMode(writeSynchronizationMode: any): this;
    /**
     *
     *
     * @return {CacheConfiguration.WRITE_SYNCHRONIZATION_MODE}
     */
    getWriteSynchronizationMode(): any;
    /**
     *
     *
     * @param {...CacheKeyConfiguration} keyConfigurations
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setKeyConfigurations(...keyConfigurations: any[]): this;
    /**
     *
     *
     * @return {Array<CacheKeyConfiguration>}
     */
    getKeyConfigurations(): any;
    /**
     *
     *
     * @param {...QueryEntity} queryEntities
     *
     * @return {CacheConfiguration} - the same instance of the CacheConfiguration.
     *
     * @throws {IgniteClientError} if error.
     */
    setQueryEntities(...queryEntities: any[]): this;
    /**
     *
     *
     * @return {Array<QueryEntity>}
     */
    getQueryEntities(): any;
    /** Private methods */
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any, name: any): Promise<void>;
    /**
     * @ignore
     */
    _writeProperty(communicator: any, buffer: any, propertyCode: any, property: any): Promise<void>;
    /**
     * @ignore
     */
    _read(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _readProperty(communicator: any, buffer: any, propertyCode: any): Promise<void>;
}
