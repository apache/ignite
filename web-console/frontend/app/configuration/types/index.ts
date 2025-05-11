

// Cache
export type CacheModes = 'PARTITIONED' | 'REPLICATED' ;
export type AtomicityModes = 'ATOMIC' | 'TRANSACTIONAL' ;

export interface ShortCache {
    id: string,
    cacheMode: CacheModes,
    atomicityMode: AtomicityModes,
    backups: number,
    name: string
}

// Models
type QueryMetadataTypes = 'Annotations' | 'Configuration';
type DomainModelKinds = 'query' | 'store' | 'both';
export interface KeyField {
    databaseFieldName: string,
    databaseFieldType: string,
    javaFieldName: string,
    javaFieldType: string
}
export interface ValueField {
    databaseFieldName: string,
    databaseFieldType: string,
    javaFieldName: string,
    javaFieldType: string
}
export interface Field {
    name: string,
    className: string,
    comment: string
}
export interface Alias {
    field: string,
    alias: string
}
export type IndexTypes = 'SORTED' | 'FULLTEXT' | 'VECTORTEXT' | 'GEOSPATIAL';
export interface IndexField {
    id: string,
    name?: string,
    direction?: boolean
}

export enum InlineSizeType {
    'AUTO' = -1,
    'DISABLED' = 0,
    'CUSTOM' = 1
}

export interface Index {
    id: string,
    name: string,
    indexType: IndexTypes,
    fields: Array<IndexField>,
    inlineSize: number | null,
    inlineSizeType: InlineSizeType
}

export interface DomainModel {
    id: string,
    space?: string,
    clusters?: Array<string>,
    caches?: Array<string>,
    queryMetadata?: QueryMetadataTypes,
    kind?: DomainModelKinds,
    tableName?: string,
    tableComment?: string,
    keyFieldName?: string,
    valueFieldName?: string,
    databaseSchema?: string,
    databaseTable?: string,
    keyType?: string,
    valueType?: string,
    keyFields?: Array<KeyField>,
    valueFields?: Array<ValueField>,
    queryKeyFields?: Array<string>,
    fields?: Array<Field>,
    aliases?: Array<Alias>,
    indexes?: Array<Index>,
    generatePojo?: boolean
}

export interface ShortDomainModel {
    id: string,
    keyType: string,
    valueType: string,
    tableComment?: string,
    hasIndex: boolean
}

// Cluster
export type DiscoveryKinds = 'Vm'
    | 'Multicast'
    | 'Isolated'
    | 'WebConsoleServer'
    | 'SharedFs'
    | 'ZooKeeper'
    | 'ZooKeeperIpFinder'
    | 'Kubernetes';

export type LoadBalancingKinds = 'RoundRobin'
    | 'Adaptive'
    | 'WeightedRandom'
    | 'Custom';

export type WalPageCompression = 'DISABLED'
    | 'SKIP_GARBAGE'
    | 'ZSTD'
    | 'LZ4'
    | 'SNAPPY'

export type FailoverSPIs = 'JobStealing' | 'Never' | 'Always' | 'Custom';

export interface Cluster {
    id: string,
    name: string,
    // TODO: cover with types
    [key: string]: any
}

export interface ShortCluster {
    id: string,
    name: string,
    discovery: DiscoveryKinds,
    caches: number,
    models: number
}

export interface DatasourceDto {
	id: string,
    db: string,
	driverCls: string,
	jdbcUrl: string,	
	jndiName: string, // 数据库JNDI名称
	schemaName: string, // 默认的模式名称
	userName: string | null,
	password: string | null,
	jdbcProp: object
}

export type ClusterLike = Cluster | ShortCluster;
