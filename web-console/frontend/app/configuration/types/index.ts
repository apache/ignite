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

// Cache
export type CacheModes = 'PARTITIONED' | 'REPLICATED' | 'LOCAL';
export type AtomicityModes = 'ATOMIC' | 'TRANSACTIONAL' | 'TRANSACTIONAL_SNAPSHOT';

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
    className: string
}
export interface Alias {
    field: string,
    alias: string
}
export type IndexTypes = 'SORTED' | 'FULLTEXT' | 'GEOSPATIAL';
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
    hasIndex: boolean
}

// Cluster
export type DiscoveryKinds = 'Vm'
    | 'Multicast'
    | 'S3'
    | 'Cloud'
    | 'GoogleStorage'
    | 'Jdbc'
    | 'SharedFs'
    | 'ZooKeeper'
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

export type ClusterLike = Cluster | ShortCluster;
