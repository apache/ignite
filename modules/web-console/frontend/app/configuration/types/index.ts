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

// Cache
export type CacheModes = 'PARTITIONED' | 'REPLICATED' | 'LOCAL';
export type AtomicityModes = 'ATOMIC' | 'TRANSACTIONAL' | 'TRANSACTIONAL_SNAPSHOT';

export interface ShortCache {
    _id: string,
    cacheMode: CacheModes,
    atomicityMode: AtomicityModes,
    backups: number,
    name: string
}

// IGFS
type DefaultModes = 'PRIMARY' | 'PROXY' | 'DUAL_SYNC' | 'DUAL_ASYNC';

export interface ShortIGFS {
    _id: string,
    name: string,
    defaultMode: DefaultModes,
    affinnityGroupSize: number
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
    _id: string,
    name?: string,
    direction?: boolean
}
export interface Index {
    _id: string,
    name: string,
    indexType: IndexTypes,
    fields: Array<IndexField>
}

export interface DomainModel {
    _id: string,
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
    _id: string,
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

export type FailoverSPIs = 'JobStealing' | 'Never' | 'Always' | 'Custom';

export interface Cluster {
    _id: string,
    name: string,
    // TODO: cover with types
    [key: string]: any
}

export interface ShortCluster {
    _id: string,
    name: string,
    discovery: DiscoveryKinds,
    caches: number,
    models: number,
    igfs: number
}

export type ClusterLike = Cluster | ShortCluster;
