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

export type ClusterStats = {
	id: string,
	name: string,
	nids: string[],
	addresses: {[id: string]: string},
	clients: {[id: string]: boolean},
	clusterVersion: string,
	active: boolean,
	secured: false,
	gridgain: boolean,
	ultimate: boolean,
	supportedFeatures: string
}

export type AgentsStatResponse = {
	count: number,
	hasDemo: boolean,
	clusters: ClusterStats[]
}

export type CacheNamesCollectorTaskResponse = {
	caches: {[cacheName: string]: string},
	cachesComment: {[cacheName: string]: string},
	sqlSchemas: {[cacheName: string]: string}
}

export type CacheNodesTaskResponse = string[]

export type VisorQueryResult =
{
	error: null,
	result: {
		columns: null | {
			fieldName: string,
			fieldTypeName: string,
			schemaName: string,
			typeName: string
		}[],
		duration: number,
		hasMore: boolean,
		queryId: string,
		responseNodeId: string,
		rows: null | any[]
	}
} | {
	error: {
		cause: {
			className: string,
			message: string
		},
		className: string,
		message: string
	},
	result: null
}

export type QuerySqlX2Response = VisorQueryResult

export type QueryFetchFirstPageResult = VisorQueryResult

export enum IgniteFeatures {
	INDEXING = 15,
	WC_DR_EVENTS = 20,
	WC_ROLLING_UPGRADE_STATUS = 21,
	WC_SNAPSHOT_CHAIN_MODE = 22,
	WC_BASELINE_AUTO_ADJUSTMENT = 23,
	WC_SCHEDULING_NOT_AVAILABLE = 24
}

export type WebSocketResponse = {
	requestId: any,
	eventType: string,
	payload: any
}
