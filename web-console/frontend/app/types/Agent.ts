

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
