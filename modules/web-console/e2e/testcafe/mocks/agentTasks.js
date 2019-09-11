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

export const taskResult = (result) => ({
    data: {result},
    error: null,
    sessionToken: null,
    status: 0
});

export const cacheNamesCollectorTask = (caches) => (ws) => {
    ws.on('node:visor', (e) => {
        if (e.params.taskId === 'cacheNamesCollectorTask')
            return taskResult(caches);
    });
};

export const simeplFakeSQLQuery = (nid, response) => (ws) => {
    ws.on('node:visor', (e) => {
        switch (e.params.taskId) {
            case 'cacheNodesTaskX2':
                return taskResult([nid]);

            case 'querySqlX2': {
                if (e.params.nids === nid) {
                    return taskResult({
                        error: null,
                        result: {
                            columns: null,
                            duration: 0,
                            hasMore: false,
                            queryId: 'query-1',
                            responseNodeId: nid,
                            rows: null
                        }
                    });
                }

                break;
            }

            case 'queryFetchFirstPage': {
                if (e.params.nids === nid)
                    return taskResult(response);

                break;
            }
        }
    });
};

const CLUSTER_1 = {
    id: '70831a7c-2b5e-4c11-8c08-5888911d5962',
    name: 'Cluster 1',
    nids: ['143048f1-b5b8-47d6-9239-fed76222efe3'],
    addresses: {
        '143048f1-b5b8-47d6-9239-fed76222efe3': '10.0.75.1'
    },
    clients: {
        '143048f1-b5b8-47d6-9239-fed76222efe3': false
    },
    clusterVersion: '8.8.0-SNAPSHOT',
    active: true,
    secured: false,
    supportedFeatures: '+/l9'
};

const CLUSTER_2 = {
    id: '70831a7c-2b5e-4c11-8c08-5888911d5963',
    name: 'Cluster 2',
    nids: ['143048f1-b5b8-47d6-9239-fed76222efe4'],
    addresses: {
        '143048f1-b5b8-47d6-9239-fed76222efe3': '10.0.75.1'
    },
    clients: {
        '143048f1-b5b8-47d6-9239-fed76222efe3': false
    },
    clusterVersion: '8.8.0-SNAPSHOT',
    active: true,
    secured: false,
    supportedFeatures: '+/l9'
};

export const FAKE_CLUSTERS = {
    hasAgent: true,
    hasDemo: true,
    clusters: [CLUSTER_1, CLUSTER_2]
};

export const AGENT_DISCONNECTED_GRID = {
    hasAgent: false,
    hasDemo: false,
    clusters: []
};

export const INACTIVE_CLUSTER = {
    hasAgent: true,
    hasDemo: true,
    clusters: [Object.assign({ ...CLUSTER_1 }, { active: false })]
};

export const SIMPLE_QUERY_RESPONSE = {
    error: null,
    result: {
        rows: [
            [1, 'Ed'],
            [2, 'Ann'],
            [3, 'Emma']
        ],
        hasMore: false,
        duration: 0,
        columns: [{
            schemaName: 'PUBLIC',
            typeName: 'PERSON',
            fieldName: 'ID',
            fieldTypeName: 'java.lang.Integer'
        }, {
            schemaName: 'PUBLIC',
            typeName: 'PERSON',
            fieldName: 'NAME',
            fieldTypeName: 'java.lang.String'
        }],
        queryId: 'VISOR_SQL_QUERY-42b1b723-874e-48eb-a760-b6357fc71c7f',
        responseNodeId: '0daf9042-21e6-4dd3-8f8e-a3187246abe4'
    }
};

export const SIMPLE_FAILED_QUERY_RESPONSE = {
    error: {
        message: 'Outer error message',
        stackTrace: [
            'Outer error trace 1',
            'Outer error trace 2'
        ],
        cause: {
            message: 'Inner error message',
            stackTrace: [
                'Inner error trace 1',
                'Inner error trace 2'
            ],
            cause: {
                message: 'Cause without stacktrace'
            }
        }
    },
    result: null
};

export const FAKE_CACHES = {
    caches: {
        Cache1: 'a',
        Cache2: 'b'
    },
    groups: []
};

export const agentStat = (clusters) => (ws) => {
    ws.emit('agent:status', clusters);
};
