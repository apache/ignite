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

import get from 'lodash/get';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/fromPromise';
import ObjectID from 'bson-objectid/objectid';
import {uniqueName} from 'app/utils/uniqueName';
import omit from 'lodash/fp/omit';

const uniqueNameValidator = (defaultName = '') => (a, items = []) => {
    return a && !items.some((b) => b._id !== a._id && (a.name || defaultName) === (b.name || defaultName));
};

export default class Clusters {
    static $inject = ['$http'];

    /** @type {ig.menu<ig.config.cluster.DiscoveryKinds>}>} */
    discoveries = [
        {value: 'Vm', label: 'Static IPs'},
        {value: 'Multicast', label: 'Multicast'},
        {value: 'S3', label: 'AWS S3'},
        {value: 'Cloud', label: 'Apache jclouds'},
        {value: 'GoogleStorage', label: 'Google cloud storage'},
        {value: 'Jdbc', label: 'JDBC'},
        {value: 'SharedFs', label: 'Shared filesystem'},
        {value: 'ZooKeeper', label: 'Apache ZooKeeper'},
        {value: 'Kubernetes', label: 'Kubernetes'}
    ];

    minMemoryPolicySize = 10485760; // In bytes
    ackSendThreshold = {
        min: 1,
        default: 16
    };
    messageQueueLimit = {
        min: 0,
        default: 1024
    };
    unacknowledgedMessagesBufferSize = {
        min: (
            currentValue = this.unacknowledgedMessagesBufferSize.default,
            messageQueueLimit = this.messageQueueLimit.default,
            ackSendThreshold = this.ackSendThreshold.default
        ) => {
            if (currentValue === this.unacknowledgedMessagesBufferSize.default)
                return currentValue;

            const {validRatio} = this.unacknowledgedMessagesBufferSize;
            return Math.max(messageQueueLimit * validRatio, ackSendThreshold * validRatio);
        },
        validRatio: 5,
        default: 0
    };
    sharedMemoryPort = {
        default: 48100,
        min: -1,
        max: 65535,
        invalidValues: [0]
    };

    /**
     * Cluster-related configuration stuff
     * @param {ng.IHttpService} $http
     */
    constructor($http) {
        this.$http = $http;
    }

    getConfiguration(clusterID) {
        return this.$http.get(`/api/v1/configuration/${clusterID}`);
    }

    getAllConfigurations() {
        return this.$http.get('/api/v1/configuration/list');
    }

    getCluster(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}`);
    }

    getClusterCaches(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/caches`);
    }

    /**
     * @param {string} clusterID
     * @returns {ng.IPromise<ng.IHttpResponse<{data: Array<ig.config.model.ShortDomainModel>}>>}
     */
    getClusterModels(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/models`);
    }

    getClusterIGFSs(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/igfss`);
    }

    /**
     * @returns {ng.IPromise<ng.IHttpResponse<{data: Array<ig.config.cluster.ShortCluster>}>>}
     */
    getClustersOverview() {
        return this.$http.get('/api/v1/configuration/clusters/');
    }

    getClustersOverview$() {
        return Observable.fromPromise(this.getClustersOverview());
    }

    saveCluster(cluster) {
        return this.$http.post('/api/v1/configuration/clusters/save', cluster);
    }

    saveCluster$(cluster) {
        return Observable.fromPromise(this.saveCluster(cluster));
    }

    removeCluster(cluster) {
        return this.$http.post('/api/v1/configuration/clusters/remove', {_id: cluster});
    }

    removeCluster$(cluster) {
        return Observable.fromPromise(this.removeCluster(cluster));
    }

    saveBasic(changedItems) {
        return this.$http.put('/api/v1/configuration/clusters/basic', changedItems);
    }

    saveAdvanced(changedItems) {
        return this.$http.put('/api/v1/configuration/clusters/', changedItems);
    }

    getBlankCluster() {
        return {
            _id: ObjectID.generate(),
            activeOnStart: true,
            cacheSanityCheckEnabled: true,
            atomicConfiguration: {},
            cacheKeyConfiguration: [],
            deploymentSpi: {
                URI: {
                    uriList: [],
                    scanners: []
                }
            },
            marshaller: {},
            peerClassLoadingLocalClassPathExclude: [],
            sslContextFactory: {
                trustManagers: []
            },
            swapSpaceSpi: {},
            transactionConfiguration: {},
            dataStorageConfiguration: {
                pageSize: null,
                concurrencyLevel: null,
                defaultDataRegionConfiguration: {
                    name: 'default'
                },
                dataRegionConfigurations: []
            },
            memoryConfiguration: {
                pageSize: null,
                memoryPolicies: [{
                    name: 'default',
                    maxSize: null
                }]
            },
            hadoopConfiguration: {
                nativeLibraryNames: []
            },
            serviceConfigurations: [],
            executorConfiguration: [],
            sqlConnectorConfiguration: {
                tcpNoDelay: true
            },
            clientConnectorConfiguration: {
                tcpNoDelay: true,
                jdbcEnabled: true,
                odbcEnabled: true,
                thinClientEnabled: true,
                useIgniteSslContextFactory: true
            },
            space: void 0,
            discovery: {
                kind: 'Multicast',
                Vm: {addresses: ['127.0.0.1:47500..47510']},
                Multicast: {addresses: ['127.0.0.1:47500..47510']},
                Jdbc: {initSchema: true},
                Cloud: {regions: [], zones: []}
            },
            binaryConfiguration: {typeConfigurations: [], compactFooter: true},
            communication: {tcpNoDelay: true},
            connector: {noDelay: true},
            collision: {kind: 'Noop', JobStealing: {stealingEnabled: true}, PriorityQueue: {starvationPreventionEnabled: true}},
            failoverSpi: [],
            logger: {Log4j: { mode: 'Default'}},
            caches: [],
            igfss: [],
            models: [],
            checkpointSpi: [],
            loadBalancingSpi: []
        };
    }

    /** @type {ig.menu<ig.config.cluster.FailoverSPIs>} */
    failoverSpis = [
        {value: 'JobStealing', label: 'Job stealing'},
        {value: 'Never', label: 'Never'},
        {value: 'Always', label: 'Always'},
        {value: 'Custom', label: 'Custom'}
    ];

    toShortCluster(cluster) {
        return {
            _id: cluster._id,
            name: cluster.name,
            discovery: cluster.discovery.kind,
            cachesCount: (cluster.caches || []).length,
            modelsCount: (cluster.models || []).length,
            igfsCount: (cluster.igfss || []).length
        };
    }

    requiresProprietaryDrivers(cluster) {
        return get(cluster, 'discovery.kind') === 'Jdbc' && ['Oracle', 'DB2', 'SQLServer'].includes(get(cluster, 'discovery.Jdbc.dialect'));
    }

    JDBCDriverURL(cluster) {
        return ({
            Oracle: 'http://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html',
            DB2: 'http://www-01.ibm.com/support/docview.wss?uid=swg21363866',
            SQLServer: 'https://www.microsoft.com/en-us/download/details.aspx?id=11774'
        })[get(cluster, 'discovery.Jdbc.dialect')];
    }

    dataRegion = {
        name: {
            default: 'default',
            invalidValues: ['sysMemPlc']
        },
        initialSize: {
            default: 268435456,
            min: 10485760
        },
        maxSize: {
            default: '0.2 * totalMemoryAvailable',
            min: (dataRegion) => {
                if (!dataRegion)
                    return;

                return dataRegion.initialSize || this.dataRegion.initialSize.default;
            }
        },
        evictionThreshold: {
            step: 0.001,
            max: 0.999,
            min: 0.5,
            default: 0.9
        },
        emptyPagesPoolSize: {
            default: 100,
            min: 11,
            max: (cluster, dataRegion) => {
                if (!cluster || !dataRegion || !dataRegion.maxSize)
                    return;

                const perThreadLimit = 10; // Took from Ignite
                const maxSize = dataRegion.maxSize;
                const pageSize = cluster.dataStorageConfiguration.pageSize || this.dataStorageConfiguration.pageSize.default;
                const maxPoolSize = Math.floor(maxSize / pageSize / perThreadLimit);

                return maxPoolSize;
            }
        },
        metricsSubIntervalCount: {
            default: 5,
            min: 1,
            step: 1
        },
        metricsRateTimeInterval: {
            min: 1000,
            default: 60000,
            step: 1000
        }
    };

    makeBlankDataRegionConfiguration() {
        return {_id: ObjectID.generate()};
    }

    addDataRegionConfiguration(cluster) {
        const dataRegionConfigurations = get(cluster, 'dataStorageConfiguration.dataRegionConfigurations');

        if (!dataRegionConfigurations)
            return;

        return dataRegionConfigurations.push(Object.assign(this.makeBlankDataRegionConfiguration(), {
            name: uniqueName('New data region', dataRegionConfigurations.concat(cluster.dataStorageConfiguration.defaultDataRegionConfiguration))
        }));
    }

    memoryPolicy = {
        name: {
            default: 'default',
            invalidValues: ['sysMemPlc']
        },
        initialSize: {
            default: 268435456,
            min: 10485760
        },
        maxSize: {
            default: '0.8 * totalMemoryAvailable',
            min: (memoryPolicy) => {
                return memoryPolicy.initialSize || this.memoryPolicy.initialSize.default;
            }
        },
        customValidators: {
            defaultMemoryPolicyExists: (name, items = []) => {
                const def = this.memoryPolicy.name.default;
                const normalizedName = (name || def);

                if (normalizedName === def)
                    return true;

                return items.some((policy) => (policy.name || def) === normalizedName);
            },
            uniqueMemoryPolicyName: (a, items = []) => {
                const def = this.memoryPolicy.name.default;
                return !items.some((b) => b._id !== a._id && (a.name || def) === (b.name || def));
            }
        },
        emptyPagesPoolSize: {
            default: 100,
            min: 11,
            max: (cluster, memoryPolicy) => {
                if (!memoryPolicy || !memoryPolicy.maxSize)
                    return;

                const perThreadLimit = 10; // Took from Ignite
                const maxSize = memoryPolicy.maxSize;
                const pageSize = cluster.memoryConfiguration.pageSize || this.memoryConfiguration.pageSize.default;
                const maxPoolSize = Math.floor(maxSize / pageSize / perThreadLimit);
                return maxPoolSize;
            }
        }
    };

    getDefaultClusterMemoryPolicy(cluster) {
        const def = this.memoryPolicy.name.default;
        const normalizedName = get(cluster, 'memoryConfiguration.defaultMemoryPolicyName') || def;
        return get(cluster, 'memoryConfiguration.memoryPolicies', []).find((p) => {
            return (p.name || def) === normalizedName;
        });
    }

    makeBlankCheckpointSPI() {
        return {
            FS: {
                directoryPaths: []
            },
            S3: {
                awsCredentials: {
                    kind: 'Basic'
                },
                clientConfiguration: {
                    retryPolicy: {
                        kind: 'Default'
                    },
                    useReaper: true
                }
            }
        };
    }

    addCheckpointSPI(cluster) {
        const item = this.makeBlankCheckpointSPI();
        cluster.checkpointSpi.push(item);
        return item;
    }

    makeBlankLoadBalancingSpi() {
        return {
            Adaptive: {
                loadProbe: {
                    Job: {useAverage: true},
                    CPU: {
                        useAverage: true,
                        useProcessors: true
                    },
                    ProcessingTime: {useAverage: true}
                }
            }
        };
    }

    addLoadBalancingSpi(cluster) {
        return cluster.loadBalancingSpi.push(this.makeBlankLoadBalancingSpi());
    }

    /** @type {ig.menu<ig.config.cluster.LoadBalancingKinds>} */
    loadBalancingKinds = [
        {value: 'RoundRobin', label: 'Round-robin'},
        {value: 'Adaptive', label: 'Adaptive'},
        {value: 'WeightedRandom', label: 'Random'},
        {value: 'Custom', label: 'Custom'}
    ];

    makeBlankMemoryPolicy() {
        return {_id: ObjectID.generate()};
    }

    addMemoryPolicy(cluster) {
        const memoryPolicies = get(cluster, 'memoryConfiguration.memoryPolicies');

        if (!memoryPolicies)
            return;

        return memoryPolicies.push(Object.assign(this.makeBlankMemoryPolicy(), {
            // Blank name for default policy if there are not other policies
            name: memoryPolicies.length ? uniqueName('New memory policy', memoryPolicies) : ''
        }));
    }

    // For versions 2.1-2.2, use dataStorageConfiguration since 2.3
    memoryConfiguration = {
        pageSize: {
            default: 1024 * 2,
            values: [
                {value: null, label: 'Default (2kb)'},
                {value: 1024 * 1, label: '1 kb'},
                {value: 1024 * 2, label: '2 kb'},
                {value: 1024 * 4, label: '4 kb'},
                {value: 1024 * 8, label: '8 kb'},
                {value: 1024 * 16, label: '16 kb'}
            ]
        },
        systemCacheInitialSize: {
            default: 41943040,
            min: 10485760
        },
        systemCacheMaxSize: {
            default: 104857600,
            min: (cluster) => {
                return get(cluster, 'memoryConfiguration.systemCacheInitialSize') || this.memoryConfiguration.systemCacheInitialSize.default;
            }
        }
    };

    // Added in 2.3
    dataStorageConfiguration = {
        pageSize: {
            default: 1024 * 4,
            values: [
                {value: null, label: 'Default (4kb)'},
                {value: 1024 * 1, label: '1 kb'},
                {value: 1024 * 2, label: '2 kb'},
                {value: 1024 * 4, label: '4 kb'},
                {value: 1024 * 8, label: '8 kb'},
                {value: 1024 * 16, label: '16 kb'}
            ]
        },
        systemRegionInitialSize: {
            default: 41943040,
            min: 10485760
        },
        systemRegionMaxSize: {
            default: 104857600,
            min: (cluster) => {
                return get(cluster, 'dataStorageConfiguration.systemRegionInitialSize') || this.dataStorageConfiguration.systemRegionInitialSize.default;
            }
        }
    };

    swapSpaceSpi = {
        readStripesNumber: {
            default: 'availableProcessors',
            customValidators: {
                powerOfTwo: (value) => {
                    return !value || ((value & -value) === value);
                }
            }
        }
    };

    makeBlankServiceConfiguration() {
        return {_id: ObjectID.generate()};
    }

    addServiceConfiguration(cluster) {
        if (!cluster.serviceConfigurations) cluster.serviceConfigurations = [];
        cluster.serviceConfigurations.push(Object.assign(this.makeBlankServiceConfiguration(), {
            name: uniqueName('New service configuration', cluster.serviceConfigurations)
        }));
    }

    serviceConfigurations = {
        serviceConfiguration: {
            name: {
                customValidators: {
                    uniqueName: uniqueNameValidator('')
                }
            }
        }
    };

    systemThreadPoolSize = {
        default: 'max(8, availableProcessors) * 2',
        min: 2
    };

    rebalanceThreadPoolSize = {
        default: 1,
        min: 1,
        max: (cluster) => {
            return cluster.systemThreadPoolSize ? cluster.systemThreadPoolSize - 1 : void 0;
        }
    };

    addExecutorConfiguration(cluster) {
        if (!cluster.executorConfiguration) cluster.executorConfiguration = [];
        const item = {_id: ObjectID.generate(), name: ''};
        cluster.executorConfiguration.push(item);
        return item;
    }

    executorConfigurations = {
        allNamesExist: (executorConfigurations = []) => {
            return executorConfigurations.every((ec) => ec && ec.name);
        },
        allNamesUnique: (executorConfigurations = []) => {
            const uniqueNames = new Set(executorConfigurations.map((ec) => ec.name));
            return uniqueNames.size === executorConfigurations.length;
        }
    };

    executorConfiguration = {
        name: {
            customValidators: {
                uniqueName: uniqueNameValidator()
            }
        }
    };

    marshaller = {
        kind: {
            default: 'BinaryMarshaller'
        }
    };

    odbc = {
        odbcEnabled: {
            correctMarshaller: (cluster, odbcEnabled) => {
                const marshallerKind = get(cluster, 'marshaller.kind') || this.marshaller.kind.default;
                return !odbcEnabled || marshallerKind === this.marshaller.kind.default;
            },
            correctMarshallerWatch: (root) => `${root}.marshaller.kind`
        }
    };

    swapSpaceSpis = [
        {value: 'FileSwapSpaceSpi', label: 'File-based swap'},
        {value: null, label: 'Not set'}
    ];

    affinityFunctions = [
        {value: 'Rendezvous', label: 'Rendezvous'},
        {value: 'Custom', label: 'Custom'},
        {value: null, label: 'Default'}
    ];

    normalize = omit(['__v', 'space']);

    addPeerClassLoadingLocalClassPathExclude(cluster) {
        if (!cluster.peerClassLoadingLocalClassPathExclude) cluster.peerClassLoadingLocalClassPathExclude = [];
        return cluster.peerClassLoadingLocalClassPathExclude.push('');
    }

    addBinaryTypeConfiguration(cluster) {
        if (!cluster.binaryConfiguration.typeConfigurations) cluster.binaryConfiguration.typeConfigurations = [];
        const item = {_id: ObjectID.generate()};
        cluster.binaryConfiguration.typeConfigurations.push(item);
        return item;
    }
}
