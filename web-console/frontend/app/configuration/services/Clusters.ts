

import get from 'lodash/get';
import find from 'lodash/find';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {uniqueName} from 'app/utils/uniqueName';
import {DiscoveryKinds, ShortDomainModel, ShortCluster, FailoverSPIs, LoadBalancingKinds, WalPageCompression} from '../types';
import {Menu} from 'app/types';

const uniqueNameValidator = (defaultName = '') => (a, items = []) => {
    return a && !items.some((b) => b.id !== a.id && (a.name || defaultName) === (b.name || defaultName));
};

export default class Clusters {
    static $inject = ['$http'];    

    discoveries: Menu<DiscoveryKinds> = [
        {value: 'Isolated', label: 'Isolated Nodes'},
        {value: 'Vm', label: 'Static IPs'},
        {value: 'Multicast', label: 'Multicast'},
        {value: 'WebConsoleServer', label: 'Web Console Server'},
        {value: 'ZooKeeper', label: 'Apache ZooKeeper'},
        {value: 'ZooKeeperIpFinder', label: 'ZooKeeper IpFinder'},
        {value: 'Kubernetes', label: 'Kubernetes'}
    ];

    walPageCompression: Menu<WalPageCompression> = [
        {value: 'DISABLED', label: 'DISABLED'},
        {value: 'SKIP_GARBAGE', label: 'SKIP_GARBAGE'},
        {value: 'ZSTD', label: 'ZSTD'},
        {value: 'LZ4', label: 'LZ4'},
        {value: 'SNAPPY', label: 'SNAPPY'},
        {value: null, label: 'Default'}
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
    
    /**
     * Cluster-related configuration stuff
     */
    constructor(private $http: ng.IHttpService) {}

    getConfiguration(clusterID: string) {
        return this.$http.get(`/api/v1/configuration/${clusterID}`)
            .then((data) => {
                return data;
            });
    }

    getCluster(clusterID: string) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}`);
    }

    getClusterCaches(clusterID: string) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/caches`);
    }

    getClusterModels(clusterID: string) {
        return this.$http.get<{data: ShortDomainModel[]}>(`/api/v1/configuration/clusters/${clusterID}/models`);
    }

    getClustersOverview() {
        return this.$http.get<{data: ShortCluster[]}>('/api/v1/configuration/clusters/');
    }

    removeCluster(clusterIDs) {
        return this.$http.post('/api/v1/configuration/clusters/remove', {clusterIDs});
    }

    saveBasic(changedItems) {
        return this.$http.put('/api/v1/configuration/clusters/basic', changedItems);
    }

    saveAdvanced(changedItems) {
        return this.$http.put('/api/v1/configuration/clusters/', changedItems);
    }

    getBlankCluster() {
        return {
            id: uuidv4(),
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
                ZooKeeper: {zkConectionString: '127.0.0.1:2181'},
                Multicast: {addresses: ['127.0.0.1:47500..47510']}                
            },
            binaryConfiguration: {typeConfigurations: [], compactFooter: true},
            communication: {tcpNoDelay: true},
            connector: {noDelay: true},
            collision: {kind: 'Noop', JobStealing: {stealingEnabled: true}, PriorityQueue: {starvationPreventionEnabled: true}},
            failoverSpi: [],
            caches: [],
            models: [],
            checkpointSpi: [],
            loadBalancingSpi: [],
            autoActivationEnabled: true
        };
    }

    failoverSpis: Menu<FailoverSPIs> = [
        {value: 'JobStealing', label: 'Job stealing'},
        {value: 'Never', label: 'Never'},
        {value: 'Always', label: 'Always'},
        {value: 'Custom', label: 'Custom'}
    ];

    toShortCluster(cluster) {
        return {
            id: cluster.id,
            name: cluster.name,
            status: cluster.status,
            discovery: cluster.discovery.kind,
            cachesCount: (cluster.caches || []).length,
            modelsCount: (cluster.models || []).length
        };
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
        return {id: uuidv4()};
    }

    addDataRegionConfiguration(cluster) {
        const dataRegionConfigurations = get(cluster, 'dataStorageConfiguration.dataRegionConfigurations');

        if (!dataRegionConfigurations)
            return;

        return dataRegionConfigurations.push(Object.assign(this.makeBlankDataRegionConfiguration(), {
            name: uniqueName('New data region', dataRegionConfigurations.concat(cluster.dataStorageConfiguration.defaultDataRegionConfiguration))
        }));
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

    loadBalancingKinds: Menu<LoadBalancingKinds> = [
        {value: 'RoundRobin', label: 'Round-robin'},
        {value: 'Adaptive', label: 'Adaptive'},
        {value: 'WeightedRandom', label: 'Random'},
        {value: 'Custom', label: 'Custom'}
    ];
    

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

    persistenceEnabled(dataStorage) {
        return !!(get(dataStorage, 'defaultDataRegionConfiguration.persistenceEnabled')
            || find(get(dataStorage, 'dataRegionConfigurations'), (storeCfg) => storeCfg.persistenceEnabled));
    }

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
        return {id: uuidv4()};
    }

    addServiceConfiguration(cluster) {
        if (!cluster.serviceConfigurations)
            cluster.serviceConfigurations = [];

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
        default: 'max(16, availableProcessors)',
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
        if (!cluster.executorConfiguration)
            cluster.executorConfiguration = [];

        const item = {id: uuidv4(), name: ''};

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

    swapSpaceSpis = [
        {value: 'FileSwapSpaceSpi', label: 'File-based swap'},
        {value: null, label: 'Not set'}
    ];

    affinityFunctions = [
        {value: 'Rendezvous', label: 'Rendezvous'},
        {value: 'Custom', label: 'Custom'},
        {value: null, label: 'Default'}
    ];

    normalize = omit(['__v']);

    addPeerClassLoadingLocalClassPathExclude(cluster) {
        if (!cluster.peerClassLoadingLocalClassPathExclude)
            cluster.peerClassLoadingLocalClassPathExclude = [];

        return cluster.peerClassLoadingLocalClassPathExclude.push('');
    }

    addBinaryTypeConfiguration(cluster) {
        if (!cluster.binaryConfiguration.typeConfigurations)
            cluster.binaryConfiguration.typeConfigurations = [];

        const item = {id: uuidv4()};

        cluster.binaryConfiguration.typeConfigurations.push(item);

        return item;
    }

    addLocalEventListener(cluster) {
        if (!cluster.localEventListeners)
            cluster.localEventListeners = [];

        const item = {id: uuidv4()};

        cluster.localEventListeners.push(item);

        return item;
    }
}
