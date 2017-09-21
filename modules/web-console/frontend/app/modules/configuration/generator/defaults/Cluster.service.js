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

const DFLT_CLUSTER = {
    localHost: '0.0.0.0',
    discovery: {
        localPort: 47500,
        localPortRange: 100,
        socketTimeout: 5000,
        ackTimeout: 5000,
        maxAckTimeout: 600000,
        networkTimeout: 5000,
        joinTimeout: 0,
        threadPriority: 10,
        heartbeatFrequency: 2000,
        maxMissedHeartbeats: 1,
        maxMissedClientHeartbeats: 5,
        topHistorySize: 1000,
        reconnectCount: 10,
        statisticsPrintFrequency: 0,
        ipFinderCleanFrequency: 60000,
        forceServerMode: false,
        clientReconnectDisabled: false,
        Multicast: {
            multicastGroup: '228.1.2.4',
            multicastPort: 47400,
            responseWaitTime: 500,
            addressRequestAttempts: 2,
            localAddress: '0.0.0.0'
        },
        Jdbc: {
            initSchema: false
        },
        SharedFs: {
            path: 'disco/tcp'
        },
        ZooKeeper: {
            basePath: '/services',
            serviceName: 'ignite',
            allowDuplicateRegistrations: false,
            ExponentialBackoff: {
                baseSleepTimeMs: 1000,
                maxRetries: 10
            },
            BoundedExponentialBackoffRetry: {
                baseSleepTimeMs: 1000,
                maxSleepTimeMs: 2147483647,
                maxRetries: 10
            },
            UntilElapsed: {
                maxElapsedTimeMs: 60000,
                sleepMsBetweenRetries: 1000
            },
            RetryNTimes: {
                n: 10,
                sleepMsBetweenRetries: 1000
            },
            OneTime: {
                sleepMsBetweenRetry: 1000
            },
            Forever: {
                retryIntervalMs: 1000
            }
        }
    },
    atomics: {
        atomicSequenceReserveSize: 1000,
        backups: 0,
        cacheMode: {
            clsName: 'org.apache.ignite.cache.CacheMode',
            value: 'PARTITIONED'
        }
    },
    binary: {
        compactFooter: true,
        typeConfigurations: {
            enum: false
        }
    },
    collision: {
        kind: null,
        JobStealing: {
            activeJobsThreshold: 95,
            waitJobsThreshold: 0,
            messageExpireTime: 1000,
            maximumStealingAttempts: 5,
            stealingEnabled: true,
            stealingAttributes: {
                keyClsName: 'java.lang.String',
                valClsName: 'java.io.Serializable',
                items: []
            }
        },
        PriorityQueue: {
            priorityAttributeKey: 'grid.task.priority',
            jobPriorityAttributeKey: 'grid.job.priority',
            defaultPriority: 0,
            starvationIncrement: 1,
            starvationPreventionEnabled: true
        }
    },
    communication: {
        localPort: 47100,
        localPortRange: 100,
        sharedMemoryPort: 48100,
        directBuffer: false,
        directSendBuffer: false,
        idleConnectionTimeout: 30000,
        connectTimeout: 5000,
        maxConnectTimeout: 600000,
        reconnectCount: 10,
        socketSendBuffer: 32768,
        socketReceiveBuffer: 32768,
        messageQueueLimit: 1024,
        tcpNoDelay: true,
        ackSendThreshold: 16,
        unacknowledgedMessagesBufferSize: 0,
        socketWriteTimeout: 2000
    },
    networkTimeout: 5000,
    networkSendRetryDelay: 1000,
    networkSendRetryCount: 3,
    discoveryStartupDelay: 60000,
    connector: {
        port: 11211,
        portRange: 100,
        idleTimeout: 7000,
        idleQueryCursorTimeout: 600000,
        idleQueryCursorCheckFrequency: 60000,
        receiveBufferSize: 32768,
        sendBufferSize: 32768,
        sendQueueLimit: 0,
        directBuffer: false,
        noDelay: true,
        sslEnabled: false,
        sslClientAuth: false
    },
    deploymentMode: {
        clsName: 'org.apache.ignite.configuration.DeploymentMode',
        value: 'SHARED'
    },
    peerClassLoadingEnabled: false,
    peerClassLoadingMissedResourcesCacheSize: 100,
    peerClassLoadingThreadPoolSize: 2,
    failoverSpi: {
        JobStealing: {
            maximumFailoverAttempts: 5
        },
        Always: {
            maximumFailoverAttempts: 5
        }
    },
    logger: {
        Log4j: {
            level: {
                clsName: 'org.apache.log4j.Level'
            }
        },
        Log4j2: {
            level: {
                clsName: 'org.apache.logging.log4j.Level'
            }
        }
    },
    marshalLocalJobs: false,
    marshallerCacheKeepAliveTime: 10000,
    metricsHistorySize: 10000,
    metricsLogFrequency: 60000,
    metricsUpdateFrequency: 2000,
    clockSyncSamples: 8,
    clockSyncFrequency: 120000,
    timeServerPortBase: 31100,
    timeServerPortRange: 100,
    transactionConfiguration: {
        defaultTxConcurrency: {
            clsName: 'org.apache.ignite.transactions.TransactionConcurrency',
            value: 'PESSIMISTIC'
        },
        defaultTxIsolation: {
            clsName: 'org.apache.ignite.transactions.TransactionIsolation',
            value: 'REPEATABLE_READ'
        },
        defaultTxTimeout: 0,
        pessimisticTxLogLinger: 10000
    },
    attributes: {
        keyClsName: 'java.lang.String',
        valClsName: 'java.lang.String',
        items: []
    },
    odbcConfiguration: {
        endpointAddress: '0.0.0.0:10800..10810',
        maxOpenCursors: 128
    },
    eventStorage: {
        Memory: {
            expireCount: 10000
        }
    },
    checkpointSpi: {
        S3: {
            bucketNameSuffix: 'default-bucket',
            clientConfiguration: {
                protocol: {
                    clsName: 'com.amazonaws.Protocol',
                    value: 'HTTPS'
                },
                maxConnections: 50,
                retryPolicy: {
                    retryCondition: {
                        clsName: 'com.amazonaws.retry.PredefinedRetryPolicies'
                    },
                    backoffStrategy: {
                        clsName: 'com.amazonaws.retry.PredefinedRetryPolicies'
                    },
                    maxErrorRetry: {
                        clsName: 'com.amazonaws.retry.PredefinedRetryPolicies'
                    }
                },
                maxErrorRetry: -1,
                socketTimeout: 50000,
                connectionTimeout: 50000,
                requestTimeout: 0,
                socketSendBufferSizeHints: 0,
                connectionTTL: -1,
                connectionMaxIdleMillis: 60000,
                responseMetadataCacheSize: 50,
                useReaper: true,
                useGzip: false,
                preemptiveBasicProxyAuth: false,
                useTcpKeepAlive: false
            }
        },
        JDBC: {
            checkpointTableName: 'CHECKPOINTS',
            keyFieldName: 'NAME',
            keyFieldType: 'VARCHAR',
            valueFieldName: 'VALUE',
            valueFieldType: 'BLOB',
            expireDateFieldName: 'EXPIRE_DATE',
            expireDateFieldType: 'DATETIME',
            numberOfRetries: 2
        }
    },
    loadBalancingSpi: {
        RoundRobin: {
            perTask: false
        },
        Adaptive: {
            loadProbe: {
                Job: {
                    useAverage: true
                },
                CPU: {
                    useAverage: true,
                    useProcessors: true,
                    processorCoefficient: 1
                },
                ProcessingTime: {
                    useAverage: true
                }
            }
        },
        WeightedRandom: {
            nodeWeight: 10,
            useWeights: false
        }
    }
};

export default class IgniteClusterDefaults {
    constructor() {
        Object.assign(this, DFLT_CLUSTER);
    }
}
