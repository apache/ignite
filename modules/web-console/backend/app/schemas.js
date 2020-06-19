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

'use strict';

const mongoose = require('mongoose');
const passportMongo = require('passport-local-mongoose');

// Fire me up!

/**
 * Module mongo schema.
 */
module.exports = {
    implements: 'schemas',
    inject: []
};

module.exports.factory = function() {
    const Schema = mongoose.Schema;
    const ObjectId = mongoose.Schema.Types.ObjectId;

    // Define Account schema.
    const Account = new Schema({
        firstName: String,
        lastName: String,
        email: {type: String, unique: true},
        phone: String,
        company: String,
        country: String,
        registered: Date,
        lastLogin: Date,
        lastActivity: Date,
        admin: Boolean,
        token: String,
        resetPasswordToken: String,
        activated: {type: Boolean, default: false},
        activationSentAt: Date,
        activationToken: String
    });

    // Install passport plugin.
    Account.plugin(passportMongo, {
        usernameField: 'email', limitAttempts: true, lastLoginField: 'lastLogin',
        usernameLowerCase: true,
        errorMessages: {
            UserExistsError: 'A user with the given email is already registered'
        }
    });

    const transform = (doc, ret) => {
        return {
            _id: ret._id,
            email: ret.email,
            phone: ret.phone,
            firstName: ret.firstName,
            lastName: ret.lastName,
            company: ret.company,
            country: ret.country,
            admin: ret.admin,
            token: ret.token,
            registered: ret.registered,
            lastLogin: ret.lastLogin,
            lastActivity: ret.lastActivity
        };
    };

    // Configure transformation to JSON.
    Account.set('toJSON', {transform});

    // Configure transformation to JSON.
    Account.set('toObject', {transform});

    // Define Space schema.
    const Space = new Schema({
        name: String,
        owner: {type: ObjectId, ref: 'Account'},
        demo: {type: Boolean, default: false},
        usedBy: [{
            permission: {type: String, enum: ['VIEW', 'FULL']},
            account: {type: ObjectId, ref: 'Account'}
        }]
    });

    // Define Domain model schema.
    const DomainModel = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
        clusters: [{type: ObjectId, ref: 'Cluster'}],
        caches: [{type: ObjectId, ref: 'Cache'}],
        queryMetadata: {type: String, enum: ['Annotations', 'Configuration']},
        kind: {type: String, enum: ['query', 'store', 'both']},
        tableName: String,
        keyFieldName: String,
        valueFieldName: String,
        databaseSchema: String,
        databaseTable: String,
        keyType: String,
        valueType: {type: String},
        keyFields: [{
            databaseFieldName: String,
            databaseFieldType: String,
            javaFieldName: String,
            javaFieldType: String
        }],
        valueFields: [{
            databaseFieldName: String,
            databaseFieldType: String,
            javaFieldName: String,
            javaFieldType: String
        }],
        queryKeyFields: [String],
        fields: [{
            name: String,
            className: String,
            notNull: Boolean,
            defaultValue: String,
            precision: Number,
            scale: Number
        }],
        aliases: [{field: String, alias: String}],
        indexes: [{
            name: String,
            indexType: {type: String, enum: ['SORTED', 'FULLTEXT', 'GEOSPATIAL']},
            fields: [{name: String, direction: Boolean}],
            inlineSizeType: Number,
            inlineSize: Number
        }],
        generatePojo: Boolean
    });

    DomainModel.index({valueType: 1, space: 1, clusters: 1}, {unique: true});

    // Define Cache schema.
    const Cache = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
        name: {type: String},
        groupName: {type: String},
        clusters: [{type: ObjectId, ref: 'Cluster'}],
        domains: [{type: ObjectId, ref: 'DomainModel'}],
        cacheMode: {type: String, enum: ['PARTITIONED', 'REPLICATED', 'LOCAL']},
        atomicityMode: {type: String, enum: ['ATOMIC', 'TRANSACTIONAL', 'TRANSACTIONAL_SNAPSHOT']},
        partitionLossPolicy: {
            type: String,
            enum: ['READ_ONLY_SAFE', 'READ_ONLY_ALL', 'READ_WRITE_SAFE', 'READ_WRITE_ALL', 'IGNORE']
        },

        affinity: {
            kind: {type: String, enum: ['Default', 'Rendezvous', 'Fair', 'Custom']},
            Rendezvous: {
                affinityBackupFilter: String,
                partitions: Number,
                excludeNeighbors: Boolean
            },
            Fair: {
                affinityBackupFilter: String,
                partitions: Number,
                excludeNeighbors: Boolean
            },
            Custom: {
                className: String
            }
        },

        affinityMapper: String,

        nodeFilter: {
            kind: {type: String, enum: ['Default', 'Exclude', 'IGFS', 'OnNodes', 'Custom']},
            Exclude: {
                nodeId: String
            },
            IGFS: {
                igfs: {type: ObjectId, ref: 'Igfs'}
            },
            Custom: {
                className: String
            }
        },

        backups: Number,
        memoryMode: {type: String, enum: ['ONHEAP_TIERED', 'OFFHEAP_TIERED', 'OFFHEAP_VALUES']},
        offHeapMode: Number,
        offHeapMaxMemory: Number,
        startSize: Number,
        swapEnabled: Boolean,
        cacheWriterFactory: String,
        cacheLoaderFactory: String,
        expiryPolicyFactory: String,
        interceptor: String,
        storeByValue: Boolean,
        eagerTtl: {type: Boolean, default: true},
        encryptionEnabled: Boolean,
        eventsDisabled: Boolean,

        keyConfiguration: [{
            typeName: String,
            affinityKeyFieldName: String
        }],

        cacheStoreSessionListenerFactories: [String],

        onheapCacheEnabled: Boolean,

        evictionPolicy: {
            kind: {type: String, enum: ['LRU', 'FIFO', 'SORTED']},
            LRU: {
                batchSize: Number,
                maxMemorySize: Number,
                maxSize: Number
            },
            FIFO: {
                batchSize: Number,
                maxMemorySize: Number,
                maxSize: Number
            },
            SORTED: {
                batchSize: Number,
                maxMemorySize: Number,
                maxSize: Number
            }
        },

        rebalanceMode: {type: String, enum: ['SYNC', 'ASYNC', 'NONE']},
        rebalanceBatchSize: Number,
        rebalanceBatchesPrefetchCount: Number,
        rebalanceOrder: Number,
        rebalanceDelay: Number,
        rebalanceTimeout: Number,
        rebalanceThrottle: Number,

        cacheStoreFactory: {
            kind: {
                type: String,
                enum: ['CacheJdbcPojoStoreFactory', 'CacheJdbcBlobStoreFactory', 'CacheHibernateBlobStoreFactory']
            },
            CacheJdbcPojoStoreFactory: {
                dataSourceBean: String,
                dialect: {
                    type: String,
                    enum: ['Generic', 'Oracle', 'DB2', 'SQLServer', 'MySQL', 'PostgreSQL', 'H2']
                },
                implementationVersion: String,
                batchSize: Number,
                maximumPoolSize: Number,
                maximumWriteAttempts: Number,
                parallelLoadCacheMinimumThreshold: Number,
                hasher: String,
                transformer: String,
                sqlEscapeAll: Boolean
            },
            CacheJdbcBlobStoreFactory: {
                connectVia: {type: String, enum: ['URL', 'DataSource']},
                connectionUrl: String,
                user: String,
                dataSourceBean: String,
                dialect: {
                    type: String,
                    enum: ['Generic', 'Oracle', 'DB2', 'SQLServer', 'MySQL', 'PostgreSQL', 'H2']
                },
                initSchema: Boolean,
                createTableQuery: String,
                loadQuery: String,
                insertQuery: String,
                updateQuery: String,
                deleteQuery: String
            },
            CacheHibernateBlobStoreFactory: {
                hibernateProperties: [{name: String, value: String}]
            }
        },
        storeConcurrentLoadAllThreshold: Number,
        maxQueryIteratorsCount: Number,
        storeKeepBinary: Boolean,
        loadPreviousValue: Boolean,
        readThrough: Boolean,
        writeThrough: Boolean,

        writeBehindEnabled: Boolean,
        writeBehindBatchSize: Number,
        writeBehindFlushSize: Number,
        writeBehindFlushFrequency: Number,
        writeBehindFlushThreadCount: Number,
        writeBehindCoalescing: {type: Boolean, default: true},

        isInvalidate: Boolean,
        defaultLockTimeout: Number,
        atomicWriteOrderMode: {type: String, enum: ['CLOCK', 'PRIMARY']},
        writeSynchronizationMode: {type: String, enum: ['FULL_SYNC', 'FULL_ASYNC', 'PRIMARY_SYNC']},

        sqlEscapeAll: Boolean,
        sqlSchema: String,
        sqlOnheapRowCacheSize: Number,
        longQueryWarningTimeout: Number,
        sqlFunctionClasses: [String],
        snapshotableIndex: Boolean,
        queryDetailMetricsSize: Number,
        queryParallelism: Number,
        statisticsEnabled: Boolean,
        managementEnabled: Boolean,
        readFromBackup: Boolean,
        copyOnRead: Boolean,
        maxConcurrentAsyncOperations: Number,
        nearConfiguration: {
            enabled: Boolean,
            nearStartSize: Number,
            nearEvictionPolicy: {
                kind: {type: String, enum: ['LRU', 'FIFO', 'SORTED']},
                LRU: {
                    batchSize: Number,
                    maxMemorySize: Number,
                    maxSize: Number
                },
                FIFO: {
                    batchSize: Number,
                    maxMemorySize: Number,
                    maxSize: Number
                },
                SORTED: {
                    batchSize: Number,
                    maxMemorySize: Number,
                    maxSize: Number
                }
            }
        },
        clientNearConfiguration: {
            enabled: Boolean,
            nearStartSize: Number,
            nearEvictionPolicy: {
                kind: {type: String, enum: ['LRU', 'FIFO', 'SORTED']},
                LRU: {
                    batchSize: Number,
                    maxMemorySize: Number,
                    maxSize: Number
                },
                FIFO: {
                    batchSize: Number,
                    maxMemorySize: Number,
                    maxSize: Number
                },
                SORTED: {
                    batchSize: Number,
                    maxMemorySize: Number,
                    maxSize: Number
                }
            }
        },
        evictionFilter: String,
        memoryPolicyName: String,
        dataRegionName: String,
        sqlIndexMaxInlineSize: Number,
        topologyValidator: String,
        diskPageCompression: {type: String, enum: ['SKIP_GARBAGE', 'ZSTD', 'LZ4', 'SNAPPY']},
        diskPageCompressionLevel: Number
    });

    Cache.index({name: 1, space: 1, clusters: 1}, {unique: true});

    const Igfs = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
        name: {type: String},
        clusters: [{type: ObjectId, ref: 'Cluster'}],
        affinnityGroupSize: Number,
        blockSize: Number,
        streamBufferSize: Number,
        dataCacheName: String,
        metaCacheName: String,
        defaultMode: {type: String, enum: ['PRIMARY', 'PROXY', 'DUAL_SYNC', 'DUAL_ASYNC']},
        dualModeMaxPendingPutsSize: Number,
        dualModePutExecutorService: String,
        dualModePutExecutorServiceShutdown: Boolean,
        fragmentizerConcurrentFiles: Number,
        fragmentizerEnabled: Boolean,
        fragmentizerThrottlingBlockLength: Number,
        fragmentizerThrottlingDelay: Number,
        ipcEndpointConfiguration: {
            type: {type: String, enum: ['SHMEM', 'TCP']},
            host: String,
            port: Number,
            memorySize: Number,
            tokenDirectoryPath: String,
            threadCount: Number
        },
        ipcEndpointEnabled: Boolean,
        maxSpaceSize: Number,
        maximumTaskRangeLength: Number,
        managementPort: Number,
        pathModes: [{path: String, mode: {type: String, enum: ['PRIMARY', 'PROXY', 'DUAL_SYNC', 'DUAL_ASYNC']}}],
        perNodeBatchSize: Number,
        perNodeParallelBatchCount: Number,
        prefetchBlocks: Number,
        sequentialReadsBeforePrefetch: Number,
        trashPurgeTimeout: Number,
        secondaryFileSystemEnabled: Boolean,
        secondaryFileSystem: {
            userName: String,
            kind: {type: String, enum: ['Caching', 'Kerberos', 'Custom'], default: 'Caching'},
            uri: String,
            cfgPath: String,
            cfgPaths: [String],
            userNameMapper: {
                kind: {type: String, enum: ['Chained', 'Basic', 'Kerberos', 'Custom']},
                Chained: {
                    mappers: [{
                        kind: {type: String, enum: ['Basic', 'Kerberos', 'Custom']},
                        Basic: {
                            defaultUserName: String,
                            useDefaultUserName: Boolean,
                            mappings: [{
                                name: String,
                                value: String
                            }]
                        },
                        Kerberos: {
                            instance: String,
                            realm: String
                        },
                        Custom: {
                            className: String,
                        }
                    }]
                },
                Basic: {
                    defaultUserName: String,
                    useDefaultUserName: Boolean,
                    mappings: [{
                        name: String,
                        value: String
                    }]
                },
                Kerberos: {
                    instance: String,
                    realm: String
                },
                Custom: {
                    className: String,
                }
            },
            Kerberos: {
                keyTab: String,
                keyTabPrincipal: String,
                reloginInterval: Number
            },
            Custom: {
                className: String
            }
        },
        colocateMetadata: Boolean,
        relaxedConsistency: Boolean,
        updateFileLengthOnFlush: Boolean
    });

    Igfs.index({name: 1, space: 1, clusters: 1}, {unique: true});


    // Define Cluster schema.
    const Cluster = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
        name: {type: String},
        activeOnStart: {type: Boolean, default: true},
        localHost: String,
        discovery: {
            localAddress: String,
            localPort: Number,
            localPortRange: Number,
            addressResolver: String,
            socketTimeout: Number,
            ackTimeout: Number,
            maxAckTimeout: Number,
            networkTimeout: Number,
            joinTimeout: Number,
            threadPriority: Number,
            heartbeatFrequency: Number,
            maxMissedHeartbeats: Number,
            maxMissedClientHeartbeats: Number,
            topHistorySize: Number,
            listener: String,
            dataExchange: String,
            metricsProvider: String,
            reconnectCount: Number,
            statisticsPrintFrequency: Number,
            ipFinderCleanFrequency: Number,
            authenticator: String,
            forceServerMode: Boolean,
            clientReconnectDisabled: Boolean,
            connectionRecoveryTimeout: Number,
            reconnectDelay: Number,
            soLinger: Number,
            kind: {
                type: String,
                enum: ['Vm', 'Multicast', 'S3', 'Cloud', 'GoogleStorage', 'Jdbc', 'SharedFs', 'ZooKeeper', 'Kubernetes']
            },
            Vm: {
                addresses: [String]
            },
            Multicast: {
                multicastGroup: String,
                multicastPort: Number,
                responseWaitTime: Number,
                addressRequestAttempts: Number,
                localAddress: String,
                addresses: [String]
            },
            S3: {
                bucketName: String,
                bucketEndpoint: String,
                SSEAlgorithm: String,
                clientConfiguration: {
                    protocol: {type: String, enum: ['HTTP', 'HTTPS']},
                    maxConnections: Number,
                    userAgentPrefix: String,
                    userAgentSuffix: String,
                    localAddress: String,
                    proxyHost: String,
                    proxyPort: Number,
                    proxyUsername: String,
                    proxyDomain: String,
                    proxyWorkstation: String,
                    retryPolicy: {
                        kind: {
                            type: String,
                            enum: ['Default', 'DefaultMaxRetries', 'DynamoDB', 'DynamoDBMaxRetries', 'Custom', 'CustomClass']
                        },
                        DefaultMaxRetries: {
                            maxErrorRetry: Number
                        },
                        DynamoDBMaxRetries: {
                            maxErrorRetry: Number
                        },
                        Custom: {
                            retryCondition: String,
                            backoffStrategy: String,
                            maxErrorRetry: Number,
                            honorMaxErrorRetryInClientConfig: Boolean
                        },
                        CustomClass: {
                            className: String
                        }
                    },
                    maxErrorRetry: Number,
                    socketTimeout: Number,
                    connectionTimeout: Number,
                    requestTimeout: Number,
                    useReaper: Boolean,
                    useGzip: Boolean,
                    signerOverride: String,
                    preemptiveBasicProxyAuth: Boolean,
                    connectionTTL: Number,
                    connectionMaxIdleMillis: Number,
                    useTcpKeepAlive: Boolean,
                    dnsResolver: String,
                    responseMetadataCacheSize: Number,
                    secureRandom: String,
                    cacheResponseMetadata: {type: Boolean, default: true},
                    clientExecutionTimeout: Number,
                    nonProxyHosts: String,
                    socketSendBufferSizeHint: Number,
                    socketReceiveBufferSizeHint: Number,
                    useExpectContinue: {type: Boolean, default: true},
                    useThrottleRetries: {type: Boolean, default: true}
                }
            },
            Cloud: {
                credential: String,
                credentialPath: String,
                identity: String,
                provider: String,
                regions: [String],
                zones: [String]
            },
            GoogleStorage: {
                projectName: String,
                bucketName: String,
                serviceAccountP12FilePath: String,
                serviceAccountId: String,
                addrReqAttempts: String
            },
            Jdbc: {
                initSchema: Boolean,
                dataSourceBean: String,
                dialect: {
                    type: String,
                    enum: ['Generic', 'Oracle', 'DB2', 'SQLServer', 'MySQL', 'PostgreSQL', 'H2']
                }
            },
            SharedFs: {
                path: String
            },
            ZooKeeper: {
                curator: String,
                zkConnectionString: String,
                retryPolicy: {
                    kind: {
                        type: String, enum: ['ExponentialBackoff', 'BoundedExponentialBackoff', 'UntilElapsed',
                            'NTimes', 'OneTime', 'Forever', 'Custom']
                    },
                    ExponentialBackoff: {
                        baseSleepTimeMs: Number,
                        maxRetries: Number,
                        maxSleepMs: Number
                    },
                    BoundedExponentialBackoff: {
                        baseSleepTimeMs: Number,
                        maxSleepTimeMs: Number,
                        maxRetries: Number
                    },
                    UntilElapsed: {
                        maxElapsedTimeMs: Number,
                        sleepMsBetweenRetries: Number
                    },
                    NTimes: {
                        n: Number,
                        sleepMsBetweenRetries: Number
                    },
                    OneTime: {
                        sleepMsBetweenRetry: Number
                    },
                    Forever: {
                        retryIntervalMs: Number
                    },
                    Custom: {
                        className: String
                    }
                },
                basePath: String,
                serviceName: String,
                allowDuplicateRegistrations: Boolean
            },
            Kubernetes: {
                serviceName: String,
                namespace: String,
                masterUrl: String,
                accountToken: String
            }
        },
        atomicConfiguration: {
            backups: Number,
            cacheMode: {type: String, enum: ['LOCAL', 'REPLICATED', 'PARTITIONED']},
            atomicSequenceReserveSize: Number,
            affinity: {
                kind: {type: String, enum: ['Default', 'Rendezvous', 'Custom']},
                Rendezvous: {
                    affinityBackupFilter: String,
                    partitions: Number,
                    excludeNeighbors: Boolean
                },
                Custom: {
                    className: String
                }
            },
            groupName: String
        },
        binaryConfiguration: {
            idMapper: String,
            nameMapper: String,
            serializer: String,
            typeConfigurations: [{
                typeName: String,
                idMapper: String,
                nameMapper: String,
                serializer: String,
                enum: Boolean,
                enumValues: [String]
            }],
            compactFooter: Boolean
        },
        caches: [{type: ObjectId, ref: 'Cache'}],
        models: [{type: ObjectId, ref: 'DomainModel'}],
        clockSyncSamples: Number,
        clockSyncFrequency: Number,
        deploymentMode: {type: String, enum: ['PRIVATE', 'ISOLATED', 'SHARED', 'CONTINUOUS']},
        discoveryStartupDelay: Number,
        igfsThreadPoolSize: Number,
        igfss: [{type: ObjectId, ref: 'Igfs'}],
        includeEventTypes: [String],
        eventStorage: {
            kind: {type: String, enum: ['Memory', 'Custom']},
            Memory: {
                expireAgeMs: Number,
                expireCount: Number,
                filter: String
            },
            Custom: {
                className: String
            }
        },
        managementThreadPoolSize: Number,
        marshaller: {
            kind: {type: String, enum: ['OptimizedMarshaller', 'JdkMarshaller']},
            OptimizedMarshaller: {
                poolSize: Number,
                requireSerializable: Boolean
            }
        },
        marshalLocalJobs: Boolean,
        marshallerCacheKeepAliveTime: Number,
        marshallerCacheThreadPoolSize: Number,
        metricsExpireTime: Number,
        metricsHistorySize: Number,
        metricsLogFrequency: Number,
        metricsUpdateFrequency: Number,
        networkTimeout: Number,
        networkSendRetryDelay: Number,
        networkSendRetryCount: Number,
        communication: {
            listener: String,
            localAddress: String,
            localPort: Number,
            localPortRange: Number,
            sharedMemoryPort: Number,
            directBuffer: Boolean,
            directSendBuffer: Boolean,
            idleConnectionTimeout: Number,
            connectTimeout: Number,
            maxConnectTimeout: Number,
            reconnectCount: Number,
            socketSendBuffer: Number,
            socketReceiveBuffer: Number,
            messageQueueLimit: Number,
            slowClientQueueLimit: Number,
            tcpNoDelay: Boolean,
            ackSendThreshold: Number,
            unacknowledgedMessagesBufferSize: Number,
            socketWriteTimeout: Number,
            selectorsCount: Number,
            addressResolver: String,
            selectorSpins: Number,
            connectionsPerNode: Number,
            usePairedConnections: Boolean,
            filterReachableAddresses: Boolean
        },
        connector: {
            enabled: Boolean,
            jettyPath: String,
            host: String,
            port: Number,
            portRange: Number,
            idleTimeout: Number,
            idleQueryCursorTimeout: Number,
            idleQueryCursorCheckFrequency: Number,
            receiveBufferSize: Number,
            sendBufferSize: Number,
            sendQueueLimit: Number,
            directBuffer: Boolean,
            noDelay: Boolean,
            selectorCount: Number,
            threadPoolSize: Number,
            messageInterceptor: String,
            secretKey: String,
            sslEnabled: Boolean,
            sslClientAuth: Boolean,
            sslFactory: String
        },
        peerClassLoadingEnabled: Boolean,
        peerClassLoadingLocalClassPathExclude: [String],
        peerClassLoadingMissedResourcesCacheSize: Number,
        peerClassLoadingThreadPoolSize: Number,
        publicThreadPoolSize: Number,
        swapSpaceSpi: {
            kind: {type: String, enum: ['FileSwapSpaceSpi']},
            FileSwapSpaceSpi: {
                baseDirectory: String,
                readStripesNumber: Number,
                maximumSparsity: Number,
                maxWriteQueueSize: Number,
                writeBufferSize: Number
            }
        },
        systemThreadPoolSize: Number,
        timeServerPortBase: Number,
        timeServerPortRange: Number,
        transactionConfiguration: {
            defaultTxConcurrency: {type: String, enum: ['OPTIMISTIC', 'PESSIMISTIC']},
            defaultTxIsolation: {type: String, enum: ['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE']},
            defaultTxTimeout: Number,
            pessimisticTxLogLinger: Number,
            pessimisticTxLogSize: Number,
            txSerializableEnabled: Boolean,
            txManagerFactory: String,
            useJtaSynchronization: Boolean,
            txTimeoutOnPartitionMapExchange: Number, // 2.5
            deadlockTimeout: Number // 2.8
        },
        sslEnabled: Boolean,
        sslContextFactory: {
            keyAlgorithm: String,
            keyStoreFilePath: String,
            keyStoreType: String,
            protocol: String,
            trustStoreFilePath: String,
            trustStoreType: String,
            trustManagers: [String],
            cipherSuites: [String],
            protocols: [String]
        },
        rebalanceThreadPoolSize: Number,
        odbc: {
            odbcEnabled: Boolean,
            endpointAddress: String,
            socketSendBufferSize: Number,
            socketReceiveBufferSize: Number,
            maxOpenCursors: Number,
            threadPoolSize: Number
        },
        attributes: [{name: String, value: String}],
        collision: {
            kind: {type: String, enum: ['Noop', 'PriorityQueue', 'FifoQueue', 'JobStealing', 'Custom']},
            PriorityQueue: {
                parallelJobsNumber: Number,
                waitingJobsNumber: Number,
                priorityAttributeKey: String,
                jobPriorityAttributeKey: String,
                defaultPriority: Number,
                starvationIncrement: Number,
                starvationPreventionEnabled: Boolean
            },
            FifoQueue: {
                parallelJobsNumber: Number,
                waitingJobsNumber: Number
            },
            JobStealing: {
                activeJobsThreshold: Number,
                waitJobsThreshold: Number,
                messageExpireTime: Number,
                maximumStealingAttempts: Number,
                stealingEnabled: Boolean,
                stealingAttributes: [{name: String, value: String}],
                externalCollisionListener: String
            },
            Custom: {
                class: String
            }
        },
        failoverSpi: [{
            kind: {type: String, enum: ['JobStealing', 'Never', 'Always', 'Custom']},
            JobStealing: {
                maximumFailoverAttempts: Number
            },
            Always: {
                maximumFailoverAttempts: Number
            },
            Custom: {
                class: String
            }
        }],
        logger: {
            kind: {type: 'String', enum: ['Log4j2', 'Null', 'Java', 'JCL', 'SLF4J', 'Log4j', 'Custom']},
            Log4j2: {
                level: {type: String, enum: ['OFF', 'FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE', 'ALL']},
                path: String
            },
            Log4j: {
                mode: {type: String, enum: ['Default', 'Path']},
                level: {type: String, enum: ['OFF', 'FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE', 'ALL']},
                path: String
            },
            Custom: {
                class: String
            }
        },
        cacheKeyConfiguration: [{
            typeName: String,
            affinityKeyFieldName: String
        }],
        checkpointSpi: [{
            kind: {type: String, enum: ['FS', 'Cache', 'S3', 'JDBC', 'Custom']},
            FS: {
                directoryPaths: [String],
                checkpointListener: String
            },
            Cache: {
                cache: {type: ObjectId, ref: 'Cache'},
                checkpointListener: String
            },
            S3: {
                awsCredentials: {
                    kind: {type: String, enum: ['Basic', 'Properties', 'Anonymous', 'BasicSession', 'Custom']},
                    Properties: {
                        path: String
                    },
                    Custom: {
                        className: String
                    }
                },
                bucketNameSuffix: String,
                bucketEndpoint: String,
                SSEAlgorithm: String,
                clientConfiguration: {
                    protocol: {type: String, enum: ['HTTP', 'HTTPS']},
                    maxConnections: Number,
                    userAgentPrefix: String,
                    userAgentSuffix: String,
                    localAddress: String,
                    proxyHost: String,
                    proxyPort: Number,
                    proxyUsername: String,
                    proxyDomain: String,
                    proxyWorkstation: String,
                    retryPolicy: {
                        kind: {
                            type: String,
                            enum: ['Default', 'DefaultMaxRetries', 'DynamoDB', 'DynamoDBMaxRetries', 'Custom']
                        },
                        DefaultMaxRetries: {
                            maxErrorRetry: Number
                        },
                        DynamoDBMaxRetries: {
                            maxErrorRetry: Number
                        },
                        Custom: {
                            retryCondition: String,
                            backoffStrategy: String,
                            maxErrorRetry: Number,
                            honorMaxErrorRetryInClientConfig: Boolean
                        }
                    },
                    maxErrorRetry: Number,
                    socketTimeout: Number,
                    connectionTimeout: Number,
                    requestTimeout: Number,
                    useReaper: Boolean,
                    useGzip: Boolean,
                    signerOverride: String,
                    preemptiveBasicProxyAuth: Boolean,
                    connectionTTL: Number,
                    connectionMaxIdleMillis: Number,
                    useTcpKeepAlive: Boolean,
                    dnsResolver: String,
                    responseMetadataCacheSize: Number,
                    secureRandom: String,
                    cacheResponseMetadata: {type: Boolean, default: true},
                    clientExecutionTimeout: Number,
                    nonProxyHosts: String,
                    socketSendBufferSizeHint: Number,
                    socketReceiveBufferSizeHint: Number,
                    useExpectContinue: {type: Boolean, default: true},
                    useThrottleRetries: {type: Boolean, default: true}
                },
                checkpointListener: String
            },
            JDBC: {
                dataSourceBean: String,
                dialect: {
                    type: String,
                    enum: ['Generic', 'Oracle', 'DB2', 'SQLServer', 'MySQL', 'PostgreSQL', 'H2']
                },
                user: String,
                checkpointTableName: String,
                keyFieldName: String,
                keyFieldType: String,
                valueFieldName: String,
                valueFieldType: String,
                expireDateFieldName: String,
                expireDateFieldType: String,
                numberOfRetries: Number,
                checkpointListener: String
            },
            Custom: {
                className: String
            }
        }],
        clientConnectorConfiguration: {
            enabled: Boolean,
            host: String,
            port: Number,
            portRange: Number,
            socketSendBufferSize: Number,
            socketReceiveBufferSize: Number,
            tcpNoDelay: {type: Boolean, default: true},
            maxOpenCursorsPerConnection: Number,
            threadPoolSize: Number,
            idleTimeout: Number,
            jdbcEnabled: {type: Boolean, default: true},
            odbcEnabled: {type: Boolean, default: true},
            thinClientEnabled: {type: Boolean, default: true},
            sslEnabled: Boolean,
            useIgniteSslContextFactory: {type: Boolean, default: true},
            sslClientAuth: Boolean,
            sslContextFactory: String
        },
        loadBalancingSpi: [{
            kind: {type: String, enum: ['RoundRobin', 'Adaptive', 'WeightedRandom', 'Custom']},
            RoundRobin: {
                perTask: Boolean
            },
            Adaptive: {
                loadProbe: {
                    kind: {type: String, enum: ['Job', 'CPU', 'ProcessingTime', 'Custom']},
                    Job: {
                        useAverage: Boolean
                    },
                    CPU: {
                        useAverage: Boolean,
                        useProcessors: Boolean,
                        processorCoefficient: Number
                    },
                    ProcessingTime: {
                        useAverage: Boolean
                    },
                    Custom: {
                        className: String
                    }
                }
            },
            WeightedRandom: {
                nodeWeight: Number,
                useWeights: Boolean
            },
            Custom: {
                className: String
            }
        }],
        deploymentSpi: {
            kind: {type: String, enum: ['URI', 'Local', 'Custom']},
            URI: {
                uriList: [String],
                temporaryDirectoryPath: String,
                scanners: [String],
                listener: String,
                checkMd5: Boolean,
                encodeUri: Boolean
            },
            Local: {
                listener: String
            },
            Custom: {
                className: String
            }
        },
        warmupClosure: String,
        hadoopConfiguration: {
            mapReducePlanner: {
                kind: {type: String, enum: ['Weighted', 'Custom']},
                Weighted: {
                    localMapperWeight: Number,
                    remoteMapperWeight: Number,
                    localReducerWeight: Number,
                    remoteReducerWeight: Number,
                    preferLocalReducerThresholdWeight: Number
                },
                Custom: {
                    className: String
                }
            },
            finishedJobInfoTtl: Number,
            maxParallelTasks: Number,
            maxTaskQueueSize: Number,
            nativeLibraryNames: [String]
        },
        serviceConfigurations: [{
            name: String,
            service: String,
            maxPerNodeCount: Number,
            totalCount: Number,
            nodeFilter: {
                kind: {type: String, enum: ['Default', 'Exclude', 'IGFS', 'OnNodes', 'Custom']},
                Exclude: {
                    nodeId: String
                },
                IGFS: {
                    igfs: {type: ObjectId, ref: 'Igfs'}
                },
                Custom: {
                    className: String
                }
            },
            cache: {type: ObjectId, ref: 'Cache'},
            affinityKey: String
        }],
        cacheSanityCheckEnabled: {type: Boolean, default: true},
        classLoader: String,
        consistentId: String,
        failureDetectionTimeout: Number,
        clientFailureDetectionTimeout: Number,
        systemWorkerBlockedTimeout: Number,
        workDirectory: String,
        igniteHome: String,
        lateAffinityAssignment: Boolean,
        utilityCacheKeepAliveTime: Number,
        asyncCallbackPoolSize: Number,
        dataStreamerThreadPoolSize: Number,
        queryThreadPoolSize: Number,
        stripedPoolSize: Number,
        serviceThreadPoolSize: Number,
        utilityCacheThreadPoolSize: Number,
        executorConfiguration: [{
            name: String,
            size: Number
        }],
        dataStorageConfiguration: {
            systemRegionInitialSize: Number,
            systemRegionMaxSize: Number,
            pageSize: Number,
            concurrencyLevel: Number,
            defaultDataRegionConfiguration: {
                name: String,
                initialSize: Number,
                maxSize: Number,
                swapPath: String,
                pageEvictionMode: {type: String, enum: ['DISABLED', 'RANDOM_LRU', 'RANDOM_2_LRU']},
                evictionThreshold: Number,
                emptyPagesPoolSize: Number,
                metricsEnabled: Boolean,
                metricsSubIntervalCount: Number,
                metricsRateTimeInterval: Number,
                persistenceEnabled: Boolean,
                checkpointPageBufferSize: Number
            },
            dataRegionConfigurations: [{
                name: String,
                initialSize: Number,
                maxSize: Number,
                swapPath: String,
                pageEvictionMode: {type: String, enum: ['DISABLED', 'RANDOM_LRU', 'RANDOM_2_LRU']},
                evictionThreshold: Number,
                emptyPagesPoolSize: Number,
                metricsEnabled: Boolean,
                metricsSubIntervalCount: Number,
                metricsRateTimeInterval: Number,
                persistenceEnabled: Boolean,
                checkpointPageBufferSize: Number
            }],
            storagePath: String,
            metricsEnabled: Boolean,
            alwaysWriteFullPages: Boolean,
            checkpointFrequency: Number,
            checkpointThreads: Number,
            checkpointWriteOrder: {type: String, enum: ['RANDOM', 'SEQUENTIAL']},
            walPath: String,
            walArchivePath: String,
            walMode: {type: String, enum: ['DEFAULT', 'LOG_ONLY', 'BACKGROUND', 'NONE']},
            walSegments: Number,
            walSegmentSize: Number,
            walHistorySize: Number,
            walFlushFrequency: Number,
            walFsyncDelayNanos: Number,
            walRecordIteratorBufferSize: Number,
            lockWaitTime: Number,
            walBufferSize: Number,
            walThreadLocalBufferSize: Number,
            metricsSubIntervalCount: Number,
            metricsRateTimeInterval: Number,
            fileIOFactory: {type: String, enum: ['RANDOM', 'ASYNC']},
            walAutoArchiveAfterInactivity: Number,
            writeThrottlingEnabled: Boolean,
            walCompactionEnabled: Boolean,
            checkpointReadLockTimeout: Number,
            maxWalArchiveSize: Number,
            walCompactionLevel: Number
        },
        memoryConfiguration: {
            systemCacheInitialSize: Number,
            systemCacheMaxSize: Number,
            pageSize: Number,
            concurrencyLevel: Number,
            defaultMemoryPolicyName: String,
            defaultMemoryPolicySize: Number,
            memoryPolicies: [{
                name: String,
                initialSize: Number,
                maxSize: Number,
                swapFilePath: String,
                pageEvictionMode: {type: String, enum: ['DISABLED', 'RANDOM_LRU', 'RANDOM_2_LRU']},
                evictionThreshold: Number,
                emptyPagesPoolSize: Number,
                metricsEnabled: Boolean,
                subIntervals: Number,
                rateTimeInterval: Number
            }]
        },
        longQueryWarningTimeout: Number,
        sqlConnectorConfiguration: {
            enabled: Boolean,
            host: String,
            port: Number,
            portRange: Number,
            socketSendBufferSize: Number,
            socketReceiveBufferSize: Number,
            tcpNoDelay: {type: Boolean, default: true},
            maxOpenCursorsPerConnection: Number,
            threadPoolSize: Number
        },
        persistenceStoreConfiguration: {
            enabled: Boolean,
            persistentStorePath: String,
            metricsEnabled: Boolean,
            alwaysWriteFullPages: Boolean,
            checkpointingFrequency: Number,
            checkpointingPageBufferSize: Number,
            checkpointingThreads: Number,
            walStorePath: String,
            walArchivePath: String,
            walMode: {type: String, enum: ['DEFAULT', 'LOG_ONLY', 'BACKGROUND', 'NONE']},
            walSegments: Number,
            walSegmentSize: Number,
            walHistorySize: Number,
            walFlushFrequency: Number,
            walFsyncDelayNanos: Number,
            walRecordIteratorBufferSize: Number,
            lockWaitTime: Number,
            rateTimeInterval: Number,
            tlbSize: Number,
            subIntervals: Number,
            walAutoArchiveAfterInactivity: Number
        },
        encryptionSpi: {
            kind: {type: String, enum: ['Noop', 'Keystore', 'Custom']},
            Keystore: {
                keySize: Number,
                masterKeyName: String,
                keyStorePath: String
            },
            Custom: {
                className: String
            }
        },
        failureHandler: {
            kind: {type: String, enum: ['RestartProcess', 'StopNodeOnHalt', 'StopNode', 'Noop', 'Custom']},
            ignoredFailureTypes: [{type: String, enum: ['SEGMENTATION', 'SYSTEM_WORKER_TERMINATION',
                    'SYSTEM_WORKER_BLOCKED', 'CRITICAL_ERROR', 'SYSTEM_CRITICAL_OPERATION_TIMEOUT']}],
            StopNodeOnHalt: {
                tryStop: Boolean,
                timeout: Number
            },
            Custom: {
                className: String
            }
        },
        localEventListeners: [{
            className: String,
            eventTypes: [String]
        }],
        mvccVacuumThreadCount: Number,
        mvccVacuumFrequency: Number,
        authenticationEnabled: Boolean,
        sqlQueryHistorySize: Number,
        lifecycleBeans: [String],
        addressResolver: String,
        mBeanServer: String,
        networkCompressionLevel: Number,
        includeProperties: [String],
        cacheStoreSessionListenerFactories: [String],
        autoActivationEnabled: {type: Boolean, default: true},
        sqlSchemas: [String],
        communicationFailureResolver: String
    });

    Cluster.index({name: 1, space: 1}, {unique: true});

    // Define Notebook schema.
    const Notebook = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
        name: String,
        expandedParagraphs: [Number],
        paragraphs: [{
            name: String,
            query: String,
            editor: Boolean,
            result: {type: String, enum: ['none', 'table', 'bar', 'pie', 'line', 'area']},
            pageSize: Number,
            timeLineSpan: String,
            maxPages: Number,
            hideSystemColumns: Boolean,
            cacheName: String,
            useAsDefaultSchema: Boolean,
            chartsOptions: {barChart: {stacked: Boolean}, areaChart: {style: String}},
            rate: {
                value: Number,
                unit: Number
            },
            qryType: String,
            nonCollocatedJoins: {type: Boolean, default: false},
            enforceJoinOrder: {type: Boolean, default: false},
            lazy: {type: Boolean, default: false},
            collocated: Boolean
        }]
    });

    Notebook.index({name: 1, space: 1}, {unique: true});

    // Define Activities schema.
    const Activities = new Schema({
        owner: {type: ObjectId, ref: 'Account'},
        date: Date,
        group: String,
        action: String,
        amount: {type: Number, default: 0}
    });

    Activities.index({owner: 1, group: 1, action: 1, date: 1}, {unique: true});

    // Define Notifications schema.
    const Notifications = new Schema({
        owner: {type: ObjectId, ref: 'Account'},
        date: Date,
        message: String,
        isShown: Boolean
    });

    return {
        Space,
        Account,
        DomainModel,
        Cache,
        Igfs,
        Cluster,
        Notebook,
        Activities,
        Notifications
    };
};
