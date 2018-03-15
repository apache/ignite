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

const passportMongo = require('passport-local-mongoose');

// Fire me up!

/**
 * Module mongo schema.
 */
module.exports = {
    implements: 'mongo',
    inject: ['settings', 'mongoose']
};

const defineSchema = (mongoose) => {
    const Schema = mongoose.Schema;
    const ObjectId = mongoose.Schema.Types.ObjectId;
    const result = { connection: mongoose.connection };

    result.ObjectId = ObjectId;

    // Define Account schema.
    const AccountSchema = new Schema({
        firstName: String,
        lastName: String,
        email: String,
        phone: String,
        company: String,
        country: String,
        registered: Date,
        lastLogin: Date,
        lastActivity: Date,
        admin: Boolean,
        token: String,
        resetPasswordToken: String
    });

    // Install passport plugin.
    AccountSchema.plugin(passportMongo, {
        usernameField: 'email', limitAttempts: true, lastLoginField: 'lastLogin',
        usernameLowerCase: true
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
    AccountSchema.set('toJSON', { transform });

    // Configure transformation to JSON.
    AccountSchema.set('toObject', { transform });

    result.errCodes = {
        DUPLICATE_KEY_ERROR: 11000,
        DUPLICATE_KEY_UPDATE_ERROR: 11001
    };

    // Define Account model.
    result.Account = mongoose.model('Account', AccountSchema);

    // Define Space model.
    result.Space = mongoose.model('Space', new Schema({
        name: String,
        owner: {type: ObjectId, ref: 'Account'},
        demo: {type: Boolean, default: false},
        usedBy: [{
            permission: {type: String, enum: ['VIEW', 'FULL']},
            account: {type: ObjectId, ref: 'Account'}
        }]
    }));

    // Define Domain model schema.
    const DomainModelSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
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
        fields: [{name: String, className: String}],
        aliases: [{field: String, alias: String}],
        indexes: [{
            name: String,
            indexType: {type: String, enum: ['SORTED', 'FULLTEXT', 'GEOSPATIAL']},
            fields: [{name: String, direction: Boolean}]
        }],
        generatePojo: Boolean
    });

    DomainModelSchema.index({valueType: 1, space: 1}, {unique: true});

    // Define model of Domain models.
    result.DomainModel = mongoose.model('DomainModel', DomainModelSchema);

    // Define Cache schema.
    const CacheSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true, required: true},
        name: {type: String},
        groupName: {type: String},
        clusters: [{type: ObjectId, ref: 'Cluster'}],
        domains: [{type: ObjectId, ref: 'DomainModel'}],
        cacheMode: {type: String, enum: ['PARTITIONED', 'REPLICATED', 'LOCAL']},
        atomicityMode: {type: String, enum: ['ATOMIC', 'TRANSACTIONAL']},
        partitionLossPolicy: {type: String, enum: ['READ_ONLY_SAFE', 'READ_ONLY_ALL', 'READ_WRITE_SAFE', 'READ_WRITE_ALL', 'IGNORE']},

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
        offHeapMaxMemory: Number,
        startSize: Number,
        swapEnabled: Boolean,

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

        invalidate: Boolean,
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
        topologyValidator: String
    });

    CacheSchema.index({name: 1, space: 1}, {unique: true});

    // Define Cache model.
    result.Cache = mongoose.model('Cache', CacheSchema);

    const IgfsSchema = new Schema({
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
            uri: String,
            cfgPath: String,
            userName: String
        },
        colocateMetadata: Boolean,
        relaxedConsistency: Boolean,
        updateFileLengthOnFlush: Boolean
    });

    IgfsSchema.index({name: 1, space: 1}, {unique: true});

    // Define IGFS model.
    result.Igfs = mongoose.model('Igfs', IgfsSchema);

    // Define Cluster schema.
    const ClusterSchema = new Schema({
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
            kind: {type: String, enum: ['Vm', 'Multicast', 'S3', 'Cloud', 'GoogleStorage', 'Jdbc', 'SharedFs', 'ZooKeeper', 'Kubernetes']},
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
                clientConfiguration: {
                    protocol: {type: String, enum: ['HTTP', 'HTTPS']},
                    maxConnections: Number,
                    userAgent: String,
                    localAddress: String,
                    proxyHost: String,
                    proxyPort: Number,
                    proxyUsername: String,
                    proxyDomain: String,
                    proxyWorkstation: String,
                    retryPolicy: {
                        kind: {type: String, enum: ['Default', 'DefaultMaxRetries', 'DynamoDB', 'DynamoDBMaxRetries', 'Custom', 'CustomClass']},
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
                    secureRandom: String
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
                    kind: {type: String, enum: ['ExponentialBackoff', 'BoundedExponentialBackoff', 'UntilElapsed',
                        'NTimes', 'OneTime', 'Forever', 'Custom']},
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
            }
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
                enum: Boolean
            }],
            compactFooter: Boolean
        },
        caches: [{type: ObjectId, ref: 'Cache'}],
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
            addressResolver: String
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
            txManagerFactory: String
        },
        sslEnabled: Boolean,
        sslContextFactory: {
            keyAlgorithm: String,
            keyStoreFilePath: String,
            keyStoreType: String,
            protocol: String,
            trustStoreFilePath: String,
            trustStoreType: String,
            trustManagers: [String]
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
                clientConfiguration: {
                    protocol: {type: String, enum: ['HTTP', 'HTTPS']},
                    maxConnections: Number,
                    userAgent: String,
                    localAddress: String,
                    proxyHost: String,
                    proxyPort: Number,
                    proxyUsername: String,
                    proxyDomain: String,
                    proxyWorkstation: String,
                    retryPolicy: {
                        kind: {type: String, enum: ['Default', 'DefaultMaxRetries', 'DynamoDB', 'DynamoDBMaxRetries', 'Custom']},
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
                    secureRandom: String
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
            threadPoolSize: Number
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
        workDirectory: String,
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
            walCompactionEnabled: Boolean
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
            subIntervals: Number
        }
    });

    ClusterSchema.index({name: 1, space: 1}, {unique: true});

    // Define Cluster model.
    result.Cluster = mongoose.model('Cluster', ClusterSchema);

    // Define Notebook schema.
    const NotebookSchema = new Schema({
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
            lazy: {type: Boolean, default: false}
        }]
    });

    NotebookSchema.index({name: 1, space: 1}, {unique: true});

    // Define Notebook model.
    result.Notebook = mongoose.model('Notebook', NotebookSchema);

    // Define Activities schema.
    const ActivitiesSchema = new Schema({
        owner: {type: ObjectId, ref: 'Account'},
        date: Date,
        group: String,
        action: String,
        amount: { type: Number, default: 0 }
    });

    ActivitiesSchema.index({ owner: 1, group: 1, action: 1, date: 1}, { unique: true });

    // Define Activities model.
    result.Activities = mongoose.model('Activities', ActivitiesSchema);

    // Define Notifications schema.
    const NotificationsSchema = new Schema({
        owner: {type: ObjectId, ref: 'Account'},
        date: Date,
        message: String,
        isShown: Boolean
    });

    // Define Notifications model.
    result.Notifications = mongoose.model('Notifications', NotificationsSchema);

    result.handleError = function(res, err) {
        // TODO IGNITE-843 Send error to admin
        res.status(err.code || 500).send(err.message);
    };

    return result;
};

module.exports.factory = function(settings, mongoose) {
    // Use native promises
    mongoose.Promise = global.Promise;

    console.log('Trying to connect to local MongoDB...');

    // Connect to mongoDB database.
    return mongoose.connect(settings.mongoUrl, {server: {poolSize: 4}})
        .then(() => defineSchema(mongoose))
        .catch(() => {
            console.log('Failed to connect to local MongoDB, will try to download and start embedded MongoDB');

            const {MongodHelper} = require('mongodb-prebuilt');
            const {MongoDBDownload} = require('mongodb-download');

            const helper = new MongodHelper(['--port', '27017', '--dbpath', `${process.cwd()}/user_data`]);

            helper.mongoBin.mongoDBPrebuilt.mongoDBDownload = new MongoDBDownload({
                downloadDir: `${process.cwd()}/libs/mongodb`,
                version: '3.4.7'
            });

            let mongodRun;

            if (settings.packaged) {
                mongodRun = new Promise((resolve, reject) => {
                    helper.resolveLink = resolve;
                    helper.rejectLink = reject;

                    helper.mongoBin.runCommand()
                        .then(() => {
                            helper.mongoBin.childProcess.removeAllListeners('close');

                            helper.mongoBin.childProcess.stderr.on('data', (data) => helper.stderrHandler(data));
                            helper.mongoBin.childProcess.stdout.on('data', (data) => helper.stdoutHandler(data));
                            helper.mongoBin.childProcess.on('close', (code) => helper.closeHandler(code));
                        });
                });
            }
            else
                mongodRun = helper.run();

            return mongodRun
                .catch((err) => {
                    console.log('Failed to start embedded MongoDB', err);

                    return Promise.reject(err);
                })
                .then(() => {
                    console.log('Embedded MongoDB successfully started');

                    return mongoose.connect(settings.mongoUrl, {server: {poolSize: 4}})
                        .catch((err) => {
                            console.log('Failed to connect to embedded MongoDB', err);

                            return Promise.reject(err);
                        });
                })
                .then(() => defineSchema(mongoose))
                .then((mongo) => {
                    if (settings.packaged) {
                        return mongo.Account.count()
                            .then((count) => {
                                if (count === 0) {
                                    return Promise.all([
                                        mongo.Account.create({
                                            _id: '59fc0c25e145c32be0f83b33',
                                            salt: '7b4ccb9e375508a8f87c8f347083ce98cb8785d857dd18208f9a480e992a26bb',
                                            hash: '909d5ed6e0b0a656ef542e2e8e851e9eb00cfb77984e0a6b4597c335d1436a577b3b289601eb8d1f3646e488cd5ea2bbb3e97fcc131cd6a9571407a45b1817bf1af1dd0ccdd070f07733da19e636ff9787369c5f38f86075f78c60809fe4a52288a68ca38aae0ad2bd0cc77b4cae310abf260e9523d361fd9be60e823a7d8e73954ddb18091e668acd3f57baf9fa7db4267e198d829761997a4741734335589ab62793ceb089e8fffe6e5b0e86f332b33a3011ba44e6efd29736f31cbd2b2023e5173baf517f337eb7a4321ea2b67ec827cffa271d26d3f2def93b5efa3ae7e6e327e55feb121ee96b8ff5016527cc7d854a9b49b44c993387c1093705cb26b1802a2e4c1d34508fb93d051d7e5e2e6cc65b6048a999f94c369973b46b204295f0b2f23f8e30723f9e984ddb2c53dcbf0a77a6d0795d44c3ad97a4ae49d6767db9630e2ef76c2069da87088f1400b1292df9bd787122b2cfef1f26a884a298a0bab3d6e6b689381cf6389d2f019e6cd19e82c84048bacfdd1bee946f9d40dda040be426e583abf92529a1c4f032d5058a9799a77e6642312b8d231d79300d5d0d3f74d62797f9d192e8581698e9539812a539ef1b9fbf718f44dd549896ea9449f6ea744586222e5fc29dfcd5eb79e7646ad3d37868f5073833c554853dee6b067bf2bbfab44c011f2de98a8570292f8109b6bde11e3be51075a656c32b521b7',
                                            email: 'admin@admin',
                                            firstName: 'admin',
                                            lastName: 'admin',
                                            company: 'admin',
                                            country: 'United States',
                                            admin: true,
                                            token: 'ruQvlWff09zqoVYyh6WJ',
                                            attempts: 0,
                                            resetPasswordToken: 'O2GWgOkKkhqpDcxjYnSP'
                                        }),
                                        mongo.Space.create({
                                            _id: '59fc0c26e145c32be0f83b34',
                                            name: 'Personal space',
                                            owner: '59fc0c25e145c32be0f83b33',
                                            usedBy: [],
                                            demo: false
                                        })
                                    ]);
                                }
                            })
                            .then(() => mongo)
                            .catch(() => mongo);
                    }

                    return mongo;
                });
        });
};
