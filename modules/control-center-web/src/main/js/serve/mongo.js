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

// Fire me up!

module.exports = {
    implements: 'mongo',
    inject: ['require(mongoose-deep-populate)', 'require(passport-local-mongoose)', 'settings', 'ignite_modules/mongo:*']
};

module.exports.factory = function(deepPopulatePlugin, passportMongo, settings, pluginMongo) {
    const mongoose = require('mongoose');

    // Use native promises
    mongoose.Promise = global.Promise;

    const deepPopulate = deepPopulatePlugin(mongoose);

    // Connect to mongoDB database.
    mongoose.connect(settings.mongoUrl, {server: {poolSize: 4}});

    const Schema = mongoose.Schema;
    const ObjectId = mongoose.Schema.Types.ObjectId;
    const result = { connection: mongoose.connection };

    result.ObjectId = ObjectId;

    // Define Account schema.
    const AccountSchema = new Schema({
        username: String,
        email: String,
        company: String,
        country: String,
        lastLogin: Date,
        admin: Boolean,
        token: String,
        resetPasswordToken: String
    });

    // Install passport plugin.
    AccountSchema.plugin(passportMongo, {
        usernameField: 'email', limitAttempts: true, lastLoginField: 'lastLogin',
        usernameLowerCase: true
    });

    // Configure transformation to JSON.
    AccountSchema.set('toJSON', {
        transform: (doc, ret) => {
            return {
                _id: ret._id,
                email: ret.email,
                username: ret.username,
                company: ret.company,
                country: ret.country,
                admin: ret.admin,
                token: ret.token,
                lastLogin: ret.lastLogin
            };
        }
    });

    // Define Account model.
    result.Account = mongoose.model('Account', AccountSchema);

    // Define Space model.
    result.Space = mongoose.model('Space', new Schema({
        name: String,
        owner: {type: ObjectId, ref: 'Account'},
        usedBy: [{
            permission: {type: String, enum: ['VIEW', 'FULL']},
            account: {type: ObjectId, ref: 'Account'}
        }]
    }));

    // Define Domain model schema.
    const DomainModelSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true},
        caches: [{type: ObjectId, ref: 'Cache'}],
        queryMetadata: {type: String, enum: ['Annotations', 'Configuration']},
        kind: {type: String, enum: ['query', 'store', 'both']},
        databaseSchema: String,
        databaseTable: String,
        keyType: String,
        valueType: String,
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
        fields: [{name: String, className: String}],
        aliases: [{field: String, alias: String}],
        indexes: [{
            name: String,
            indexType: {type: String, enum: ['SORTED', 'FULLTEXT', 'GEOSPATIAL']},
            fields: [{name: String, direction: Boolean}]
        }],
        demo: Boolean
    });

    // Define model of Domain models.
    result.DomainModel = mongoose.model('DomainModel', DomainModelSchema);

    // Define Cache schema.
    const CacheSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true},
        name: String,
        clusters: [{type: ObjectId, ref: 'Cluster'}],
        domains: [{type: ObjectId, ref: 'DomainModel'}],
        cacheMode: {type: String, enum: ['PARTITIONED', 'REPLICATED', 'LOCAL']},
        atomicityMode: {type: String, enum: ['ATOMIC', 'TRANSACTIONAL']},

        backups: Number,
        memoryMode: {type: String, enum: ['ONHEAP_TIERED', 'OFFHEAP_TIERED', 'OFFHEAP_VALUES']},
        offHeapMaxMemory: Number,
        startSize: Number,
        swapEnabled: Boolean,

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
                }
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
                hibernateProperties: [String]
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
        statisticsEnabled: Boolean,
        managementEnabled: Boolean,
        readFromBackup: Boolean,
        copyOnRead: Boolean,
        maxConcurrentAsyncOperations: Number,
        nearCacheEnabled: Boolean,
        nearConfiguration: {
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
        demo: Boolean
    });

    // Install deep populate plugin.
    CacheSchema.plugin(deepPopulate, {
        whitelist: ['domains']
    });

    // Define Cache model.
    result.Cache = mongoose.model('Cache', CacheSchema);

    const IgfsSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true},
        name: String,
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
            tokenDirectoryPath: String
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
        }
    });

    // Define IGFS model.
    result.Igfs = mongoose.model('Igfs', IgfsSchema);

    // Define Cluster schema.
    const ClusterSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true},
        name: String,
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
            kind: {type: String, enum: ['Vm', 'Multicast', 'S3', 'Cloud', 'GoogleStorage', 'Jdbc', 'SharedFs']},
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
                bucketName: String
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
                initSchema: Boolean
            },
            SharedFs: {
                path: String
            }
        },
        atomicConfiguration: {
            backups: Number,
            cacheMode: {type: String, enum: ['LOCAL', 'REPLICATED', 'PARTITIONED']},
            atomicSequenceReserveSize: Number
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
        rebalanceThreadPoolSize: Number
    });

    // Install deep populate plugin.
    ClusterSchema.plugin(deepPopulate, {
        whitelist: [
            'caches',
            'caches.domains',
            'igfss'
        ]
    });

    // Define Cluster model.
    result.Cluster = mongoose.model('Cluster', ClusterSchema);

    result.ClusterDefaultPopulate = '';

    // Define Notebook schema.
    const NotebookSchema = new Schema({
        space: {type: ObjectId, ref: 'Space', index: true},
        name: String,
        expandedParagraphs: [Number],
        paragraphs: [{
            name: String,
            query: String,
            editor: Boolean,
            result: {type: String, enum: ['none', 'table', 'bar', 'pie', 'line', 'area']},
            pageSize: Number,
            timeLineSpan: String,
            hideSystemColumns: Boolean,
            cacheName: String,
            chartsOptions: {barChart: {stacked: Boolean}, areaChart: {style: String}},
            rate: {
                value: Number,
                unit: Number
            }
        }]
    });

    // Define Notebook model.
    result.Notebook = mongoose.model('Notebook', NotebookSchema);

    result.handleError = function(res, err) {
        // TODO IGNITE-843 Send error to admin
        res.status(err.code || 500).send(err.message);
    };

    /**
     * Query for user spaces.
     *
     * @param userId User ID.
     * @returns {Promise}
     */
    result.spaces = function(userId) {
        return result.Space.find({owner: userId}).lean().exec();
    };

    /**
     * Extract IDs from user spaces.
     *
     * @param userId User ID.
     * @returns {Promise}
     */
    result.spaceIds = function(userId) {
        return result.Space.find({owner: userId}).lean().exec()
            .then((spaces) => spaces.map((space) => space._id));
    };

    // Registering the routes of all plugin modules
    for (const name in pluginMongo) {
        if (pluginMongo.hasOwnProperty(name))
            pluginMongo[name].register(mongoose, deepPopulate, result);
    }

    return result;
};
