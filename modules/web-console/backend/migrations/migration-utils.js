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

function log(msg) {
    console.log(`[${new Date().toISOString()}] [INFO ] ${msg}`);
}

function error(msg, err) {
    console.log(`[${new Date().toISOString()}] [ERROR] ${msg}.` + (err ? ` Error: ${err}` : ''));
}

function recreateIndex0(done, model, oldIdxName, oldIdx, newIdx) {
    return model.indexExists(oldIdxName)
        .then((exists) => {
            if (exists) {
                return model.dropIndex(oldIdx)
                    .then(() => model.createIndex(newIdx, {unique: true, background: false}));
            }
        })
        .then(() => done())
        .catch((err) => {
            if (err.code === 12587) {
                log(`Background operation in progress for: ${oldIdxName}, will retry in 3 seconds.`);

                setTimeout(() => recreateIndex0(done, model, oldIdxName, oldIdx, newIdx), 3000);
            }
            else {
                log(`Failed to recreate index: ${err}`);

                done();
            }
        });
}

function recreateIndex(done, model, oldIdxName, oldIdx, newIdx) {
    setTimeout(() => recreateIndex0(done, model, oldIdxName, oldIdx, newIdx), 1000);
}

const LOST_AND_FOUND = 'LOST_AND_FOUND';

function getClusterForMigration(clustersModel, space) {
    return clustersModel.findOne({space, name: LOST_AND_FOUND}).lean().exec()
        .then((cluster) => {
            if (cluster)
                return cluster;

            return clustersModel.create({
                space,
                name: LOST_AND_FOUND,
                connector: {noDelay: true},
                communication: {tcpNoDelay: true},
                igfss: [],
                caches: [],
                binaryConfiguration: {
                    compactFooter: true,
                    typeConfigurations: []
                },
                discovery: {
                    kind: 'Multicast',
                    Multicast: {addresses: ['127.0.0.1:47500..47510']},
                    Vm: {addresses: ['127.0.0.1:47500..47510']}
                }
            });
        });
}

function getCacheForMigration(clustersModel, cachesModel, space) {
    return cachesModel.findOne({space, name: LOST_AND_FOUND})
        .then((cache) => {
            if (cache)
                return cache;

            return getClusterForMigration(clustersModel, space)
                .then((cluster) => {
                    return cachesModel.create({
                        space,
                        name: LOST_AND_FOUND,
                        clusters: [cluster._id],
                        domains: [],
                        cacheMode: 'PARTITIONED',
                        atomicityMode: 'ATOMIC',
                        readFromBackup: true,
                        copyOnRead: true,
                        readThrough: false,
                        writeThrough: false,
                        sqlFunctionClasses: [],
                        writeBehindCoalescing: true,
                        cacheStoreFactory: {
                            CacheHibernateBlobStoreFactory: {hibernateProperties: []},
                            CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}
                        },
                        nearConfiguration: {},
                        evictionPolicy: {}
                    });
                })
                .then((cache) => {
                    return clustersModel.update({_id: cache.clusters[0]}, {$addToSet: {caches: cache._id}}).exec()
                        .then(() => cache);
                });
        });
}

module.exports = {
    log,
    error,
    recreateIndex,
    getClusterForMigration,
    getCacheForMigration
};




