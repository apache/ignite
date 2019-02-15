/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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




