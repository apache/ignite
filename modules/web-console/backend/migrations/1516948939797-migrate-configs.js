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

const _ = require('lodash');

const log = require('./migration-utils').log;
const error = require('./migration-utils').error;

const getClusterForMigration = require('./migration-utils').getClusterForMigration;
const getCacheForMigration = require('./migration-utils').getCacheForMigration;

const _debug = false;
const DUPLICATE_KEY_ERROR = 11000;

let dup = 1;

function makeDup(name) {
    return name + `_dup_${dup++}`;
}

function linkCacheToCluster(clustersModel, cluster, cachesModel, cache, domainsModel) {
    return clustersModel.update({_id: cluster._id}, {$addToSet: {caches: cache._id}}).exec()
        .then(() => cachesModel.update({_id: cache._id}, {clusters: [cluster._id]}).exec())
        .then(() => {
            if (_.isEmpty(cache.domains))
                return Promise.resolve();

            return _.reduce(cache.domains, (start, domain) => start.then(() => {
                return domainsModel.update({_id: domain}, {clusters: [cluster._id]}).exec()
                    .then(() => clustersModel.update({_id: cluster._id}, {$addToSet: {models: domain}}).exec());
            }), Promise.resolve());
        })
        .catch((err) => error(`Failed link cache to cluster [cache=${cache.name}, cluster=${cluster.name}]`, err));
}

function cloneCache(clustersModel, cachesModel, domainsModel, cache) {
    const cacheId = cache._id;
    const clusters = cache.clusters;

    cache.clusters = [];

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind === null)
        delete cache.cacheStoreFactory.kind;

    return _.reduce(clusters, (start, cluster, idx) => start.then(() => {
        if (idx > 0) {
            delete cache._id;

            const newCache = _.clone(cache);
            const domainIds = newCache.domains;

            newCache.clusters = [cluster];
            newCache.domains = [];

            return clustersModel.update({_id: {$in: newCache.clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => cachesModel.create(newCache))
                .catch((err) => {
                    if (err.code === DUPLICATE_KEY_ERROR) {
                        const retryWith = makeDup(newCache.name);

                        error(`Failed to clone cache, will change cache name and retry [cache=${newCache.name}, retryWith=${retryWith}]`);

                        newCache.name = retryWith;

                        return cachesModel.create(newCache);
                    }

                    return Promise.reject(err);
                })
                .then((clone) => clustersModel.update({_id: {$in: newCache.clusters}}, {$addToSet: {caches: clone._id}}, {multi: true}).exec()
                    .then(() => clone))
                .then((clone) => {
                    if (_.isEmpty(domainIds))
                        return Promise.resolve();

                    return _.reduce(domainIds, (start, domainId) => start.then(() => {
                        return domainsModel.findOne({_id: domainId}).lean().exec()
                            .then((domain) => {
                                delete domain._id;

                                const newDomain = _.clone(domain);

                                newDomain.caches = [clone._id];
                                newDomain.clusters = [cluster];

                                return domainsModel.create(newDomain)
                                    .catch((err) => {
                                        if (err.code === DUPLICATE_KEY_ERROR) {
                                            const retryWith = makeDup(newDomain.valueType);

                                            error(`Failed to clone domain, will change type name and retry [cache=${newCache.name}, valueType=${newDomain.valueType}, retryWith=${retryWith}]`);

                                            newDomain.valueType = retryWith;

                                            return domainsModel.create(newDomain);
                                        }
                                    })
                                    .then((createdDomain) => {
                                        return clustersModel.update({_id: cluster}, {$addToSet: {models: createdDomain._id}}).exec()
                                            .then(() => cachesModel.update({_id: clone.id}, {$addToSet: {domains: createdDomain._id}}));
                                    })
                                    .catch((err) => error('Failed to clone domain during cache clone', err));
                            })
                            .catch((err) => error(`Failed to duplicate domain model[domain=${domainId}], cache=${clone.name}]`, err));
                    }), Promise.resolve());
                })
                .catch((err) => error(`Failed to clone cache[id=${cacheId}, name=${cache.name}]`, err));
        }

        return cachesModel.update({_id: cacheId}, {clusters: [cluster]}).exec()
            .then(() => clustersModel.update({_id: cluster}, {$addToSet: {models: {$each: cache.domains}}}).exec());
    }), Promise.resolve());
}

function migrateCache(clustersModel, cachesModel, domainsModel, cache) {
    const clustersCnt = _.size(cache.clusters);

    if (clustersCnt < 1) {
        if (_debug)
            log(`Found cache not linked to cluster [cache=${cache.name}]`);

        return getClusterForMigration(clustersModel, cache.space)
            .then((clusterLostFound) => linkCacheToCluster(clustersModel, clusterLostFound, cachesModel, cache, domainsModel));
    }

    if (clustersCnt > 1) {
        if (_debug)
            log(`Found cache linked to many clusters [cache=${cache.name}, clustersCnt=${clustersCnt}]`);

        return cloneCache(clustersModel, cachesModel, domainsModel, cache);
    }

    // Nothing to migrate, cache linked to cluster 1-to-1.
    return Promise.resolve();
}

function migrateCaches(clustersModel, cachesModel, domainsModel) {
    return cachesModel.find({}).lean().exec()
        .then((caches) => {
            const cachesCnt = _.size(caches);

            if (cachesCnt > 0) {
                log(`Caches to migrate: ${cachesCnt}`);

                return _.reduce(caches, (start, cache) => start.then(() => migrateCache(clustersModel, cachesModel, domainsModel, cache)), Promise.resolve())
                    .then(() => log('Caches migration finished.'));
            }

            return Promise.resolve();

        })
        .catch((err) => error('Caches migration failed', err));
}

function linkIgfsToCluster(clustersModel, cluster, igfsModel, igfs) {
    return clustersModel.update({_id: cluster._id}, {$addToSet: {igfss: igfs._id}}).exec()
        .then(() => igfsModel.update({_id: igfs._id}, {clusters: [cluster._id]}).exec())
        .catch((err) => error(`Failed link IGFS to cluster [IGFS=${igfs.name}, cluster=${cluster.name}]`, err));
}

function cloneIgfs(clustersModel, igfsModel, igfs) {
    const igfsId = igfs._id;
    const clusters = igfs.clusters;

    delete igfs._id;
    igfs.clusters = [];

    return _.reduce(clusters, (start, cluster, idx) => start.then(() => {
        const newIgfs = _.clone(igfs);

        newIgfs.clusters = [cluster];

        if (idx > 0) {
            return clustersModel.update({_id: {$in: newIgfs.clusters}}, {$pull: {igfss: igfsId}}, {multi: true}).exec()
                .then(() => igfsModel.create(newIgfs))
                .then((clone) => clustersModel.update({_id: {$in: newIgfs.clusters}}, {$addToSet: {igfss: clone._id}}, {multi: true}).exec())
                .catch((err) => error(`Failed to clone IGFS: id=${igfsId}, name=${igfs.name}]`, err));
        }

        return igfsModel.update({_id: igfsId}, {clusters: [cluster]}).exec();
    }), Promise.resolve());
}

function migrateIgfs(clustersModel, igfsModel, igfs) {
    const clustersCnt = _.size(igfs.clusters);

    if (clustersCnt < 1) {
        if (_debug)
            log(`Found IGFS not linked to cluster [IGFS=${igfs.name}]`);

        return getClusterForMigration(clustersModel, igfs.space)
            .then((clusterLostFound) => linkIgfsToCluster(clustersModel, clusterLostFound, igfsModel, igfs));
    }

    if (clustersCnt > 1) {
        if (_debug)
            log(`Found IGFS linked to many clusters [IGFS=${igfs.name}, clustersCnt=${clustersCnt}]`);

        return cloneIgfs(clustersModel, igfsModel, igfs);
    }

    // Nothing to migrate, IGFS linked to cluster 1-to-1.
    return Promise.resolve();
}

function migrateIgfss(clustersModel, igfsModel) {
    return igfsModel.find({}).lean().exec()
        .then((igfss) => {
            const igfsCnt = _.size(igfss);

            if (igfsCnt > 0) {
                log(`IGFS to migrate: ${igfsCnt}`);

                return _.reduce(igfss, (start, igfs) => start.then(() => migrateIgfs(clustersModel, igfsModel, igfs)), Promise.resolve())
                    .then(() => log('IGFS migration finished.'));
            }

            return Promise.resolve();
        })
        .catch((err) => error('IGFS migration failed', err));
}

function linkDomainToCluster(clustersModel, cluster, domainsModel, domain) {
    return clustersModel.update({_id: cluster._id}, {$addToSet: {models: domain._id}}).exec()
        .then(() => domainsModel.update({_id: domain._id}, {clusters: [cluster._id]}).exec())
        .catch((err) => error(`Failed link domain model to cluster [domain=${domain._id}, cluster=${cluster.name}]`, err));
}

function linkDomainToCache(cachesModel, cache, domainsModel, domain) {
    return cachesModel.update({_id: cache._id}, {$addToSet: {domains: domain._id}}).exec()
        .then(() => domainsModel.update({_id: domain._id}, {caches: [cache._id]}).exec())
        .catch((err) => error(`Failed link domain model to cache [cache=${cache.name}, domain=${domain._id}]`, err));
}

function migrateDomain(clustersModel, cachesModel, domainsModel, domain) {
    const cachesCnt = _.size(domain.caches);

    if (cachesCnt < 1) {
        if (_debug)
            log(`Found domain model not linked to cache [domain=${domain._id}]`);

        return getClusterForMigration(clustersModel, domain.space)
            .then((clusterLostFound) => linkDomainToCluster(clustersModel, clusterLostFound, domainsModel, domain))
            .then(() => getCacheForMigration(clustersModel, cachesModel, domain.space))
            .then((cacheLostFound) => linkDomainToCache(cachesModel, cacheLostFound, domainsModel, domain))
            .catch((err) => error(`Failed to migrate not linked domain [domain=${domain._id}]`, err));
    }

    if (_.isEmpty(domain.clusters)) {
        const cachesCnt = _.size(domain.caches);

        if (_debug)
            log(`Found domain model without cluster: [domain=${domain._id}, cachesCnt=${cachesCnt}]`);

        const grpByClusters = {};

        return cachesModel.find({_id: {$in: domain.caches}}).lean().exec()
            .then((caches) => {
                if (caches) {
                    _.forEach(caches, (cache) => {
                        const c = _.get(grpByClusters, cache.clusters[0]);

                        if (c)
                            c.push(cache._id);
                        else
                            grpByClusters[cache.clusters[0]] = [cache._id];
                    });

                    return _.reduce(_.keys(grpByClusters), (start, cluster, idx) => start.then(() => {
                        const domainId = domain._id;

                        const clusterCaches = grpByClusters[cluster];

                        if (idx > 0) {
                            delete domain._id;
                            domain.caches = clusterCaches;

                            return domainsModel.create(domain)
                                .then((clonedDomain) => {
                                    return cachesModel.update({_id: {$in: clusterCaches}}, {$addToSet: {domains: clonedDomain._id}}).exec()
                                        .then(() => clonedDomain);
                                })
                                .then((clonedDomain) => linkDomainToCluster(clustersModel, {_id: cluster, name: `stub${idx}`}, domainsModel, clonedDomain))
                                .then(() => {
                                    return cachesModel.update({_id: {$in: clusterCaches}}, {$pull: {domains: domainId}}, {multi: true}).exec();
                                });
                        }

                        return domainsModel.update(domainsModel.update({_id: domainId}, {caches: clusterCaches}).exec())
                            .then(() => linkDomainToCluster(clustersModel, {_id: cluster, name: `stub${idx}`}, domainsModel, domain));
                    }), Promise.resolve());
                }

                error(`Found domain with orphaned caches: [domain=${domain._id}, caches=${domain.caches}]`);

                return Promise.resolve();
            })
            .catch((err) => error(`Failed to migrate domain [domain=${domain._id}]`, err));
    }

    // Nothing to migrate, other domains will be migrated with caches.
    return Promise.resolve();
}

function migrateDomains(clustersModel, cachesModel, domainsModel) {
    return domainsModel.find({}).lean().exec()
        .then((domains) => {
            const domainsCnt = _.size(domains);

            if (domainsCnt > 0) {
                log(`Domain models to migrate: ${domainsCnt}`);

                return _.reduce(domains, (start, domain) => start.then(() => migrateDomain(clustersModel, cachesModel, domainsModel, domain)), Promise.resolve())
                    .then(() => log('Domain models migration finished.'));
            }

            return Promise.resolve();
        })
        .catch((err) => error('Domain models migration failed', err));
}

function deduplicate(title, model, name) {
    return model.find({}).lean().exec()
        .then((items) => {
            const sz = _.size(items);

            if (sz > 0) {
                log(`Deduplication of ${title} started...`);

                let cnt = 0;

                return _.reduce(items, (start, item) => start.then(() => {
                    const data = item[name];

                    const dataSz = _.size(data);

                    if (dataSz < 2)
                        return Promise.resolve();

                    const deduped = _.uniqWith(data, _.isEqual);

                    if (dataSz !== _.size(deduped)) {
                        return model.updateOne({_id: item._id}, {$set: {[name]: deduped}})
                            .then(() => cnt++);
                    }

                    return Promise.resolve();
                }), Promise.resolve())
                    .then(() => log(`Deduplication of ${title} finished: ${cnt}.`));
            }

            return Promise.resolve();
        });
}

exports.up = function up(done) {
    const clustersModel = this('Cluster');
    const cachesModel = this('Cache');
    const domainsModel = this('DomainModel');
    const igfsModel = this('Igfs');

    process.on('unhandledRejection', function(reason, p) {
        console.log('Unhandled rejection at:', p, 'reason:', reason);
    });

    Promise.resolve()
        .then(() => deduplicate('Cluster caches', clustersModel, 'caches'))
        .then(() => deduplicate('Cluster IGFS', clustersModel, 'igfss'))
        .then(() => deduplicate('Cache clusters', cachesModel, 'clusters'))
        .then(() => deduplicate('Cache domains', cachesModel, 'domains'))
        .then(() => deduplicate('IGFS clusters', igfsModel, 'clusters'))
        .then(() => deduplicate('Domain model caches', domainsModel, 'caches'))
        .then(() => migrateCaches(clustersModel, cachesModel, domainsModel))
        .then(() => migrateIgfss(clustersModel, igfsModel))
        .then(() => migrateDomains(clustersModel, cachesModel, domainsModel))
        .then(() => log(`Duplicates counter: ${dup}`))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    log('Model migration can not be reverted');

    done();
};
