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

const _ = require('lodash');

// Fire me up!

module.exports = {
    implements: 'services/clusters',
    inject: ['mongo', 'services/spaces', 'services/caches', 'services/domains', 'services/igfss', 'errors']
};

/**
 * @param mongo
 * @param {SpacesService} spacesService
 * @param {CachesService} cachesService
 * @param {DomainsService} modelsService
 * @param {IgfssService} igfssService
 * @param errors
 * @returns {ClustersService}
 */
module.exports.factory = (mongo, spacesService, cachesService, modelsService, igfssService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     *
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    /**
     * Update existing cluster.
     *
     * @param {Object} cluster - The cluster for updating.
     * @returns {Promise.<mongo.ObjectId>} that resolves cluster id.
     */
    const update = (cluster) => {
        const clusterId = cluster._id;

        return mongo.Cluster.update({_id: clusterId}, cluster, {upsert: true}).exec()
            .then(() => mongo.Cache.update({_id: {$in: cluster.caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}).exec())
            .then(() => mongo.Cache.update({_id: {$nin: cluster.caches}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
            .then(() => mongo.Igfs.update({_id: {$in: cluster.igfss}}, {$addToSet: {clusters: clusterId}}, {multi: true}).exec())
            .then(() => mongo.Igfs.update({_id: {$nin: cluster.igfss}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
            .then(() => cluster)
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cluster with name: "' + cluster.name + '" already exist.');
                else
                    throw err;
            });
    };

    /**
     * Create new cluster.
     *
     * @param {Object} cluster - The cluster for creation.
     * @returns {Promise.<mongo.ObjectId>} that resolves cluster id.
     */
    const create = (cluster) => {
        return mongo.Cluster.create(cluster)
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException(`Cluster with name: "${cluster.name}" already exist.`);
                else
                    throw err;
            })
            .then((savedCluster) =>
                mongo.Cache.update({_id: {$in: savedCluster.caches}}, {$addToSet: {clusters: savedCluster._id}}, {multi: true}).exec()
                    .then(() => mongo.Igfs.update({_id: {$in: savedCluster.igfss}}, {$addToSet: {clusters: savedCluster._id}}, {multi: true}).exec())
                    .then(() => savedCluster)
            );
    };

    /**
     * Remove all caches by space ids.
     *
     * @param {Number[]} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return Promise.all([
            mongo.DomainModel.remove({space: {$in: spaceIds}}).exec(),
            mongo.Cache.remove({space: {$in: spaceIds}}).exec(),
            mongo.Igfs.remove({space: {$in: spaceIds}}).exec()
        ])
            .then(() => mongo.Cluster.remove({space: {$in: spaceIds}}).exec());
    };

    class ClustersService {
        static shortList(userId, demo) {
            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => mongo.Cluster.find({space: {$in: spaceIds}}).select('name discovery.kind caches models igfss').lean().exec())
                .then((clusters) => _.map(clusters, (cluster) => ({
                    _id: cluster._id,
                    name: cluster.name,
                    discovery: cluster.discovery.kind,
                    cachesCount: _.size(cluster.caches),
                    modelsCount: _.size(cluster.models),
                    igfsCount: _.size(cluster.igfss)
                })));
        }

        static get(userId, demo, _id) {
            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => mongo.Cluster.findOne({space: {$in: spaceIds}, _id}).lean().exec());
        }

        static normalize(spaceId, cluster, ...models) {
            cluster.space = spaceId;

            _.forEach(models, (model) => {
                _.forEach(model, (item) => {
                    item.space = spaceId;
                    item.clusters = [cluster._id];
                });
            });
        }

        static removedInCluster(oldCluster, newCluster, field) {
            return _.difference(_.invokeMap(_.get(oldCluster, field), 'toString'), _.get(newCluster, field));
        }

        static upsertBasic(userId, demo, {cluster, caches}) {
            if (_.isNil(cluster._id))
                return Promise.reject(new errors.IllegalArgumentException('Cluster id can not be undefined or null'));

            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => {
                    this.normalize(_.head(spaceIds), cluster, caches);

                    const query = _.pick(cluster, ['space', '_id']);
                    const basicCluster = _.pick(cluster, [
                        'space',
                        '_id',
                        'name',
                        'discovery',
                        'caches',
                        'memoryConfiguration.memoryPolicies',
                        'dataStorageConfiguration.defaultDataRegionConfiguration.maxSize'
                    ]);

                    return mongo.Cluster.findOneAndUpdate(query, {$set: basicCluster}, {projection: 'caches', upsert: true}).lean().exec()
                        .catch((err) => {
                            if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                                throw new errors.DuplicateKeyException(`Cluster with name: "${cluster.name}" already exist.`);

                            throw err;
                        })
                        .then((oldCluster) => {
                            if (oldCluster) {
                                const ids = this.removedInCluster(oldCluster, cluster, 'caches');

                                return cachesService.remove(ids);
                            }

                            cluster.caches = _.map(caches, '_id');

                            return mongo.Cluster.update(query, {$set: cluster, new: true}, {upsert: true}).exec();
                        });
                })
                .then(() => _.map(caches, cachesService.upsertBasic))
                .then(() => ({rowsAffected: 1}));
        }

        static upsert(userId, demo, {cluster, caches, models, igfss}) {
            if (_.isNil(cluster._id))
                return Promise.reject(new errors.IllegalArgumentException('Cluster id can not be undefined or null'));

            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => {
                    this.normalize(_.head(spaceIds), cluster, caches, models, igfss);

                    const query = _.pick(cluster, ['space', '_id']);

                    return mongo.Cluster.findOneAndUpdate(query, {$set: cluster}, {projection: {models: 1, caches: 1, igfss: 1}, upsert: true}).lean().exec()
                        .catch((err) => {
                            if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                                throw new errors.DuplicateKeyException(`Cluster with name: "${cluster.name}" already exist.`);

                            throw err;
                        })
                        .then((oldCluster) => {
                            const modelIds = this.removedInCluster(oldCluster, cluster, 'models');
                            const cacheIds = this.removedInCluster(oldCluster, cluster, 'caches');
                            const igfsIds = this.removedInCluster(oldCluster, cluster, 'igfss');

                            return Promise.all([modelsService.remove(modelIds), cachesService.remove(cacheIds), igfssService.remove(igfsIds)]);
                        });
                })
                .then(() => Promise.all(_.concat(_.map(models, modelsService.upsert), _.map(caches, cachesService.upsert), _.map(igfss, igfssService.upsert))))
                .then(() => ({rowsAffected: 1}));
        }

        /**
         * Create or update cluster.
         *
         * @param {Object} cluster - The cluster.
         * @returns {Promise.<mongo.ObjectId>} that resolves cluster id of merge operation.
         */
        static merge(cluster) {
            if (cluster._id)
                return update(cluster);

            return create(cluster);
        }

        /**
         * Get clusters and linked objects by space.
         *
         * @param {mongo.ObjectId|String} spaceIds The spaces ids that own clusters.
         * @returns {Promise.<Array<mongo.Cluster>>} Requested clusters.
         */
        static listBySpaces(spaceIds) {
            return mongo.Cluster.find({space: {$in: spaceIds}}).sort('name').lean().exec();
        }

        /**
         * Remove clusters.
         *
         * @param {Array.<String>|String} ids - The cluster ids for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(ids) {
            if (_.isNil(ids))
                return Promise.reject(new errors.IllegalArgumentException('Cluster id can not be undefined or null'));

            if (_.isEmpty(ids))
                return Promise.resolve({rowsAffected: 0});

            ids = _.castArray(ids);

            return Promise.all(_.map(ids, (id) => {
                return mongo.Cluster.findByIdAndRemove(id).exec()
                    .then((cluster) => {
                        if (_.isNil(cluster))
                            return 0;

                        return Promise.all([
                            mongo.DomainModel.remove({_id: {$in: cluster.models}}).exec(),
                            mongo.Cache.remove({_id: {$in: cluster.caches}}).exec(),
                            mongo.Igfs.remove({_id: {$in: cluster.igfss}}).exec()
                        ])
                            .then(() => 1);
                    });
            }))
                .then((res) => ({rowsAffected: _.sum(res)}));
        }

        /**
         * Remove all clusters by user.
         * @param {mongo.ObjectId|String} userId - The user id that own cluster.
         * @param {Boolean} demo - The flag indicates that need lookup in demo space.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static removeAll(userId, demo) {
            return spacesService.spaceIds(userId, demo)
                .then(removeAllBySpaces)
                .then(convertRemoveStatus);
        }
    }

    return ClustersService;
};
