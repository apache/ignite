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
    implements: 'services/clusters',
    inject: ['require(lodash)', 'mongo', 'services/spaces', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param {SpacesService} spacesService
 * @param errors
 * @returns {ClustersService}
 */
module.exports.factory = (_, mongo, spacesService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    /**
     * Update existing cluster
     * @param {Object} cluster - The cluster for updating
     * @returns {Promise.<mongo.ObjectId>} that resolves cluster id
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
            });
    };

    /**
     * Create new cluster.
     * @param {Object} cluster - The cluster for creation.
     * @returns {Promise.<mongo.ObjectId>} that resolves cluster id.
     */
    const create = (cluster) => {
        return mongo.Cluster.create(cluster)
            .then((savedCluster) =>
                mongo.Cache.update({_id: {$in: savedCluster.caches}}, {$addToSet: {clusters: savedCluster._id}}, {multi: true}).exec()
                    .then(() => mongo.Igfs.update({_id: {$in: savedCluster.igfss}}, {$addToSet: {clusters: savedCluster._id}}, {multi: true}).exec())
                    .then(() => savedCluster)
            )
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cluster with name: "' + cluster.name + '" already exist.');
            });
    };

    /**
     * Remove all caches by space ids.
     * @param {Number[]} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cache.update({space: {$in: spaceIds}}, {clusters: []}, {multi: true}).exec()
            .then(() => mongo.Igfs.update({space: {$in: spaceIds}}, {clusters: []}, {multi: true}).exec())
            .then(() => mongo.Cluster.remove({space: {$in: spaceIds}}).exec());
    };

    class ClustersService {
        /**
         * Create or update cluster.
         * @param {Object} cluster - The cluster
         * @returns {Promise.<mongo.ObjectId>} that resolves cluster id of merge operation.
         */
        static merge(cluster) {
            if (cluster._id)
                return update(cluster);

            return create(cluster);
        }

        /**
         * Get clusters and linked objects by space.
         * @param {mongo.ObjectId|String} spaceIds - The spaces id that own cluster.
         * @returns {Promise.<[mongo.Cache[], mongo.Cluster[], mongo.DomainModel[], mongo.Space[]]>} - contains requested caches and array of linked objects: clusters, domains, spaces.
         */
        static listBySpaces(spaceIds) {
            return mongo.Cluster.find({space: {$in: spaceIds}}).sort('name').lean().exec();
        }

        /**
         * Remove cluster.
         * @param {mongo.ObjectId|String} clusterId - The cluster id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(clusterId) {
            if (_.isNil(clusterId))
                return Promise.reject(new errors.IllegalArgumentException('Cluster id can not be undefined or null'));

            return mongo.Cache.update({clusters: {$in: [clusterId]}}, {$pull: {clusters: clusterId}}, {multi: true}).exec()
                .then(() => mongo.Igfs.update({clusters: {$in: [clusterId]}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
                .then(() => mongo.Cluster.remove({_id: clusterId}).exec())
                .then(convertRemoveStatus);
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
