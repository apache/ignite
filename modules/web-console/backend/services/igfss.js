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
    implements: 'services/igfss',
    inject: ['require(lodash)', 'mongo', 'services/spaces', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param {SpacesService} spacesService
 * @param errors
 * @returns {IgfssService}
 */
module.exports.factory = (_, mongo, spacesService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    /**
     * Update existing IGFS
     * @param {Object} igfs - The IGFS for updating
     * @returns {Promise.<mongo.ObjectId>} that resolves IGFS id
     */
    const update = (igfs) => {
        const igfsId = igfs._id;

        return mongo.Igfs.update({_id: igfsId}, igfs, {upsert: true}).exec()
            .then(() => mongo.Cluster.update({_id: {$in: igfs.clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}).exec())
            .then(() => mongo.Cluster.update({_id: {$nin: igfs.clusters}}, {$pull: {igfss: igfsId}}, {multi: true}).exec())
            .then(() => igfs)
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('IGFS with name: "' + igfs.name + '" already exist.');
            });
    };

    /**
     * Create new IGFS.
     * @param {Object} igfs - The IGFS for creation.
     * @returns {Promise.<mongo.ObjectId>} that resolves IGFS id.
     */
    const create = (igfs) => {
        return mongo.Igfs.create(igfs)
            .then((savedIgfs) =>
                mongo.Cluster.update({_id: {$in: savedIgfs.clusters}}, {$addToSet: {igfss: savedIgfs._id}}, {multi: true}).exec()
                    .then(() => savedIgfs)
            )
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('IGFS with name: "' + igfs.name + '" already exist.');
            });
    };

    /**
     * Remove all IGFSs by space ids.
     * @param {Number[]} spaceIds - The space ids for IGFS deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cluster.update({space: {$in: spaceIds}}, {igfss: []}, {multi: true}).exec()
            .then(() => mongo.Igfs.remove({space: {$in: spaceIds}}).exec());
    };

    class IgfssService {
        /**
         * Create or update IGFS.
         * @param {Object} igfs - The IGFS
         * @returns {Promise.<mongo.ObjectId>} that resolves IGFS id of merge operation.
         */
        static merge(igfs) {
            if (igfs._id)
                return update(igfs);

            return create(igfs);
        }

        /**
         * Get IGFS by spaces.
         * @param {mongo.ObjectId|String} spacesIds - The spaces ids that own IGFSs.
         * @returns {Promise.<mongo.IGFS[]>} - contains requested IGFSs.
         */
        static listBySpaces(spacesIds) {
            return mongo.Igfs.find({space: {$in: spacesIds}}).sort('name').lean().exec();
        }

        /**
         * Remove IGFS.
         * @param {mongo.ObjectId|String} igfsId - The IGFS id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(igfsId) {
            if (_.isNil(igfsId))
                return Promise.reject(new errors.IllegalArgumentException('IGFS id can not be undefined or null'));

            return mongo.Cluster.update({igfss: {$in: [igfsId]}}, {$pull: {igfss: igfsId}}, {multi: true}).exec()
                .then(() => mongo.Igfs.remove({_id: igfsId}).exec())
                .then(convertRemoveStatus);
        }

        /**
         * Remove all IGFSes by user.
         * @param {mongo.ObjectId|String} userId - The user id that own IGFS.
         * @param {Boolean} demo - The flag indicates that need lookup in demo space.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static removeAll(userId, demo) {
            return spacesService.spaceIds(userId, demo)
                .then(removeAllBySpaces)
                .then(convertRemoveStatus);
        }
    }

    return IgfssService;
};
