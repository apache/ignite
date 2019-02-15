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

'use strict';

// Fire me up!

module.exports = {
    implements: 'services/spaces',
    inject: ['mongo', 'errors']
};

/**
 * @param mongo
 * @param errors
 * @returns {SpacesService}
 */
module.exports.factory = (mongo, errors) => {
    class SpacesService {
        /**
         * Query for user spaces.
         *
         * @param {mongo.ObjectId|String} userId User ID.
         * @param {Boolean} demo Is need use demo space.
         * @returns {Promise}
         */
        static spaces(userId, demo) {
            return mongo.Space.find({owner: userId, demo: !!demo}).lean().exec()
                .then((spaces) => {
                    if (!spaces.length)
                        throw new errors.MissingResourceException('Failed to find space');

                    return spaces;
                });
        }

        /**
         * Extract IDs from user spaces.
         *
         * @param {mongo.ObjectId|String} userId User ID.
         * @param {Boolean} demo Is need use demo space.
         * @returns {Promise}
         */
        static spaceIds(userId, demo) {
            return this.spaces(userId, demo)
                .then((spaces) => spaces.map((space) => space._id.toString()));
        }

        /**
         * Create demo space for user
         * @param userId - user id
         * @returns {Promise<mongo.Space>} that resolves created demo space for user
         */
        static createDemoSpace(userId) {
            return new mongo.Space({name: 'Demo space', owner: userId, demo: true}).save();
        }

        /**
         * Clean up spaces.
         *
         * @param {mongo.ObjectId|String} spaceIds - The space ids for clean up.
         * @returns {Promise.<>}
         */
        static cleanUp(spaceIds) {
            return Promise.all([
                mongo.Cluster.remove({space: {$in: spaceIds}}).exec(),
                mongo.Cache.remove({space: {$in: spaceIds}}).exec(),
                mongo.DomainModel.remove({space: {$in: spaceIds}}).exec(),
                mongo.Igfs.remove({space: {$in: spaceIds}}).exec()
            ]);
        }
    }

    return SpacesService;
};

