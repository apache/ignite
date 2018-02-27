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
                .then((spaces) => spaces.map((space) => space._id));
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

