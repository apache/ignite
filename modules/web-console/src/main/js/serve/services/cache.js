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
    implements: 'services/cacheService',
    inject: ['require(lodash)', 'require(express)', 'mongo', 'errors']
};

module.exports.factory = function (_, express, mongo, errors) {

    const save = (cache) => {
        return mongo.Cache.findOne({space: cache.space, name: cache.name}).exec()
            .then((existingCache) => {
                const cacheId = cache._id;

                if (existingCache && cacheId !== existingCache._id.toString())
                    throw new errors.DupleError('Cache with name: "' + existingCache.name + '" already exist.');

                if (cacheId) {
                    return mongo.Cache.update({_id: cacheId}, cache, {upsert: true}).exec()
                        .then(() => mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                        .then(() => mongo.Cluster.update({_id: {$nin: cache.clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                        .then(() => mongo.DomainModel.update({_id: {$in: cache.domains}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                        .then(() => mongo.DomainModel.update({_id: {$nin: cache.domains}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                        .then(() => cacheId);
                }

                // TODO: Replace to mongo.Cache.create()
                return (new mongo.Cache(cache)).save() 
                    .then((cache) =>
                        mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cache._id}}, {multi: true}).exec()
                            .then(() => mongo.DomainModel.update({_id: {$in: cache.domains}}, {$addToSet: {caches: cache._id}}, {multi: true}).exec())
                            .then(() => cache._id)
                    );
            });
        };
    
    return {
        save
    };
};
