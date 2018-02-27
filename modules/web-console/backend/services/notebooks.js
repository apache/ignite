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
    implements: 'services/notebooks',
    inject: ['require(lodash)', 'mongo', 'services/spaces', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param {SpacesService} spacesService
 * @param errors
 * @returns {NotebooksService}
 */
module.exports.factory = (_, mongo, spacesService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     *
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    /**
     * Update existing notebook.
     *
     * @param {Object} notebook - The notebook for updating
     * @returns {Promise.<mongo.ObjectId>} that resolves cache id
     */
    const update = (notebook) => {
        return mongo.Notebook.findOneAndUpdate({_id: notebook._id}, notebook, {new: true, upsert: true}).exec()
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Notebook with name: "' + notebook.name + '" already exist.');
            });
    };

    /**
     * Create new notebook.
     *
     * @param {Object} notebook - The notebook for creation.
     * @returns {Promise.<mongo.ObjectId>} that resolves cache id.
     */
    const create = (notebook) => {
        return mongo.Notebook.create(notebook)
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Notebook with name: "' + notebook.name + '" already exist.');
            });
    };

    class NotebooksService {
        /**
         * Create or update Notebook.
         *
         * @param {Object} notebook - The Notebook
         * @returns {Promise.<mongo.ObjectId>} that resolves Notebook id of merge operation.
         */
        static merge(notebook) {
            if (notebook._id)
                return update(notebook);

            return create(notebook);
        }

        /**
         * Get notebooks by spaces.
         *
         * @param {mongo.ObjectId|String} spaceIds - The spaces ids that own caches.
         * @returns {Promise.<mongo.Notebook[]>} - contains requested caches.
         */
        static listBySpaces(spaceIds) {
            return mongo.Notebook.find({space: {$in: spaceIds}}).sort('name').lean().exec();
        }

        /**
         * Remove notebook.
         *
         * @param {mongo.ObjectId|String} notebookId - The Notebook id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(notebookId) {
            if (_.isNil(notebookId))
                return Promise.reject(new errors.IllegalArgumentException('Notebook id can not be undefined or null'));

            return mongo.Notebook.remove({_id: notebookId}).exec()
                .then(convertRemoveStatus);
        }
    }

    return NotebooksService;
};
