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
    implements: 'notebooks-routes',
    inject: ['require(express)', 'mongo']
};

module.exports.factory = function(express, mongo) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get notebooks names accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            mongo.spaces(req.currentUserId())
                .then((spaces) => mongo.Notebook.find({space: {$in: spaces.map((value) => value._id)}}).select('_id name').sort('name').lean().exec())
                .then((notebooks) => res.json(notebooks))
                .catch((err) => mongo.handleError(res, err));

        });

        /**
         * Get notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/get', (req, res) => {
            mongo.spaces(req.currentUserId())
                .then((spaces) => mongo.Notebook.findOne({space: {$in: spaces.map((value) => value._id)}, _id: req.body.noteId}).lean().exec())
                .then((notebook) => res.json(notebook))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Save notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/save', (req, res) => {
            const note = req.body;

            mongo.Notebook.findOne({space: note.space, name: note.name}).exec()
                .then((notebook) => {
                    const noteId = note._id;

                    if (notebook && noteId !== notebook._id.toString())
                        throw new Error('Notebook with name: "' + notebook.name + '" already exist.');

                    if (noteId) {
                        return mongo.Notebook.update({_id: noteId}, note, {upsert: true}).exec()
                            .then(() => res.send(noteId))
                            .catch((err) => mongo.handleError(res, err));
                    }

                    return (new mongo.Notebook(req.body)).save();
                })
                .then((notebook) => res.send(notebook._id))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove notebook by ._id.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/remove', (req, res) => {
            mongo.Notebook.remove(req.body).exec()
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Create new notebook for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/new', (req, res) => {
            mongo.spaceIds(req.currentUserId())
                .then((spaceIds) =>
                    mongo.Notebook.findOne({space: spaceIds[0], name: req.body.name})
                        .then((notebook) => {
                            if (notebook)
                                throw new Error('Notebook with name: "' + notebook.name + '" already exist.');

                            return spaceIds;
                        }))
                .then((spaceIds) => (new mongo.Notebook({space: spaceIds[0], name: req.body.name})).save())
                .then((notebook) => res.send(notebook._id))
                .catch((err) => mongo.handleError(res, err));
        });

        factoryResolve(router);
    });
};
