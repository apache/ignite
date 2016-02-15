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
    return new Promise((resolve) => {
        const router = express.Router();

        /**
         * Get notebooks names accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (errSpace)
                    return res.status(500).send(errSpace.message);

                const space_ids = spaces.map((value) => value._id);

                // Get all metadata for spaces.
                mongo.Notebook.find({space: {$in: space_ids}}).select('_id name').sort('name').exec((errNotebook, notebooks) => {
                    if (errNotebook)
                        return res.status(500).send(errNotebook.message);

                    res.json(notebooks);
                });
            });
        });

        /**
         * Get notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/get', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (errSpace)
                    return res.status(500).send(errSpace.message);

                const space_ids = spaces.map((value) => value._id);

                // Get all metadata for spaces.
                mongo.Notebook.findOne({space: {$in: space_ids}, _id: req.body.noteId}).exec((errNotebook, notebook) => {
                    if (errNotebook)
                        return res.status(500).send(errNotebook.message);

                    res.json(notebook);
                });
            });
        });

        /**
         * Save notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/save', (req, res) => {
            const note = req.body;
            const noteId = note._id;

            if (noteId) {
                mongo.Notebook.update({_id: noteId}, note, {upsert: true}, (err) => {
                    if (err)
                        return res.status(500).send(err.message);

                    res.send(noteId);
                });
            }
            else {
                mongo.Notebook.findOne({space: note.space, name: note.name}, (errNotebookFind, notebookFoud) => {
                    if (errNotebookFind)
                        return res.status(500).send(errNotebookFind.message);

                    if (notebookFoud)
                        return res.status(500).send('Notebook with name: "' + notebookFoud.name + '" already exist.');

                    (new mongo.Notebook(req.body)).save((errNotebook, noteNew) => {
                        if (errNotebook)
                            return res.status(500).send(errNotebook.message);

                        res.send(noteNew._id);
                    });
                });
            }
        });

        /**
         * Remove notebook by ._id.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/remove', (req, res) => {
            mongo.Notebook.remove(req.body, (err) => {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            });
        });

        /**
         * Create new notebook for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/new', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.findOne({owner: user_id}, (errSpace, space) => {
                if (errSpace)
                    return res.status(500).send(errSpace.message);

                (new mongo.Notebook({space: space.id, name: req.body.name})).save((errNotebook, note) => {
                    if (errNotebook)
                        return res.status(500).send(errNotebook.message);

                    return res.send(note._id);
                });
            });
        });

        resolve(router);
    });
};
