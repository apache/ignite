/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

// Fire me up!

const _ = require('lodash');

module.exports = {
    implements: 'middlewares:api'
};

module.exports.factory = () => {
    return (req, res, next) => {
        // Set headers to avoid API caching in browser (esp. IE)
        res.header('Cache-Control', 'must-revalidate');
        res.header('Expires', '-1');
        res.header('Last-Modified', new Date().toUTCString());

        res.api = {
            error(err) {
                if (_.includes(['MongoError', 'MongooseError'], err.name))
                    return res.status(500).send(err.message);

                if (_.isObject(err.data))
                    return res.status(err.httpCode || err.code || 500).json(err.data);

                res.status(err.httpCode || err.code || 500).send(err.message);
            },

            ok(data) {
                if (_.isNil(data))
                    return res.sendStatus(404);

                res.status(200).json(data);
            },

            done() {
                res.sendStatus(200);
            }
        };

        next();
    };
};
