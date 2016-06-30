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

function sendServerError(err) {
    err.httpCode = 500;

    this.api.error(err);
}

function sendError(err) {
    // TODO: removed code from error
    this.status(err.httpCode || err.code || 500).send(err.message);
}

function sendOk(data) {
    this.status(200).json(data);
}

module.exports = {
    implements: 'middlewares/api',
    factory: () => {
        return (req, res, next) => {
            res.api = {
                error: sendError.bind(res),
                ok: sendOk.bind(res),
                serverError: sendServerError.bind(res)
            };

            next();
        };
    }
};
