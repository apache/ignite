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

/**
 * Module with server-side configuration.
 */
module.exports = {
    implements: 'nconf',
    inject: ['require(nconf)', 'require(fs)']
};

module.exports.factory = function(nconf, fs) {
    const default_config = './config/settings.json';
    const file = process.env.SETTINGS || default_config;

    nconf.env({separator: '_'});

    try {
        fs.accessSync(file, fs.F_OK);

        nconf.file({file});
    } catch (ignore) {
        nconf.file({file: default_config});
    }

    if (process.env.CONFIG_PATH && fs.existsSync(process.env.CONFIG_PATH))
        nconf.file({file: process.env.CONFIG_PATH});

    return nconf;
};
