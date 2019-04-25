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

const fs = require('fs');
const nconf = require('nconf');

// Fire me up!

/**
 * Module with server-side configuration.
 */
module.exports = {
    implements: 'nconf',
    factory() {
        nconf.env({separator: '_'}).argv();

        const dfltFile = 'config/settings.json';
        const customFile = nconf.get('settings') || dfltFile;

        try {
            fs.accessSync(customFile, fs.F_OK);

            nconf.file({file: customFile});
        }
        catch (ignored) {
            try {
                fs.accessSync(dfltFile, fs.F_OK);

                nconf.file({file: dfltFile});
            }
            catch (ignored2) {
                // No-op.
            }
        }

        return nconf;
    }
};
