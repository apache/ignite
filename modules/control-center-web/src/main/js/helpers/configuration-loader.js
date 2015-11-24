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

var config = require('nconf');

config.file({'file': 'config/default.json'});

/**
 * Normalize a port into a number, string, or false.
 */
config.normalizePort = function (val) {
    var port = parseInt(val, 10);

    if (isNaN(port)) {
        // named pipe
        return val;
    }

    if (port >= 0) {
        // port number
        return port;
    }

    return false;
};

config.findIgniteModules = function () {
    var fs = require('fs');
    var path = require('path');

    var igniteModules = process.env.IGNITE_MODULES || path.resolve(__dirname, 'ignite_modules');

    function _find (root, filter, files, prefix) {
        prefix = prefix || '';
        files = files || [];

        var dir = path.join(root, prefix);

        if (!fs.existsSync(dir))
            return files;

        if (fs.statSync(dir).isDirectory())
            fs.readdirSync(dir)
                .filter(function (name) { return name[0] !== '.' })
                .forEach(function (name) {
                    _find(root, filter, files, path.join(prefix, name))
                });
        else
            files.push(path.join(igniteModules, prefix));

        return files;
    }

    return _find(igniteModules);
};

module.exports = config;
