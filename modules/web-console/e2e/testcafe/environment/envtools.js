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

const ClientFunction = require('testcafe').ClientFunction;
const request = require('request-promise-native');
const { spawn } = require('child_process');
const url = require('url');

const testUser = {
    password: 'a',
    email: 'a@example.com',
    firstName: 'John',
    lastName: 'Doe',
    company: 'TestCompany',
    country: 'Canada',
    industry: 'Banking'
};

const insertTestUser = () => {
    return request({
        method: 'PUT',
        uri: resolveUrl('/api/v1/test/admins'),
        body: testUser,
        json: true
    })
        .catch((err) => {throw err.message;});
};

const dropTestDB = () => {
    return request({
        method: 'DELETE',
        uri: resolveUrl('/api/v1/test/users/@example.com')
    })
        .catch((err) => {throw err.message;});
};


/**
 * Spawns a new process using the given command.
 * @param command {String} The command to run.
 * @param onResolveString {String} Await string in output.
 * @param cwd {String} Current working directory of the child process.
 * @param env {Object} Environment key-value pairs.
 * @return {Promise<ChildProcess>}
 */
const exec = (command, onResolveString, cwd, env) => {
    return new Promise((resolve) => {
        env = Object.assign({}, process.env, env, { FORCE_COLOR: true });

        const [cmd, ...args] = command.split(' ');

        const detached = process.platform !== 'win32';

        const child = spawn(cmd, args, {cwd, env, detached});

        if (detached) {
            // do something when app is closing
            process.on('exit', () => process.kill(-child.pid));

            // catches ctrl+c event
            process.on('SIGINT', () => process.kill(-child.pid));

            // catches "kill pid" (for example: nodemon restart)
            process.on('SIGUSR1', () => process.kill(-child.pid));
            process.on('SIGUSR2', () => process.kill(-child.pid));

            // catches uncaught exceptions
            process.on('uncaughtException', () => process.kill(-child.pid));
        }

        // Pipe error messages to stdout.
        child.stderr.on('data', (data) => {
            process.stdout.write(data.toString());
        });

        child.stdout.on('data', (data) => {
            process.stdout.write(data.toString());

            if (data.includes(onResolveString))
                resolve(child);
        });
    });
};

/**
 * @param {string} targetUrl
 * @param {string?} host
 * @returns {string}
 */
const resolveUrl = (targetUrl, host = 'http://localhost:9001') => {
    return url.resolve(process.env.APP_URL || host, targetUrl);
};

const enableDemoMode = ClientFunction(() => {
    window.sessionStorage.demoMode = 'true';
    window.location.reload();
});

module.exports = { insertTestUser, dropTestDB, resolveUrl, enableDemoMode };
