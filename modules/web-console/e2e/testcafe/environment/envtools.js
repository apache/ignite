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

const MongoClient = require('mongodb').MongoClient;
const objectid = require('objectid');
const { spawn } = require('child_process');
const url = require('url');

const argv = require('minimist')(process.argv.slice(2));
const start = argv._.includes('start');
const stop = argv._.includes('stop');

const mongoUrl = process.env.DB_URL || 'mongodb://localhost/console-e2e';

const insertTestUser = ({userId = '000000000000000000000001', token = 'ppw4tPI3JUOGHva8CODO'} = {}) => {
    return new Promise((res, rej) => {
        MongoClient
            .connect(mongoUrl, function(err, db) {
                if (err) {
                    rej();
                    throw err;
                }

                // Add test user.
                const user = {
                    _id: objectid(userId),
                    salt: 'ca8b49c2eacd498a0973de30c0873c166ed99fa0605981726aedcc85bee17832',
                    hash: 'c052c87e454cd0875332719e1ce085ccd92bedb73c8f939ba45d387f724da97128280643ad4f841d929d48de802f48f4a27b909d2dc806d957d38a1a4049468ce817490038f00ac1416aaf9f8f5a5c476730b46ea22d678421cd269869d4ba9d194f73906e5d5a4fec5229459e20ebda997fb95298067126f6c15346d886d44b67def03bf3ffe484b2e4fa449985de33a0c12e4e1da4c7d71fe7af5d138433f703d8c7eeebbb3d57f1a89659010a1f1d3cd4fbc524abab07860daabb08f08a28b8bfc64ecde2ea3c103030d0d54fc24d9c02f92ee6b3aa1bcd5c70113ab9a8045faea7dd2dc59ec4f9f69fcf634232721e9fb44012f0e8c8fdf7c6bf642db6867ef8e7877123e1bc78af7604fee2e34ad0191f8b97613ea458e0fca024226b7055e08a4bdb256fabf0a203a1e5b6a6c298fb0c60308569cefba779ce1e41fb971e5d1745959caf524ab0bedafce67157922f9c505cea033f6ed28204791470d9d08d31ce7e8003df8a3a05282d4d60bfe6e2f7de06f4b18377dac0fe764ed683c9b2553e75f8280c748aa166fef6f89190b1c6d369ab86422032171e6f9686de42ac65708e63bf018a043601d85bc5c820c7ad1d51ded32e59cdaa629a3f7ae325bbc931f9f21d90c9204effdbd53721a60c8b180dd8c236133e287a47ccc9e5072eb6593771e435e4d5196d50d6ddb32c226651c6503387895c5ad025f69fd3',
                    password: 'a',
                    email: 'a@a',
                    firstName: 'John',
                    lastName: 'Doe',
                    company: 'TestCompany',
                    country: 'Canada',
                    industry: 'Banking',
                    admin: true,
                    token,
                    attempts: 0,
                    lastLogin: '2018-01-28T10:41:07.463Z',
                    resetPasswordToken: '892rnLbEnVp1FP75Jgpi'
                };
                db.collection('accounts').insert(user);

                // Add test spaces.

                const spaces = [
                    {
                        _id: objectid('000000000000000000000001'),
                        name: 'Personal space',
                        owner: objectid(userId),
                        demo: false
                    },
                    {
                        _id: objectid('000000000000000000000002'),
                        name: 'Demo space',
                        owner: objectid(userId),
                        demo: true
                    }
                ];
                db.collection('spaces').insertMany(spaces);

                db.close();
                res();

            });
    });
};

const dropTestDB = () => {
    return new Promise((resolve, reject) => {
        MongoClient.connect(mongoUrl, async(err, db) => {
            if (err)
                return reject(err);

            db.dropDatabase((err) => {
                if (err)
                    return reject(err);

                resolve();
            });
        });
    });
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

const startEnv = (webConsoleRootDirectoryPath = '../../') => {
    return new Promise(async(resolve) => {
        const command = `${process.platform === 'win32' ? 'npm.cmd' : 'npm'} start`;

        let port = 9001;

        if (process.env.APP_URL)
            port = parseInt(url.parse(process.env.APP_URL).port, 10) || 80;

        const backendInstanceLaunch = exec(command, 'Start listening', `${webConsoleRootDirectoryPath}backend`, {server_port: 3001, mongodb_url: mongoUrl}); // Todo: refactor cwd for backend when it's linked
        const frontendInstanceLaunch = exec(command, 'Compiled successfully', `${webConsoleRootDirectoryPath}frontend`, {BACKEND_PORT: 3001, PORT: port});

        console.log('Building backend in progress...');
        await backendInstanceLaunch;
        console.log('Building backend done!');

        console.log('Building frontend in progress...');
        await frontendInstanceLaunch;
        console.log('Building frontend done!');

        resolve();
    });
};

if (start) {
    startEnv();

    process.on('SIGINT', async() => {
        await dropTestDB();

        process.exit(0);
    });
}

if (stop) {
    dropTestDB();

    console.log('Cleaning done...');
}


/**
 * @param {string} targetUrl
 * @returns {string}
 */
const resolveUrl = (targetUrl) => {
    return url.resolve(process.env.APP_URL || 'http://localhost:9001', targetUrl);
};

module.exports = { startEnv, insertTestUser, dropTestDB, resolveUrl };
