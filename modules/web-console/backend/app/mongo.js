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

const _ = require('lodash');
const {MongodHelper} = require('mongodb-prebuilt');
const {MongoDBDownload} = require('mongodb-download');

// Fire me up!

/**
 * Module mongo schema.
 */
module.exports = {
    implements: 'mongo',
    inject: ['settings', 'mongoose', 'schemas']
};

const defineSchema = (mongoose, schemas) => {
    const result = { connection: mongoose.connection };

    result.ObjectId = mongoose.Types.ObjectId;

    result.errCodes = {
        DUPLICATE_KEY_ERROR: 11000,
        DUPLICATE_KEY_UPDATE_ERROR: 11001
    };

    // Define models.
    _.forEach(schemas, (schema, name) => {
        result[name] = mongoose.model(name, schema);
    });

    result.handleError = function(res, err) {
        // TODO IGNITE-843 Send error to admin
        res.status(err.code || 500).send(err.message);
    };

    return result;
};

module.exports.factory = function(settings, mongoose, schemas) {
    // Use native promises
    mongoose.Promise = global.Promise;

    console.log('Trying to connect to local MongoDB...');

    // Connect to mongoDB database.
    return mongoose.connect(settings.mongoUrl, {server: {poolSize: 4}})
        .then(() => defineSchema(mongoose, schemas))
        .catch((err) => {
            console.log('Failed to connect to local MongoDB, will try to download and start embedded MongoDB', err);

            const helper = new MongodHelper(['--port', '27017', '--dbpath', `${process.cwd()}/user_data`]);

            helper.mongoBin.mongoDBPrebuilt.mongoDBDownload = new MongoDBDownload({
                downloadDir: `${process.cwd()}/libs/mongodb`,
                version: '3.4.7'
            });

            let mongodRun;

            if (settings.packaged) {
                mongodRun = new Promise((resolve, reject) => {
                    helper.resolveLink = resolve;
                    helper.rejectLink = reject;

                    helper.mongoBin.runCommand()
                        .then(() => {
                            helper.mongoBin.childProcess.removeAllListeners('close');

                            helper.mongoBin.childProcess.stderr.on('data', (data) => helper.stderrHandler(data));
                            helper.mongoBin.childProcess.stdout.on('data', (data) => helper.stdoutHandler(data));
                            helper.mongoBin.childProcess.on('close', (code) => helper.closeHandler(code));
                        });
                });
            }
            else
                mongodRun = helper.run();

            return mongodRun
                .catch((err) => {
                    console.log('Failed to start embedded MongoDB', err);

                    return Promise.reject(err);
                })
                .then(() => {
                    console.log('Embedded MongoDB successfully started');

                    return mongoose.connect(settings.mongoUrl, {server: {poolSize: 4}})
                        .catch((err) => {
                            console.log('Failed to connect to embedded MongoDB', err);

                            return Promise.reject(err);
                        });
                })
                .then(() => defineSchema(mongoose, schemas))
                .then((mongo) => {
                    if (settings.packaged) {
                        return mongo.Account.count()
                            .then((count) => {
                                if (count === 0) {
                                    return Promise.all([
                                        mongo.Account.create({
                                            _id: '59fc0c25e145c32be0f83b33',
                                            salt: '7b4ccb9e375508a8f87c8f347083ce98cb8785d857dd18208f9a480e992a26bb',
                                            hash: '909d5ed6e0b0a656ef542e2e8e851e9eb00cfb77984e0a6b4597c335d1436a577b3b289601eb8d1f3646e488cd5ea2bbb3e97fcc131cd6a9571407a45b1817bf1af1dd0ccdd070f07733da19e636ff9787369c5f38f86075f78c60809fe4a52288a68ca38aae0ad2bd0cc77b4cae310abf260e9523d361fd9be60e823a7d8e73954ddb18091e668acd3f57baf9fa7db4267e198d829761997a4741734335589ab62793ceb089e8fffe6e5b0e86f332b33a3011ba44e6efd29736f31cbd2b2023e5173baf517f337eb7a4321ea2b67ec827cffa271d26d3f2def93b5efa3ae7e6e327e55feb121ee96b8ff5016527cc7d854a9b49b44c993387c1093705cb26b1802a2e4c1d34508fb93d051d7e5e2e6cc65b6048a999f94c369973b46b204295f0b2f23f8e30723f9e984ddb2c53dcbf0a77a6d0795d44c3ad97a4ae49d6767db9630e2ef76c2069da87088f1400b1292df9bd787122b2cfef1f26a884a298a0bab3d6e6b689381cf6389d2f019e6cd19e82c84048bacfdd1bee946f9d40dda040be426e583abf92529a1c4f032d5058a9799a77e6642312b8d231d79300d5d0d3f74d62797f9d192e8581698e9539812a539ef1b9fbf718f44dd549896ea9449f6ea744586222e5fc29dfcd5eb79e7646ad3d37868f5073833c554853dee6b067bf2bbfab44c011f2de98a8570292f8109b6bde11e3be51075a656c32b521b7',
                                            email: 'admin@admin',
                                            firstName: 'admin',
                                            lastName: 'admin',
                                            company: 'admin',
                                            country: 'United States',
                                            admin: true,
                                            token: 'ruQvlWff09zqoVYyh6WJ',
                                            attempts: 0,
                                            resetPasswordToken: 'O2GWgOkKkhqpDcxjYnSP'
                                        }),
                                        mongo.Space.create({
                                            _id: '59fc0c26e145c32be0f83b34',
                                            name: 'Personal space',
                                            owner: '59fc0c25e145c32be0f83b33',
                                            usedBy: [],
                                            demo: false
                                        })
                                    ]);
                                }
                            })
                            .then(() => mongo)
                            .catch(() => mongo);
                    }

                    return mongo;
                });
        });
};
