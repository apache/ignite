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
//tag::example-block[]
const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

async function connectClient() {
    const igniteClient = new IgniteClient(onStateChanged);
    try {
        //tag::auth[]
        const ENDPOINT = 'localhost:10800';
        const USER_NAME = 'ignite';
        const PASSWORD = 'ignite';

        const igniteClientConfiguration = new IgniteClientConfiguration(
            ENDPOINT).setUserName(USER_NAME).setPassword(PASSWORD);
        //end::auth[]
        // Connect to Ignite node
        await igniteClient.connect(igniteClientConfiguration);
    } catch (err) {
        console.log(err.message);
    }
}

function onStateChanged(state, reason) {
    if (state === IgniteClient.STATE.CONNECTED) {
        console.log('Client is started');
    } else if (state === IgniteClient.STATE.CONNECTING) {
        console.log('Client is connecting');
    } else if (state === IgniteClient.STATE.DISCONNECTED) {
        console.log('Client is stopped');
        if (reason) {
            console.log(reason);
        }
    }
}

connectClient();
//end::example-block[]
