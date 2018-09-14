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

const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

const ENDPOINT1 = 'localhost:10800';
const ENDPOINT2 = 'localhost:10801';
const ENDPOINT3 = 'localhost:10802';

// This example demonstrates failover behavior of the client
// - configures the client to connect to a set of nodes
// - connects to a node
// - if connection is broken, the client automatically tries to reconnect to another node
// - if no specified nodes are available, stops the client
async function connectClient() {
    const igniteClient = new IgniteClient(onStateChanged);
    igniteClient.setDebug(true);
    try {
        const igniteClientConfiguration = new IgniteClientConfiguration(
            ENDPOINT1, ENDPOINT2, ENDPOINT3);
        // connect to Ignite a node
        await igniteClient.connect(igniteClientConfiguration);
    }
    catch (err) {
        console.log(err.message);
    }
}

function onStateChanged(state, reason) {
    if (state === IgniteClient.STATE.CONNECTED) {
        console.log('Client is started');
    }
    else if (state === IgniteClient.STATE.CONNECTING) {
        console.log('Client is connecting');
    }
    else if (state === IgniteClient.STATE.DISCONNECTED) {
        console.log('Client is stopped');
        if (reason) {
            console.log(reason);
        }
    }
}

connectClient();