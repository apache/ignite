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

package org.apache.ignite.internal.ducktest.tests.thin_client_test;

import java.util.List;
import java.util.function.Consumer;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/** Start Thin Client making some operations for a given time. */
public class ThinClientContiniusRunner implements Runnable {
    /** Ignite Client configuration. */
    private final ClientConfiguration cfg;

    /** Time of connection. */
    private final List<Long> connectTime;

    /** Function to execute with IgniteClient. */
    private final Consumer<IgniteClient> func;

    /**
     * Start IgniteClient, save connect time and execute function
     *
     * @param cfg Configuration of Ignite Thin Client
     * @param connectTime list to save connection times
     * @param func Function to execute with IgniteClient
     */
    ThinClientContiniusRunner(ClientConfiguration cfg, List<Long> connectTime, Consumer<IgniteClient> func) {
        this.cfg = cfg;
        this.func = func;
        this.connectTime = connectTime;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        long connectStart;

        while (!Thread.currentThread().isInterrupted()) {

            connectStart = System.currentTimeMillis();

            try (IgniteClient client = Ignition.startClient(cfg)) {
                connectTime.add(System.currentTimeMillis() - connectStart);

                func.accept(client);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
