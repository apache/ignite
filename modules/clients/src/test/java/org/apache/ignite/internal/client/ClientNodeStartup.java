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

package org.apache.ignite.internal.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Starts up one grid node (server) with pre-defined ports and tasks to test client-server interactions.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 * <p>
 * After this example has been started you can use pre-defined endpoints and task names in your
 * client-server interactions to work with the node over un-secure protocols (binary or http).
 * <p>
 * Usually you cannot start secured and unsecured nodes in one grid, so started together
 * secured and unsecured nodes belong to different grids.
 * <p>
 * Available endponts:
 * <ul>
 *     <li>127.0.0.1:10080 - TCP unsecured endpoint.</li>
 *     <li>127.0.0.1:11080 - HTTP unsecured endpoint.</li>
 * </ul>
 * <p>
 * Required credentials for remote client authentication: "s3cret".
 */
public class ClientNodeStartup {
    /**
     * Starts up two nodes with specified cache configuration on pre-defined endpoints.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException In case of any exception.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite g = G.start("modules/clients/src/test/resources/spring-server-node.xml")) {
            U.sleep(Long.MAX_VALUE);
        }
    }
}