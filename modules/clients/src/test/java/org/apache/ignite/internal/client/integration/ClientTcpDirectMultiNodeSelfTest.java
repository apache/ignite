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

package org.apache.ignite.internal.client.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientProtocol;

/**
 *
 */
public class ClientTcpDirectMultiNodeSelfTest extends ClientAbstractMultiNodeSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        assert NODES_CNT > 3 : "Too few nodes to execute direct multinode test";

        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());

        Collection<String> srvs = new ArrayList<>(3);

        for (int i = 0; i < NODES_CNT / 2; i++)
            srvs.add(HOST + ':' + (REST_TCP_PORT_BASE + i));

        cfg.setRouters(srvs);

        return cfg;
    }
}
