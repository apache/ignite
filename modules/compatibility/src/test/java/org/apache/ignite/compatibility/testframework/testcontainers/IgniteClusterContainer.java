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

package org.apache.ignite.compatibility.testframework.testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

/** Ignite cluster container. */
public class IgniteClusterContainer implements Startable {
    /** Containers. */
    private final List<IgniteContainer> containers;

    /** Network. */
    private final Network net = Network.newNetwork();

    /**
     * @param commitHash Commit hash.
     * @param consistentIds Consistent ID's.
     */
    public IgniteClusterContainer(String commitHash, List<String> consistentIds) throws IOException {
        containers = new ArrayList<>(consistentIds.size());

        for (int i = 0; i < consistentIds.size(); i++) {
            String hostname = "node" + (1 + i);

            IgniteContainer ignite = new IgniteContainer(commitHash, net, hostname, consistentIds.get(i), i);

            containers.add(ignite);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        Startables.deepStart(containers).join();

        containers.get(0).activateCluster(containers.size());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        for (IgniteContainer container : containers)
            container.stop();

        net.close();
    }

    /** */
    public List<IgniteContainer> containers() {
        return Collections.unmodifiableList(containers);
    }
}
