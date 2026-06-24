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

import java.util.ArrayList;
import java.util.List;
import org.testcontainers.lifecycle.Startable;

/** Ignite cluster container. */
public class IgniteClusterContainer implements Startable {
    /** Containers. */
    private final List<IgniteContainer> containers;

    /** @param commitHash Commit hash. */
    public IgniteClusterContainer(String commitHash, int size) {
        containers = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            String hostname = "node" + (1 + i);

            IgniteContainer ignite = new IgniteContainer(commitHash, hostname);

            containers.add(ignite);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        for (IgniteContainer container : containers)
            container.start();

        containers.get(0).activateCluster(containers.size());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        for (IgniteContainer container : containers)
            container.stop();
    }

    /** */
    public List<IgniteContainer> containers() {
        return containers;
    }
}
