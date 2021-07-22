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

package org.apache.ignite.network;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * {@code NodeFinder} implementation similar to the {@link StaticNodeFinder} but encapsulates a list of static local
 * addresses over a port range.
 */
public class LocalPortRangeNodeFinder implements NodeFinder {
    /** */
    private final List<NetworkAddress> addresses;

    /**
     * Creates a node finder that contains local network addresses over the given port range.
     *
     * @param startPort Start port (including).
     * @param endPort End port (excluding).
     */
    public LocalPortRangeNodeFinder(int startPort, int endPort) {
        addresses = IntStream.range(startPort, endPort)
            .mapToObj(port -> new NetworkAddress("localhost", port))
            .collect(toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override public List<NetworkAddress> findNodes() {
        return addresses;
    }
}
