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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.function.Consumer;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;

public class NodeLeaveHandler implements TopologyEventHandler {
    private final Consumer<ClusterNode> onDisappeared;

    public NodeLeaveHandler(Consumer<ClusterNode> onDisappeared) {
        this.onDisappeared = onDisappeared;
    }

    /** {@inheritDoc} */
    @Override
    public void onAppeared(ClusterNode member) {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override
    public void onDisappeared(ClusterNode member) {
        onDisappeared.accept(member);
    }
}
