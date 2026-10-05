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

package org.apache.ignite.internal.management.ssl;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.jetbrains.annotations.Nullable;

/** */
public class SslStatusCommand implements ComputeCommand<NoArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Show the TLS certificates every cluster node serves, the authorities it trusts, and how its last " +
            "reload went";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SslStatusTask> taskClass() {
        return SslStatusTask.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, NoArg arg) {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public void printResult(NoArg arg, String res, Consumer<String> printer) {
        printer.accept(res);
    }
}
