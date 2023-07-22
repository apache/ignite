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

package org.apache.ignite.internal.management;

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.typedef.internal.U.DELIM;

/** */
public class StateCommand implements LocalCommand<NoArg, GridTuple3<UUID, String, ClusterState>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print current cluster state";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public GridTuple3<UUID, String, ClusterState> execute(
        @Nullable GridClient cli,
        @Nullable Ignite ignite,
        NoArg arg,
        Consumer<String> printer
    ) throws GridClientException {
        ClusterState state;
        UUID id;
        String tag;

        if (cli != null) {
            GridClientClusterState state0 = cli.state();

            state = state0.state();
            id = state0.id();
            tag = state0.tag();
        }
        else {
            state = ignite.cluster().state();
            id = ignite.cluster().id();
            tag = ignite.cluster().tag();
        }

        printer.accept("Cluster  ID: " + id);
        printer.accept("Cluster tag: " + tag);

        printer.accept(DELIM);

        switch (state) {
            case ACTIVE:
                printer.accept("Cluster is active");

                break;

            case INACTIVE:
                printer.accept("Cluster is inactive");

                break;

            case ACTIVE_READ_ONLY:
                printer.accept("Cluster is active (read-only)");

                break;

            default:
                throw new IllegalStateException("Unknown state: " + state);
        }

        return F.t(id, tag, state);
    }
}
