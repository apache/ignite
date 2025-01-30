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
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTask;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTaskResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.api.CommandUtils.nodes;
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
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        NoArg arg,
        Consumer<String> printer
    ) throws Exception {
        ClusterState state;
        UUID id;
        String tag;

        if (client != null) {
            VisorIdAndTagViewTaskResult idAndTag = CommandUtils.execute(client, null,
                VisorIdAndTagViewTask.class, null, nodes(client, ignite));

            state = client.cluster().state();
            id = idAndTag.id();
            tag = idAndTag.tag();
        }
        else {
            state = ignite.cluster().state();
            id = ignite.cluster().id();
            tag = ignite.cluster().tag();
        }

        printer.accept("Cluster  ID: " + id);
        printer.accept("Cluster tag: " + tag);

        printer.accept(DELIM);

        printer.accept("Cluster state: " + state);

        return F.t(id, tag, state);
    }
}
