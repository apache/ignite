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

package org.apache.ignite.internal.management.kill;

import java.util.Collection;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.X;

/** */
public class KillClientCommand implements ComputeCommand<KillClientCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Kill client connection by id";
    }

    /** {@inheritDoc} */
    @Override public Class<KillClientCommandArg> argClass() {
        return KillClientCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<ClientConnectionDropTask> taskClass() {
        return ClientConnectionDropTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, KillClientCommandArg arg) {
        return CommandUtils.nodeOrAll(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public Void handleException(Exception e) throws Exception {
        if (X.hasCause(e, ClientConnectionException.class))
            return null;

        throw e;
    }
}
