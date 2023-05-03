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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.tx.KillTransactionTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;

/** */
public class KillTransactionCommand implements Command<KillTransactionCommandArg, Map<ClusterNode, VisorTxTaskResult>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Kill transaction by xid";
    }

    /** {@inheritDoc} */
    @Override public Class<KillTransactionCommandArg> argClass() {
        return KillTransactionCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<KillTransactionTask> taskClass() {
        return KillTransactionTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T2<Boolean, Object>> nodes, KillTransactionCommandArg arg) {
        return nodes.keySet();
    }
}
