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

package org.apache.ignite.internal.processors.query.h2.ddl.cmd;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlCommandArguments;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationAck;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationCancel;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationInit;

/**
 * Interface for a DDL command handler - the handler for DDL protocol events containing actual logic.
 * Implementations should be stateless and rely only on arguments passed to methods.
 */
public interface GridDdlCommand<A extends DdlCommandArguments> {
    /**
     * Choose the nodes this command will be executed on. Invoked once at the <b>coordinator</b>.
     *
     * @param kernalCtx Kernal context.
     * @param args Command arguments.
     * @param topVer Topology version.
     * @return Nodes to execute the command.
     */
    public Collection<ClusterNode> filterNodes(GridKernalContext kernalCtx, A args, AffinityTopologyVersion topVer);

    /**
     * Handle {@link DdlOperationInit} message - do <i>fast</i> local checks and preparations.
     *
     * @param args Command arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void init(A args) throws IgniteCheckedException;

    /**
     * Do actual DDL work on a local node. Is called from {@link DdlOperationAck} handler.
     *
     * @param args Command arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void execute(A args) throws IgniteCheckedException;

    /**
     * Revert effects of executing init or local part of DDL job on this node.
     * May be called in the case of an error on one of the peer nodes, or user cancel.
     * Is called from {@link DdlOperationCancel} message handler.
     *
     * @param args Command arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void cancel(A args) throws IgniteCheckedException;
}
