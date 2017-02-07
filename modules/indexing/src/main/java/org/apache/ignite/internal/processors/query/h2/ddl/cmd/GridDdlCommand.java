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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlOperationArguments;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationInit;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlOperationInitError;

/**
 * Interface for a DDL command handler - the handler for DDL protocol events containing actual logic.\
 * Implementations should be stateless and rely only on arguments passed to methods.
 */
public interface GridDdlCommand<A extends DdlOperationArguments> {
    /**
     * Handle {@link DdlOperationInit} message <b>synchronously</b> - do <i>fast</i> local checks and preparations.
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void init(A args) throws IgniteCheckedException;

    /**
     * Handle {@link DdlOperationInitError} message <b>synchronously</b> - do local cleanup
     * of whatever {@link #init) has done.
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void initError(A args) throws IgniteCheckedException;

    /**
     * Do actual DDL work on a local node.
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void execute(A args) throws IgniteCheckedException;

    /**
     * Revert effects of executing local part of DDL job on this node.
     * May be called in the case of an error on one of the peer nodes, or user cancel.
     * @param args Operation arguments.
     * @throws IgniteCheckedException if failed.
     */
    public void cancel(A args) throws IgniteCheckedException;
}
