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

package org.apache.ignite.internal.commandline.query;

import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;

/**
 * Subcommands of the kill command.
 *
 * @see KillCommand
 * @see QueryMXBean
 * @see ComputeMXBean
 * @see TransactionsMXBean
 * @see ServiceMXBean
 * @see SnapshotMXBean
 */
public enum KillSubcommand {
    /** Kill compute task. */
    COMPUTE,

    /** Kill transaction. */
    TRANSACTION,

    /** Kill service. */
    SERVICE,

    /** Kill sql query. */
    SQL,

    /** Kill scan query. */
    SCAN,

    /** Kill continuous query. */
    CONTINUOUS,

    /** Kill snapshot operation. */
    SNAPSHOT,
}
