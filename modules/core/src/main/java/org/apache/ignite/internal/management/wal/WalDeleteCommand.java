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

package org.apache.ignite.internal.management.wal;

import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.visor.misc.VisorWalTask;
import org.apache.ignite.internal.visor.misc.VisorWalTaskResult;

/** */
public class WalDeleteCommand implements ExperimentalCommand<WalDeleteCommandArg, VisorWalTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Delete unused archived wal segments on each node";
    }

    /** {@inheritDoc} */
    @Override public Class<WalDeleteCommandArg> argClass() {
        return WalDeleteCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorWalTask> taskClass() {
        return VisorWalTask.class;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(WalDeleteCommandArg arg) {
        return "Warning: the command will delete unused WAL segments.";
    }
}
