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

package org.apache.ignite.internal.management.snapshot;

/** */
public class SnapshotDeleteCommand extends AbstractSnapshotCommand<SnapshotDeleteCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Delete snapshot and all its increments from the cluster";
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotDeleteCommandArg> argClass() {
        return SnapshotDeleteCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotDeleteTask> taskClass() {
        return SnapshotDeleteTask.class;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(SnapshotDeleteCommandArg arg) {
        return "Warning: command will delete snapshot " + arg.snapshotName() + " and all its increments " +
            "from all the cluster nodes. This operation is irreversible.";
    }
}
