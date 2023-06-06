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

import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTask;
import org.jetbrains.annotations.Nullable;

/** */
public class SnapshotRestoreCommand extends AbstractSnapshotCommand<SnapshotRestoreCommandArg> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Restore snapshot";
    }

    /** {@inheritDoc} */
    @Override public @Nullable String deprecationMessage(SnapshotRestoreCommandArg arg) {
        if (arg.start())
            return "Command option '--start' is redundant and must be avoided.";
        else if (arg.cancel())
            return "Command deprecated. Use '--snapshot cancel' instead.";
        else if (arg.status())
            return "Command deprecated. Use '--snapshot status' instead.";

        return null;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotRestoreCommandArg> argClass() {
        return SnapshotRestoreCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorSnapshotRestoreTask> taskClass() {
        return VisorSnapshotRestoreTask.class;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(SnapshotRestoreCommandArg arg) {
        return arg.status() || arg.cancel() || arg.groups() != null
            ? null :
            "Warning: command will restore ALL USER-CREATED CACHE GROUPS from the snapshot " + arg.snapshotName() + '.';
    }
}
