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

import java.util.Collection;
import java.util.function.Consumer;

/** */
public class SnapshotListCommand extends AbstractSnapshotCommand<SnapshotListCommandArg, Collection<String>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "List all snapshots and their increments in the cluster";
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListCommandArg> argClass() {
        return SnapshotListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListTask> taskClass() {
        return SnapshotListTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(SnapshotListCommandArg arg, Collection<String> snapshots, Consumer<String> printer) {
        if (snapshots.isEmpty()) {
            printer.accept("There are no snapshots in the cluster.");

            return;
        }

        for (String snpName : snapshots)
            printer.accept(snpName);
    }
}
