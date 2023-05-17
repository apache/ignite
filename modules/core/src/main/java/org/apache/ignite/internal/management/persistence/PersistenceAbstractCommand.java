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

package org.apache.ignite.internal.management.persistence;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceCleanAllTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceCleanCorruptedTaskArg;
import org.apache.ignite.internal.management.persistence.PersistenceCommand.PersistenceTaskArg;
import org.apache.ignite.internal.visor.persistence.PersistenceTask;
import org.apache.ignite.internal.visor.persistence.PersistenceTaskResult;
import org.apache.ignite.lang.IgniteBiTuple;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** */
public abstract class PersistenceAbstractCommand implements ComputeCommand<PersistenceTaskArg, PersistenceTaskResult> {
    /** {@inheritDoc} */
    @Override public Class<PersistenceTask> taskClass() {
        return PersistenceTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(PersistenceTaskArg arg, PersistenceTaskResult res, Consumer<String> printer) {
        if (!res.inMaintenanceMode())
            printer.accept("Persistence command can be sent only to node in Maintenance Mode.");
        else if (res.cachesInfo() != null) {
            //info command
            printer.accept("Persistent caches found on node:");

            //sort results so corrupted caches occur in the list at the top
            res.cachesInfo().entrySet().stream().sorted((ci0, ci1) -> {
                IgniteBiTuple<Boolean, Boolean> t0 = ci0.getValue();
                IgniteBiTuple<Boolean, Boolean> t1 = ci1.getValue();

                boolean corrupted0 = t0.get1() || t0.get2();
                boolean corrupted1 = t1.get1() || t1.get2();

                if (corrupted0 && corrupted1)
                    return 0;
                else if (!corrupted0 && !corrupted1)
                    return 0;
                else if (corrupted0 && !corrupted1)
                    return -1;
                else
                    return 1;
            }).forEach(
                e -> {
                    IgniteBiTuple<Boolean, Boolean> t = e.getValue();

                    String status;

                    if (!t.get1())
                        status = "corrupted - WAL disabled globally.";
                    else if (!t.get2())
                        status = "corrupted - WAL disabled locally.";
                    else
                        status = "no corruption.";

                    printer.accept(INDENT + "cache name: " + e.getKey() + ". Status: " + status);
                }
            );
        }
        else if (arg instanceof PersistenceCleanAllTaskArg
                || arg instanceof PersistenceCleanCorruptedTaskArg
                || arg instanceof PersistenceCleanCachesTaskArg) {
            //clean command
            printer.accept("Maintenance task is " + (!res.maintenanceTaskCompleted() ? "not " : "") + "fixed.");

            List<String> cleanedCaches = res.handledCaches();

            if (cleanedCaches != null && !cleanedCaches.isEmpty()) {
                String cacheDirNames = String.join(", ", cleanedCaches);

                printer.accept("Cache directories were cleaned: [" + cacheDirNames + ']');
            }

            List<String> failedToHandleCaches = res.failedCaches();

            if (failedToHandleCaches != null && !failedToHandleCaches.isEmpty()) {
                String failedToHandleCachesStr = String.join(", ", failedToHandleCaches);

                printer.accept("Failed to clean following directories: [" + failedToHandleCachesStr + ']');
            }
        }
        else {
            // backup command
            List<String> backupCompletedCaches = res.handledCaches();

            if (backupCompletedCaches != null && !backupCompletedCaches.isEmpty()) {
                String cacheDirNames = String.join(", ", backupCompletedCaches);

                printer.accept("Cache data files was backed up to the following directories in node's work directory: [" +
                    cacheDirNames + ']');
            }

            List<String> backupFailedCaches = res.failedCaches();

            if (backupFailedCaches != null && !backupFailedCaches.isEmpty()) {
                String backupFailedCachesStr = String.join(", ", backupFailedCaches);

                printer.accept("Failed to backup the following directories in node's work directory: [" +
                    backupFailedCachesStr + ']');
            }
        }
    }
}
