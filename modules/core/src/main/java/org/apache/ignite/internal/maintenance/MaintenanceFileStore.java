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

package org.apache.ignite.internal.maintenance;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceTask;

/**
 * Provides API for durable storage of {@link MaintenanceTask}s and hides implementation details from higher levels.
 *
 * Human-readable storage format is rigid but simple.
 * <ol>
 *     <li>
 *         Maintenance file with tasks is stored in work directory of node
 *         under persistent store root defined by consistentId of node.
 *     </li>
 *     <li>
 *         Each task is written to disk as a {@link String} on a separate line.
 *     </li>
 *     <li>
 *         Task consists of two or three parts: task UUID, task description and optional parameters.
 *     </li>
 * </ol>
 */
public class MaintenanceFileStore {
    /** */
    private static final String TASKS_SEPARATOR = System.lineSeparator();

    /** */
    private static final String TASK_PARTS_SEPARATOR = "\t";

    /** Maintenance task consists of two or three parts: ID, description (user-readable part)
     * and optional task parameters. */
    private static final int MAX_MNTC_TASK_PARTS_COUNT = 3;

    /** */
    private final boolean disabled;

    /** */
    private final PdsFoldersResolver pdsFoldersResolver;

    /** */
    private volatile File mntcTasksFile;

    /** */
    private volatile FileIO mntcTasksFileIO;

    /** */
    private final FileIOFactory ioFactory;

    /** */
    private final Map<String, MaintenanceTask> tasksInSync = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    public MaintenanceFileStore(boolean disabled,
                                PdsFoldersResolver pdsFoldersResolver,
                                FileIOFactory ioFactory,
                                IgniteLogger log) {
        this.disabled = disabled;
        this.pdsFoldersResolver = pdsFoldersResolver;
        this.ioFactory = ioFactory;
        this.log = log;
    }

    /** */
    public void init() throws IgniteCheckedException, IOException {
        if (disabled)
            return;

        NodeFileTree ft = pdsFoldersResolver.fileTree();

        U.ensureDirectory(ft.nodeStorage(), "store directory for node persistent data", log);

        mntcTasksFile = ft.maintenance();

        if (!mntcTasksFile.exists())
            mntcTasksFile.createNewFile();

        mntcTasksFileIO = ioFactory.create(mntcTasksFile);

        readTasksFromFile();
    }

    /**
     * Deletes file with maintenance tasks.
     */
    public void clear() {
        if (mntcTasksFile != null)
            mntcTasksFile.delete();
    }

    /**
     * Stops
     */
    public void stop() throws IOException {
        if (disabled)
            return;

        if (mntcTasksFileIO != null)
            mntcTasksFileIO.close();
    }

    /** */
    private void readTasksFromFile() throws IOException {
        int len = (int)mntcTasksFileIO.size();

        if (len == 0)
            return;

        byte[] allBytes = new byte[len];

        mntcTasksFileIO.read(allBytes, 0, len);

        String[] allTasks = new String(allBytes).split(TASKS_SEPARATOR);

        for (String taskStr : allTasks) {
            String[] subStrs = taskStr.split(TASK_PARTS_SEPARATOR);

            int partsNum = subStrs.length;

            if (partsNum < MAX_MNTC_TASK_PARTS_COUNT - 1) {
                log.info("Corrupted maintenance task found and will be skipped, " +
                    "mandatory parts are missing: " + taskStr);

                continue;
            }

            if (partsNum > MAX_MNTC_TASK_PARTS_COUNT) {
                log.info("Corrupted maintenance task found and will be skipped, " +
                    "too many parts in task: " + taskStr);

                continue;
            }

            String name = subStrs[0];

            MaintenanceTask task = new MaintenanceTask(name, subStrs[1], partsNum == 3 ? subStrs[2] : null);

            tasksInSync.put(name, task);
        }
    }

    /** */
    private void writeTasksToFile() throws IOException {
        mntcTasksFileIO.clear();

        String allTasks = tasksInSync.values().stream()
            .map(
                task -> task.name() +
                    TASK_PARTS_SEPARATOR +
                    task.description() +
                    TASK_PARTS_SEPARATOR +
                    (task.parameters() != null ? task.parameters() : "")
            )
            .collect(Collectors.joining(System.lineSeparator()));

        byte[] allTasksBytes = allTasks.getBytes();

        int left = allTasksBytes.length;
        int len = allTasksBytes.length;

        while ((left -= mntcTasksFileIO.writeFully(allTasksBytes, len - left, left)) > 0)
            ;

        mntcTasksFileIO.force();
    }

    /** */
    public Map<String, MaintenanceTask> getAllTasks() {
        if (disabled)
            return null;

        return Collections.unmodifiableMap(tasksInSync);
    }

    /** */
    public void writeMaintenanceTask(MaintenanceTask task) throws IOException {
        if (disabled)
            return;

        tasksInSync.put(task.name(), task);

        writeTasksToFile();
    }

    /** */
    public void deleteMaintenanceTask(String taskName) throws IOException {
        if (disabled)
            return;

        tasksInSync.remove(taskName);

        writeTasksToFile();
    }
}
