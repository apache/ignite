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

package org.apache.ignite.internal.profiling.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.profiling.util.OrderedFixedSizeStructure;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.profiling.ProfilingLogParser.currentNodeId;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;

/**
 * Builds JSON with aggregated tasks and jobs statistics.
 *
 * Example:
 * <pre>
 * {
 *      $taskName : {
 *          "count" : $executionsCount,
 *          "duration" : $duration,
 *          "jobsCount" : $jobsCount,
 *          "jobsTotalDuration" : $jobsTotalDuration
 *      }
 * }
 * </pre>
 * Example of slowest tasks:
 * <pre>
 * [
 *  {
 *      "taskName" : $taskName,
 *      "startTime" : $startTime,
 *      "duration" : $duration,
 *      "affPartId" : $affPartId,
 *      "nodeId" : $nodeId,
 *      "jobsTotalDuration" : $jobsTotalDuration,
 *      "jobs" : [ {
 *          "queuedTime" : $queuedTime,
 *          "startTime" : $startTime,
 *          "duration" : $duration,
 *          "isTimedOut" : $isTimedOut,
 *          "nodeId" : "$nodeId"
 *      } ]
 *  }
 * ]
 * </pre>
 */
public class ComputeParser implements IgniteLogParser {
    /** Aggregated tasks: taskName -> aggregatedInfo. */
    private final Map<String, AggregatedTaskInfo> taskRes = new HashMap<>();

    /** Jobs: sesId -> jobs. */
    private final Map<IgniteUuid, List<Job>> jobs = new HashMap<>();

    /** Top of slow tasks: duration -> task. */
    private final OrderedFixedSizeStructure<Long, Task> topSlowTask = new OrderedFixedSizeStructure<>();

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        Task task = new Task(currentNodeId(), sesId, taskName, startTime, duration, affPartId);

        topSlowTask.put(duration, task);

        AggregatedTaskInfo info = taskRes.computeIfAbsent(taskName, k -> new AggregatedTaskInfo());

        info.mergeTask(sesId, duration);
    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        Job job = new Job(currentNodeId(), queuedTime, startTime, duration, timedOut);

        jobs.computeIfAbsent(sesId, uuid -> new LinkedList<>()).add(job);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode taskJson = MAPPER.createObjectNode();

        taskRes.forEach((taskName, info) -> {
            info.ids.forEach(uuid -> {
                List<Job> jobs = this.jobs.get(uuid);

                if (jobs == null)
                    return;

                jobs.forEach(job -> {
                    info.jobsCnt += 1;
                    info.jobsDuration += job.duration;
                });
            });

            ObjectNode task = MAPPER.createObjectNode();

            task.put("count", info.count);
            task.put("duration", info.totalDuration);
            task.put("jobsCount", info.jobsCnt);
            task.put("jobsTotalDuration", info.jobsDuration);

            taskJson.set(taskName, task);
        });

        ArrayNode topSlowJson = MAPPER.createArrayNode();

        topSlowTask.values().forEach(task -> {
            ObjectNode node = MAPPER.createObjectNode();

            long jobsTotalDuration = 0;

            ArrayNode jobsJson = MAPPER.createArrayNode();

            for (Job job : jobs.get(task.sesId)) {
                ObjectNode jobJson = MAPPER.createObjectNode();

                jobJson.put("queuedTime", job.queuedTime);
                jobJson.put("startTime", job.startTime);
                jobJson.put("duration", job.duration);
                jobJson.put("isTimedOut", job.isTimedOut);
                jobJson.put("nodeId", String.valueOf(job.nodeId));

                jobsJson.add(jobJson);

                jobsTotalDuration += job.duration;
            }

            node.put("taskName", task.taskName);
            node.put("startTime", task.startTime);
            node.put("duration", task.duration);
            node.put("affPartId", task.affPartId);
            node.put("nodeId", String.valueOf(task.nodeId));
            node.put("jobsTotalDuration", jobsTotalDuration);

            node.set("jobs", jobsJson);

            topSlowJson.add(node);
        });

        return U.map("totalCompute", taskJson, "topSlowCompute", topSlowJson);
    }

    /** */
    private static class AggregatedTaskInfo {
        /** Executions. */
        long count;

        /** Duration. */
        long totalDuration;

        /** Jobs count. */
        long jobsCnt;

        /** Jobs total duration. */
        long jobsDuration;

        /** Jobs ids. */
        final Set<IgniteUuid> ids = new HashSet<>();

        /** */
        public void mergeTask(IgniteUuid sesId, long duration) {
            count += 1;
            totalDuration += duration;

            ids.add(sesId);
        }
    }

    /** Task. */
    public class Task {
        /** Node id. */
        private final UUID nodeId;

        /** Session id. */
        private final IgniteUuid sesId;

        /** Task name. */
        private final String taskName;

        /** Start time. */
        private final long startTime;

        /** Duration. */
        private final long duration;

        /** Affinity partition id. */
        private final int affPartId;

        /**
         * @param id Node id.
         * @param sesId Session id.
         * @param taskName Task name.
         * @param startTime Start time.
         * @param duration Duration.
         * @param affPartId Affinity partition id.
         */
        public Task(UUID id, IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
            nodeId = id;
            this.sesId = sesId;
            this.taskName = taskName;
            this.startTime = startTime;
            this.duration = duration;
            this.affPartId = affPartId;
        }
    }

    /** Job. */
    public class Job {
        /** Node id. */
        private final UUID nodeId;

        /** Time job spent on waiting queue. */
        private final long queuedTime;

        /** Start time. */
        private final long startTime;

        /** Job execution time. */
        private final long duration;

        /** {@code True} if job is timed out. */
        private final boolean isTimedOut;

        /**
         * @param nodeId Node id.
         * @param queuedTime Time job spent on waiting queue.
         * @param startTime Start time.
         * @param duration Job execution time.
         * @param isTimedOut {@code True} if job is timed out.
         */
        public Job(UUID nodeId, long queuedTime, long startTime, long duration, boolean isTimedOut) {
            this.nodeId = nodeId;
            this.queuedTime = queuedTime;
            this.startTime = startTime;
            this.duration = duration;
            this.isTimedOut = isTimedOut;
        }
    }
}
