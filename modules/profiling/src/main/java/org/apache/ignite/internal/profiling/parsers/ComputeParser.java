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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.profiling.util.OrderedFixedSizeStructure;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

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
    /** Compute results: taskName -> aggregatedInfo. */
    private final Map<String, AggregatedTaskInfo> taskRes = new HashMap<>();

    /** Parsed jobs that have not mapped to task yet: sesId -> count&duration. */
    private final Map<IgniteUuid, long[]> unmergedIds = new HashMap<>();

    /** Top of slow tasks: duration -> sesId. */
    private final OrderedFixedSizeStructure<Long, IgniteUuid> topSlowTask = new OrderedFixedSizeStructure<>();

    /** Result map with collected jobs for slow tasks: sesId -> task & jobs. */
    private final Map<IgniteUuid, ObjectNode> topSlowRes = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        topSlowTask.put(duration, sesId);

        AggregatedTaskInfo info = taskRes.computeIfAbsent(taskName, k -> new AggregatedTaskInfo());

        info.mergeTask(sesId, duration);

        // Try merge unmerged ids.
        long[] arr = unmergedIds.get(sesId);

        if (arr != null) {
            info.mergeJob(arr[0], arr[1]);

            unmergedIds.remove(sesId);
        }
    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        for (AggregatedTaskInfo info : taskRes.values()) {
            if (info.ids.contains(sesId)) {
                info.mergeJob(1, duration);

                return;
            }
        }

        long[] arr = unmergedIds.computeIfAbsent(sesId, uuid -> new long[] {0, 0});

        arr[0] += 1;
        arr[1] += duration;
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode taskJson = MAPPER.createObjectNode();

        taskRes.forEach((taskName, info) -> {
            ObjectNode task = MAPPER.createObjectNode();

            task.put("count", info.count);
            task.put("duration", info.totalDuration);
            task.put("jobsCount", info.jobsCnt);
            task.put("jobsTotalDuration", info.jobsDuration);

            taskJson.set(taskName, task);
        });

        ArrayNode topSlowJson = MAPPER.createArrayNode();

//        topSlowTask.values().forEach(task -> {
//            ObjectNode node = MAPPER.createObjectNode();
//
//            long jobsTotalDuration = 0;
//
//            ArrayNode jobsJson = MAPPER.createArrayNode();
//
//            for (Job job : topSlowRes.get(task.sesId)) {
//                ObjectNode jobJson = MAPPER.createObjectNode();
//
//                jobJson.put("queuedTime", job.queuedTime);
//                jobJson.put("startTime", job.startTime);
//                jobJson.put("duration", job.duration);
//                jobJson.put("isTimedOut", job.isTimedOut);
//                jobJson.put("nodeId", job.nodeId);
//
//                jobsJson.add(jobJson);
//
//                jobsTotalDuration += job.duration;
//            }
//
//            node.put("taskName", task.taskName);
//            node.put("startTime", task.startTime);
//            node.put("duration", task.duration);
//            node.put("affPartId", task.affPartId);
//            node.put("nodeId", task.nodeId);
//            node.put("jobsTotalDuration", jobsTotalDuration);
//
//            node.set("jobs", jobsJson);
//
//            topSlowJson.add(node);
//        });

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

        /** */
        public void mergeJob(long jobsCnt, long jobsDuration) {
            this.jobsCnt += jobsCnt;
            this.jobsDuration += jobsDuration;
        }
    }
}
