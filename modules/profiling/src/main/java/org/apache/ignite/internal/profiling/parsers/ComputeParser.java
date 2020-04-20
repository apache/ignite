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
import org.apache.ignite.internal.profiling.util.OrderedFixedSizeStructure;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 * totalRes = {$taskName:{count:zzz, totalDuration:yyy, jobsCount:ccc}}
 * topSlowRes = [{$taskName:xxx, duration:yyy, startTime:zzz, nodes:[{$nodeName:nnn, duration:sss, startTime:zzz}]}]
 */
public class ComputeParser implements IgniteLogParser {
    /** Compute results: taskName -> aggregatedInfo. */
    private final Map<String, AggregatedTaskInfo> taskRes = new HashMap<>();

    /** Parsed jobs that have not mapped to task yet: sesId -> count&duration. */
    private final Map<IgniteUuid, long[]> unmergedIds = new HashMap<>();

    /** Tree to store top of slow tasks: duration -> task. */
    private final OrderedFixedSizeStructure<Long, Task> topSlowTask = new OrderedFixedSizeStructure<>();

    /** Result map with collected jobs for slow tasks: sesId -> jobs. */
    private final Map<IgniteUuid, List<Job>> topSlowRes = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void parse(String nodeId, String str) {
        if (str.startsWith("task"))
            parseTask(nodeId, str);
        else if (str.startsWith("job"))
            parseJob(nodeId, str);
    }

    /** */
    private void parseTask(String nodeId, String str) {
        Task task = new Task(nodeId, str);

        topSlowTask.put(task.duration, task);

        AggregatedTaskInfo info = taskRes.computeIfAbsent(task.taskName, k -> new AggregatedTaskInfo());

        info.merge(task);

        // Try merge unmerged ids.
        long[] arr = unmergedIds.get(task.sesId);

        if (arr != null) {
            info.jobsCnt += arr[0];
            info.jobsDuration += arr[1];

            unmergedIds.remove(task.sesId);
        }
    }

    /** */
    private void parseJob(String nodeId, String str) {
        Job job = new Job(nodeId, str);

        for (AggregatedTaskInfo info : taskRes.values()) {
            if (info.ids.contains(job.sesId)) {
                info.merge(job);

                return;
            }
        }

        long[] arr = unmergedIds.computeIfAbsent(job.sesId, uuid -> new long[] {0, 0});

        arr[0] += 1;
        arr[1] += job.duration;
    }

    /** {@inheritDoc} */
    @Override public void onFirstPhaseEnd() {
        topSlowTask.values().forEach(task -> topSlowRes.put(task.sesId, new LinkedList<>()));
    }

    /** {@inheritDoc} */
    @Override public void parsePhase2(String nodeId, String str) {
        if (str.startsWith("job") && !topSlowRes.isEmpty()) {
            Job job = new Job(nodeId, str);

            topSlowRes.computeIfPresent(job.sesId, (uuid, jobs) -> {
                jobs.add(job);

                return jobs;
            });
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode taskJson = mapper.createObjectNode();

        taskRes.forEach((taskName, info) -> {
            ObjectNode task = mapper.createObjectNode();

            task.put("count", info.count);
            task.put("duration", info.totalDuration);
            task.put("jobsCount", info.jobsCnt);
            task.put("jobsTotalDuration", info.jobsDuration);

            taskJson.set(taskName, task);
        });

        ArrayNode topSlowJson = mapper.createArrayNode();

        topSlowTask.values().forEach(task -> {
            ObjectNode node = mapper.createObjectNode();

            long jobsTotalDuration = 0;

            ArrayNode jobsJson = mapper.createArrayNode();

            for (Job job : topSlowRes.get(task.sesId)) {
                ObjectNode jobJson = mapper.createObjectNode();

                jobJson.put("queuedTime", job.queuedTime);
                jobJson.put("startTime", job.startTime);
                jobJson.put("duration", job.duration);
                jobJson.put("isTimedOut", job.isTimedOut);
                jobJson.put("nodeId", job.nodeId);

                jobsJson.add(jobJson);

                jobsTotalDuration += job.duration;
            }

            node.put("taskName", task.taskName);
            node.put("startTime", task.startTime);
            node.put("duration", task.duration);
            node.put("affPartId", task.affPartId);
            node.put("nodeId", task.nodeId);
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

        /** @param task Task to merge with. */
        public void merge(Task task) {
            count += 1;
            totalDuration += task.duration;

            ids.add(task.sesId);
        }

        /** @param job Job to merge with. */
        public void merge(Job job) {
            jobsCnt += 1;
            jobsDuration += job.duration;
        }
    }

    /** Task. */
    private static class Task {
        /** Node id. */
        final String nodeId;

        /** Session id. */
        IgniteUuid sesId;

        /** Task name. */
        String taskName;

        /** Start time. */
        long startTime;

        /** Duration. */
        long duration;

        /** Affinity partition id. */
        int affPartId;

        /**
         * @param nodeId Node id.
         * @param str String to parse from.
         */
        Task (String nodeId, String str) {
            this.nodeId = nodeId;

            int idx = str.indexOf('=');
            int idx2 = str.indexOf(',', idx);
            sesId = IgniteUuid.fromString(str.substring(idx + 1, idx2));

            int textIdx = str.indexOf('=', idx2) + 1;

            idx = str.lastIndexOf(']');
            idx2 = str.lastIndexOf('=', idx);
            affPartId = Integer.parseInt(str.substring(idx2 + 1, idx));

            idx = str.lastIndexOf(',', idx2);
            idx2 = str.lastIndexOf('=', idx);
            duration = Long.parseLong(str.substring(idx2 + 1, idx));

            idx = str.lastIndexOf(',', idx2);
            idx2 = str.lastIndexOf('=', idx);
            startTime = Long.parseLong(str.substring(idx2 + 1, idx));

            idx = str.lastIndexOf(',', idx2);
            taskName = str.substring(textIdx, idx);
        }
    }

    /** Job. */
    private static class Job {
        /** Node id. */
        final String nodeId;

        /** Session id. */
        IgniteUuid sesId;

        /** Time job spent on waiting queue. */
        long queuedTime;

        /** Start time. */
        long startTime;

        /** Job execution time. */
        long duration;

        /**  {@code True} if job is timed out. */
        boolean isTimedOut;

        /**
         * @param nodeId Node id.
         * @param str String to parse from.
         */
        Job(String nodeId, String str) {
            this.nodeId = nodeId;

            isTimedOut = str.charAt(str.length() - 3) == 'u';

            int idx = str.indexOf('=');
            int idx2 = str.indexOf(',', idx);
            sesId = IgniteUuid.fromString(str.substring(idx + 1, idx2));

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(',', idx);
            queuedTime = Long.parseLong(str.substring(idx + 1, idx2));

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(',', idx);
            startTime = Long.parseLong(str.substring(idx + 1, idx2));

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(',', idx);
            duration = Long.parseLong(str.substring(idx + 1, idx2));
        }
    }
}
