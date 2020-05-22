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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.cache.IdleVerify.logParsedArgs;

/**
 * Task for collection checksums primary and backup partitions of specified caches. <br> Argument: Set of cache names,
 * 'null' will trigger verification for all caches. <br> Result: {@link IdleVerifyDumpResult} with all found partitions.
 * <br> Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsDumpTask extends ComputeTaskAdapter<VisorIdleVerifyTaskArg, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");

    /** Visible for testing. */
    public static final String IDLE_DUMP_FILE_PREFIX = "idle-dump-";

    /** Delegate for map execution */
    private final VerifyBackupPartitionsTaskV2 delegate = new VerifyBackupPartitionsTaskV2();

    /** */
    private VisorIdleVerifyDumpTaskArg taskArg;

    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        VisorIdleVerifyTaskArg arg
    ) throws IgniteException {
        if (arg instanceof VisorIdleVerifyDumpTaskArg)
            taskArg = (VisorIdleVerifyDumpTaskArg)arg;

        return delegate.map(subgrid, arg);
    }

    /** {@inheritDoc} */
    @Override public @Nullable String reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes = new TreeMap<>(buildPartitionKeyComparator());

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                continue;

            Map<PartitionKeyV2, PartitionHashRecordV2> nodeHashes = res.getData();

            for (Map.Entry<PartitionKeyV2, PartitionHashRecordV2> e : nodeHashes.entrySet()) {
                clusterHashes
                    .computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                    .add(e.getValue());
            }
        }

        Comparator<PartitionHashRecordV2> recordComp = buildRecordComparator().reversed();

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> partitions = new LinkedHashMap<>();

        int skippedRecords = 0;

        for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : clusterHashes.entrySet()) {
            if (needToAdd(entry.getValue())) {
                entry.getValue().sort(recordComp);

                partitions.put(entry.getKey(), entry.getValue());
            }
            else
                skippedRecords++;
        }

        return writeHashes(partitions, delegate.reduce(results), skippedRecords);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(
        ComputeJobResult res,
        List<ComputeJobResult> rcvd
    ) throws IgniteException {
        return delegate.result(res, rcvd);
    }

    /**
     * Checking conditions for adding given record to result.
     *
     * @param records records to check.
     * @return {@code true} if this records should be add to result and {@code false} otherwise.
     */
    private boolean needToAdd(List<PartitionHashRecordV2> records) {
        if (records.isEmpty() || (taskArg != null && !taskArg.skipZeros()))
            return true;

        PartitionHashRecordV2 record = records.get(0);

        if (record.size() != 0)
            return true;

        int firstHash = record.partitionHash();

        for (int i = 1; i < records.size(); i++) {
            record = records.get(i);

            if (record.partitionHash() != firstHash || record.size() != 0)
                return true;
        }

        return false;
    }

    /**
     * @param partitions Dump result.
     * @param conflictRes Conflict results.
     * @param skippedRecords Number of empty partitions.
     * @return Path where results are written.
     * @throws IgniteException If failed to write the file.
     */
    private String writeHashes(
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> partitions,
        IdleVerifyResultV2 conflictRes,
        int skippedRecords
    ) throws IgniteException {
        String wd = ignite.configuration().getWorkDirectory();

        File workDir = wd == null ? new File("/tmp") : new File(wd);

        File out = new File(workDir, IDLE_DUMP_FILE_PREFIX + LocalDateTime.now().format(TIME_FORMATTER) + ".txt");

        if (ignite.log().isInfoEnabled())
            ignite.log().info("IdleVerifyDumpTask will write output to " + out.getAbsolutePath());

        try (PrintWriter writer = new PrintWriter(new FileWriter(out))) {
            writeResult(partitions, conflictRes, skippedRecords, writer);

            writer.flush();

            if (ignite.log().isInfoEnabled())
                ignite.log().info("IdleVerifyDumpTask successfully written dump to '" + out.getAbsolutePath() + "'");
        }
        catch (IOException | IgniteException e) {
            ignite.log().error("Failed to write dump file: " + out.getAbsolutePath(), e);

            throw new IgniteException(e);
        }

        return out.getAbsolutePath();
    }

    /** */
    private void writeResult(
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> partitions,
        IdleVerifyResultV2 conflictRes,
        int skippedRecords,
        PrintWriter writer
    ) {
        Map<ClusterNode, Exception> exceptions = conflictRes.exceptions();

        if (!F.isEmpty(exceptions)) {
            boolean noMatchingCaches = false;

            for (Exception e : exceptions.values()) {
                if (e instanceof NoMatchingCachesException) {
                    noMatchingCaches = true;

                    break;
                }
            }

            int size = exceptions.size();

            writer.write("idle_verify failed on " + size + " node" + (size == 1 ? "" : "s") + ".\n");

            if (noMatchingCaches)
                writer.write("There are no caches matching given filter options.");
        }

        if (!partitions.isEmpty())
            writer.write("idle_verify check has finished, found " + partitions.size() + " partitions\n");

        logParsedArgs(taskArg, writer::write);

        if (skippedRecords > 0)
            writer.write(skippedRecords + " partitions was skipped\n");

        if (!F.isEmpty(partitions)) {
            writer.write("Cluster partitions:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : partitions.entrySet()) {
                writer.write("Partition: " + entry.getKey() + "\n");

                writer.write("Partition instances: " + entry.getValue() + "\n");
            }

            writer.write("\n\n-----------------------------------\n\n");

            conflictRes.print(writer::write);
        }
    }

    /**
     * @return Comparator for {@link PartitionHashRecordV2}.
     */
    private Comparator<PartitionHashRecordV2> buildRecordComparator() {
        return (o1, o2) -> {
            int compare = Boolean.compare(o1.isPrimary(), o2.isPrimary());

            if (compare != 0)
                return compare;

            return o1.consistentId().toString().compareTo(o2.consistentId().toString());
        };
    }

    /**
     * @return Comparator for {@link PartitionKeyV2}.
     */
    private Comparator<PartitionKeyV2> buildPartitionKeyComparator() {
        return (o1, o2) -> {
            int compare = Integer.compare(o1.groupId(), o2.groupId());

            if (compare != 0)
                return compare;

            return Integer.compare(o1.partitionId(), o2.partitionId());
        };
    }
}
