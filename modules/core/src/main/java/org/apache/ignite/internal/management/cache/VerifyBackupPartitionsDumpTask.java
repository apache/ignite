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
package org.apache.ignite.internal.management.cache;

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
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task for collection checksums primary and backup partitions of specified caches. <br> Argument: Set of cache names,
 * 'null' will trigger verification for all caches. <br> Result: {@link IdleVerifyDumpResult} with all found partitions.
 * <br> Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsDumpTask extends ComputeTaskAdapter<CacheIdleVerifyCommandArg, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");

    /** Visible for testing. */
    public static final String IDLE_DUMP_FILE_PREFIX = "idle-dump-";

    /** Delegate for map execution */
    private final VerifyBackupPartitionsTask delegate = new VerifyBackupPartitionsTask();

    /** */
    private CacheIdleVerifyDumpCommandArg taskArg;

    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        CacheIdleVerifyCommandArg arg
    ) throws IgniteException {
        if (arg instanceof CacheIdleVerifyDumpCommandArg)
            taskArg = (CacheIdleVerifyDumpCommandArg)arg;

        return delegate.map(subgrid, arg);
    }

    /** {@inheritDoc} */
    @Override public @Nullable String reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<PartitionKey, List<PartitionHashRecord>> clusterHashes = new TreeMap<>(buildPartitionKeyComparator());

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                continue;

            Map<PartitionKey, PartitionHashRecord> nodeHashes = res.getData();

            for (Map.Entry<PartitionKey, PartitionHashRecord> e : nodeHashes.entrySet()) {
                clusterHashes
                    .computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                    .add(e.getValue());
            }
        }

        Comparator<PartitionHashRecord> recordComp = buildRecordComparator().reversed();

        Map<PartitionKey, List<PartitionHashRecord>> partitions = new LinkedHashMap<>();

        int skippedRecords = 0;

        for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : clusterHashes.entrySet()) {
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
    private boolean needToAdd(List<PartitionHashRecord> records) {
        if (records.isEmpty() || (taskArg != null && !taskArg.skipZeros()))
            return true;

        PartitionHashRecord record = records.get(0);

        if (record.size() != 0)
            return true;

        int firstHash = record.partitionHash();
        int firstVerHash = record.partitionVersionsHash();

        for (int i = 1; i < records.size(); i++) {
            record = records.get(i);

            if (record.partitionHash() != firstHash
                || record.partitionVersionsHash() != firstVerHash
                || record.size() != 0
            )
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
        Map<PartitionKey, List<PartitionHashRecord>> partitions,
        IdleVerifyResult conflictRes,
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
        Map<PartitionKey, List<PartitionHashRecord>> partitions,
        IdleVerifyResult conflictRes,
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

            writer.write("The check procedure failed on " + size + " node" + (size == 1 ? "" : "s") + ".\n");

            if (noMatchingCaches)
                writer.write("There are no caches matching given filter options.");
        }

        if (!partitions.isEmpty())
            writer.write("The check procedure has finished, found " + partitions.size() + " partitions\n");

        logParsedArgs(taskArg, writer::write);

        if (skippedRecords > 0)
            writer.write(skippedRecords + " partitions was skipped\n");

        if (!F.isEmpty(partitions)) {
            writer.write("Cluster partitions:\n");

            long cf = 0;
            long noCf = 0;
            long binary = 0;
            long regular = 0;

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : partitions.entrySet()) {
                writer.write("Partition: " + entry.getKey() + "\n");

                writer.write("Partition instances: " + entry.getValue() + "\n");

                if (!entry.getValue().isEmpty()) {
                    PartitionHashRecord rec = entry.getValue().get(0);

                    cf += rec.compactFooterKeys();
                    noCf += rec.noCompactFooterKeys();
                    binary += rec.binaryKeys();
                    regular += rec.regularKeys();
                }
            }

            writer.write("\n\n-----------------------------------\n\n");

            writer.write("CompactFooter statistic for keys [compactFooter=" + cf +
                ", noCompactFooter=" + noCf +
                ", binary=" + binary +
                ", regular=" + regular + "]\n\n");

            conflictRes.print(writer::write, true);
        }
    }

    /**
     * @return Comparator for {@link PartitionHashRecord}.
     */
    private Comparator<PartitionHashRecord> buildRecordComparator() {
        return (o1, o2) -> {
            int compare = Boolean.compare(o1.isPrimary(), o2.isPrimary());

            if (compare != 0)
                return compare;

            return o1.consistentId().toString().compareTo(o2.consistentId().toString());
        };
    }

    /**
     * @return Comparator for {@link PartitionKey}.
     */
    private Comparator<PartitionKey> buildPartitionKeyComparator() {
        return (o1, o2) -> {
            int compare = Integer.compare(o1.groupId(), o2.groupId());

            if (compare != 0)
                return compare;

            return Integer.compare(o1.partitionId(), o2.partitionId());
        };
    }

    /**
     * Passes idle_verify parsed arguments to given log consumer.
     *
     * @param args idle_verify arguments.
     * @param logConsumer Logger.
     */
    public static void logParsedArgs(CacheIdleVerifyCommandArg args, Consumer<String> logConsumer) {
        SB options = new SB("The check procedure task was executed with the following args: ");

        options
            .a("caches=[")
            .a(args.caches() == null ? "" : String.join(", ", args.caches()))
            .a("], excluded=[")
            .a(args.excludeCaches() == null ? "" : String.join(", ", args.excludeCaches()))
            .a("]")
            .a(", cacheFilter=[")
            .a(args.cacheFilter().toString())
            .a("]\n");

        logConsumer.accept(options.toString());
    }
}
