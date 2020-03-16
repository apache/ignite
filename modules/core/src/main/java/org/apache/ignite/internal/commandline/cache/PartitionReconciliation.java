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

package org.apache.ignite.internal.commandline.cache;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTask;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.PARTITION_RECONCILIATION;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.BATCH_SIZE;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.FAST_CHECK;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.INCLUDE_SENSITIVE;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.LOCAL_OUTPUT;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.PARALLELISM;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.RECHECK_ATTEMPTS;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.RECHECK_DELAY;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.REPAIR;

/**
 * Partition reconciliation command.
 */
public class PartitionReconciliation implements Command<PartitionReconciliation.Arguments> {
    /** Parallelism format error message. */
    public static final String PARALLELISM_FORMAT_MESSAGE = "The positive integer should be specified, " +
        "or 0 (number of cores on a server node will be used as parallelism in such case). " +
        "If the given value is greater than the number of cores on a server node, " +
        "the behavior will be equal to the case when 0 is specified.";

    /** Batch size format error message. */
    public static final String BATCH_SIZE_FORMAT_MESSAGE = "Invalid batch size: %s" +
        ". Integer value greater than zero should be used.";

    /** Recheck attempts format error message. */
    public static final String RECHECK_ATTEMPTS_FORMAT_MESSAGE = "Invalid recheck attempts: %s" +
        ". Integer value between 1 (inclusive) and 5 (exclusive) should be used.";

    /** Recheck delay format error message. */
    public static final String RECHECK_DELAY_FORMAT_MESSAGE = "Invalid recheck delay: %s" +
        ". Integer value between 0 (inclusive) and 100 (exclusive) should be used.";

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String caches = "cacheName1,...,cacheNameN";

        String desc = "Verify whether there are inconsistent entries for the specified caches " +
            "and print out the differences if any. Fix inconsistency if " + REPAIR + "argument is presented. " +
            "When no parameters are specified, " +
            "all user caches are verified. Cache filtering options configure the set of caches that will be " +
            "processed by " + PARTITION_RECONCILIATION + " command. If cache names are specified, in form of regular " +
            "expressions, only matching caches will be verified.";

        Map<String, String> paramsDesc = new HashMap<>();

        paramsDesc.put(FAST_CHECK.toString(),
            "This option allows checking and repairing only partitions that did not pass validation" +
                " during the last partition map exchnage, otherwise all partitions will ba taken into account.");

        paramsDesc.put(REPAIR.toString(),
            "If present, fix all inconsistent data. Specifies which repair algorithm to use for doubtful keys. The following values can be used: "
                + Arrays.toString(RepairAlgorithm.values()) + ". Default value is " + REPAIR.defaultValue() + '.');

        paramsDesc.put(PARALLELISM.toString(),
            "Maximum number of threads that can be involved in partition reconciliation activities on one node. " +
                "Default value equals number of cores.");

        paramsDesc.put(BATCH_SIZE.toString(),
            "Amount of keys to retrieve within one job. Default value is " + BATCH_SIZE.defaultValue() + '.');

        paramsDesc.put(RECHECK_ATTEMPTS.toString(),
            "Amount of potentially inconsistent keys recheck attempts. Value between 1 (inclusive) and 5 (exclusive) should be used." +
                " Default value is " + RECHECK_ATTEMPTS.defaultValue() + '.');

        paramsDesc.put(INCLUDE_SENSITIVE.toString(),
            "Print data to result with sensitive information: keys and values." +
                " Default value is " + INCLUDE_SENSITIVE.defaultValue() + '.');

        // RECHECK_DELAY arg intentionally skipped.

        usageCache(
            log,
            PARTITION_RECONCILIATION,
            desc,
            paramsDesc,
            optional(REPAIR),
            optional(FAST_CHECK),
            optional(PARALLELISM),
            optional(BATCH_SIZE),
            optional(RECHECK_ATTEMPTS),
            optional(INCLUDE_SENSITIVE),
            optional(caches));
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PARTITION_RECONCILIATION.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            return partitionReconciliationCheck(client, clientCfg, log);
        }
        catch (Throwable e) {
            log.severe("Failed to execute partition reconciliation command " + CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * Prepares arguments, executes partition reconciliation task, prints logs and optionally fix inconsistency.
     *
     * @param client Client node to run initial task.
     * @param clientCfg Client configuration.
     * @param log Logger.
     * @return Result of operation.
     * @throws GridClientException If failed.
     */
    private ReconciliationResult partitionReconciliationCheck(
        GridClient client,
        GridClientConfiguration clientCfg,
        Logger log
    ) throws GridClientException {
        VisorPartitionReconciliationTaskArg taskArg = new VisorPartitionReconciliationTaskArg(
            args.caches,
            args.fastCheck,
            args.repair,
            args.includeSensitive,
            args.locOutput,
            args.parallelism,
            args.batchSize,
            args.recheckAttempts,
            args.repairAlg,
            args.recheckDelay
        );

        List<GridClientNode> unsupportedSrvNodes = client.compute().nodes().stream()
            .filter(node -> Objects.equals(node.attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE), false))
            .filter(node -> !nodeSupports((byte[])node.attribute(ATTR_IGNITE_FEATURES), IgniteFeatures.PARTITION_RECONCILIATION))
            .collect(Collectors.toList());

        if (!unsupportedSrvNodes.isEmpty()) {
            final String strErrReason = "Partition reconciliation was rejected. The node [id=%s, consistentId=%s] doesn't support this feature.";

            List<String> errs = unsupportedSrvNodes.stream()
                .map(n -> String.format(strErrReason, n.nodeId(), n.consistentId()))
                .collect(toList());

            print(new ReconciliationResult(new ReconciliationAffectedEntries(), new HashMap<>(), errs), log::info);

            throw new IgniteException("There are server nodes not supported partition reconciliation.");
        }
        else {
            ReconciliationResult res =
                executeTask(client, VisorPartitionReconciliationTask.class, taskArg, clientCfg);

            print(res, log::info);

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        boolean repair = false;
        boolean fastCheck = (boolean)FAST_CHECK.defaultValue();
        boolean includeSensitive = (boolean)INCLUDE_SENSITIVE.defaultValue();
        boolean locOutput = (boolean)LOCAL_OUTPUT.defaultValue();
        int parallelism = (int)PARALLELISM.defaultValue();
        int batchSize = (int)BATCH_SIZE.defaultValue();
        int recheckAttempts = (int)RECHECK_ATTEMPTS.defaultValue();
        RepairAlgorithm repairAlg = (RepairAlgorithm)REPAIR.defaultValue();
        int recheckDelay = (int)RECHECK_DELAY.defaultValue();

        int partReconciliationArgsCnt = 8;

        while (argIter.hasNextSubArg() && partReconciliationArgsCnt-- > 0) {
            String nextArg = argIter.nextArg("");

            PartitionReconciliationCommandArg arg =
                CommandArgUtils.of(nextArg, PartitionReconciliationCommandArg.class);

            if (arg == null) {
                cacheNames = argIter.parseStringSet(nextArg);

                validateRegexps(cacheNames);
            }
            else {
                String strVal;

                switch (arg) {
                    case REPAIR:
                        repair = true;

                        String peekedNextArg = argIter.peekNextArg();

                        if (!PartitionReconciliationCommandArg.args().contains(peekedNextArg)) {
                            strVal = argIter.nextArg(
                                "The repair algorithm should be specified. The following " +
                                    "values can be used: " + Arrays.toString(RepairAlgorithm.values()) + '.');

                            try {
                                repairAlg = RepairAlgorithm.valueOf(strVal);
                            }
                            catch (IllegalArgumentException e) {
                                throw new IllegalArgumentException(
                                    "Invalid repair algorithm: " + strVal + ". The following " +
                                        "values can be used: " + Arrays.toString(RepairAlgorithm.values()) + '.');
                            }
                        }

                        break;

                    case FAST_CHECK:
                        fastCheck = true;

                        break;

                    case INCLUDE_SENSITIVE:
                        includeSensitive = true;

                        break;

                    case LOCAL_OUTPUT:
                        locOutput = true;

                        break;

                    case PARALLELISM:
                        strVal = argIter.nextArg("The parallelism level should be specified.");

                        try {
                            parallelism = Integer.parseInt(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException(String.format(PARALLELISM_FORMAT_MESSAGE, strVal));
                        }

                        if (parallelism < 0)
                            throw new IllegalArgumentException(String.format(PARALLELISM_FORMAT_MESSAGE, strVal));

                        break;

                    case BATCH_SIZE:
                        strVal = argIter.nextArg("The batch size should be specified.");

                        try {
                            batchSize = Integer.parseInt(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException(String.format(BATCH_SIZE_FORMAT_MESSAGE, strVal));
                        }

                        if (batchSize <= 0)
                            throw new IllegalArgumentException(String.format(BATCH_SIZE_FORMAT_MESSAGE, strVal));

                        break;

                    case RECHECK_ATTEMPTS:
                        strVal = argIter.nextArg("The recheck attempts should be specified.");

                        try {
                            recheckAttempts = Integer.parseInt(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException(String.format(RECHECK_ATTEMPTS_FORMAT_MESSAGE, strVal));
                        }

                        if (recheckAttempts < 1 || recheckAttempts > 5)
                            throw new IllegalArgumentException(String.format(RECHECK_ATTEMPTS_FORMAT_MESSAGE, strVal));

                        break;

                    case RECHECK_DELAY:
                        strVal = argIter.nextArg("The recheck delay should be specified.");

                        try {
                            recheckDelay = Integer.parseInt(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException(String.format(RECHECK_DELAY_FORMAT_MESSAGE, strVal));
                        }

                        if (recheckDelay < 0 || recheckDelay > 100)
                            throw new IllegalArgumentException(String.format(RECHECK_DELAY_FORMAT_MESSAGE, strVal));

                        break;
                }
            }
        }

        args = new Arguments(
            cacheNames,
            repair,
            fastCheck,
            includeSensitive,
            locOutput,
            parallelism,
            batchSize,
            recheckAttempts,
            repairAlg,
            recheckDelay);
    }

    /**
     * @param str To validate that given name is valid regexp.
     */
    private void validateRegexps(Set<String> str) {
        str.forEach(s -> {
            try {
                Pattern.compile(s);
            }
            catch (PatternSyntaxException e) {
                throw new IgniteException(format("Invalid cache name regexp '%s': %s", s, e.getMessage()));
            }
        });
    }

    /**
     * @return String with meta information about current run of partition-reconciliation: used arguments, params, etc.
     */
    private String prepareHeaderMeta() {
        SB options = new SB("partition-reconciliation task was executed with the following args: ");

        options
            .a("caches=[")
            .a(args.caches() == null ? "" : String.join(", ", args.caches()))
            .a("], repair=[" + args.repair)
            .a("], fast-check=[" + args.fastCheck)
            .a("], includeSensitive=[" + args.includeSensitive)
            .a("], parallelism=[" + args.parallelism)
            .a("], batch-size=[" + args.batchSize)
            .a("], recheck-attempts=[" + args.recheckAttempts)
            .a("], fix-alg=[" + args.repairAlg + "]")
            .a("], recheck-delay=[" + args.recheckDelay + "]")
            .a(System.lineSeparator());

        if (args.includeSensitive) {
            options
                .a("WARNING: Please be aware that sensitive data will be printed to the console and output file(s).")
                .a(System.lineSeparator());
        }

        return options.toString();
    }

    /**
     * @param nodeIdsToFolders Mapping node id to the directory to be used for inconsistency report.
     * @param nodesIdsToConsistenceIdsMap Mapping node id to consistent id.
     * @return String with folder of results and their locations.
     */
    private String prepareResultFolders(
        Map<UUID, String> nodeIdsToFolders,
        Map<UUID, String> nodesIdsToConsistenceIdsMap
    ) {
        SB out = new SB("partition-reconciliation task prepared result where line is " +
            "- <nodeConsistentId>, <nodeId> : <folder> \n");

        for (Map.Entry<UUID, String> entry : nodeIdsToFolders.entrySet()) {
            String consId = nodesIdsToConsistenceIdsMap.get(entry.getKey());

            out
                .a(consId + " " + entry.getKey() + " : " + (entry.getValue() == null ?
                    "All keys on this node are consistent: report wasn't generated." : entry.getValue()))
                .a("\n");
        }

        return out.toString();
    }

    /**
     * Print partition reconciliation output.
     *
     * @param res Partition reconciliation result.
     * @param printer Printer.
     */
    private void print(ReconciliationResult res, Consumer<String> printer) {
        ReconciliationAffectedEntries reconciliationRes = res.partitionReconciliationResult();

        printer.accept(prepareHeaderMeta());

        printer.accept(prepareErrors(res.errors()));

        printer.accept(prepareResultFolders(res.nodeIdToFolder(), reconciliationRes.nodesIdsToConsistenceIdsMap()));

        reconciliationRes.print(printer, args.includeSensitive);
    }

    /**
     * Returns string representation of the given list of errors.
     *
     * @param errors List of errors.
     * @return String representation of the given list of errors.
     */
    private String prepareErrors(List<String> errors) {
        SB errorMsg = new SB();

        if (!errors.isEmpty()) {
            errorMsg
                .a("The following errors occurred during the execution of partition reconciliation:")
                .a(System.lineSeparator());

            for (int i = 0; i < errors.size(); i++)
                errorMsg.a(i + 1).a(". ").a(errors.get(i)).a(System.lineSeparator());
        }

        return errorMsg.toString();
    }

    /**
     * Container for command arguments.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    protected static class Arguments {
        /** List of caches to be checked. */
        private final Set<String> caches;

        /** Flag indicates that an inconsistency should be fixed in accordance with {@link RepairAlgorithm} parameter. */
        private final boolean repair;

        /** Flag indicates that only subset of partitions should be checked and repaired. */
        private final boolean fastCheck;

        /** Flag indicates that the result should include sensitive information such as key and value. */
        private final boolean includeSensitive;

        /** Flag indicates that the result is printed to the console. */
        private final boolean locOutput;

        /** Maximum number of threads that can be involved in reconciliation activities. */
        private final int parallelism;

        /** Amount of keys to checked at one time. */
        private final int batchSize;

        /** Amount of potentially inconsistent keys recheck attempts. */
        private final int recheckAttempts;

        /** Partition reconciliation repair algorithm to be used. */
        private final RepairAlgorithm repairAlg;

        /** Recheck delay in seconds. */
        private int recheckDelay;

        /**
         * Constructor.
         *
         * @param caches Caches.
         * @param repair Fix inconsistency if {@code true}.
         * @param fastCheck If {@code true} then only partitions that did not pass validation
         *                  on the last partition map exchange will be checked and repaired.
         *                  Otherwise, all partitions will be taken into account.
         * @param includeSensitive Print key and value to result log if {@code true}.
         * @param locOutput Print result to local console.
         * @param parallelism Maximum number of threads that can be involved in reconciliation activities.
         * @param batchSize Batch size.
         * @param recheckAttempts Amount of recheck attempts.
         * @param repairAlg Partition reconciliation repair algorithm to be used.
         * @param recheckDelay Recheck delay in seconds.
         */
        public Arguments(
            Set<String> caches,
            boolean repair,
            boolean fastCheck,
            boolean includeSensitive,
            boolean locOutput,
            int parallelism,
            int batchSize,
            int recheckAttempts,
            RepairAlgorithm repairAlg,
            int recheckDelay
        ) {
            this.caches = caches;
            this.repair = repair;
            this.fastCheck = fastCheck;
            this.includeSensitive = includeSensitive;
            this.locOutput = locOutput;
            this.parallelism = parallelism;
            this.batchSize = batchSize;
            this.recheckAttempts = recheckAttempts;
            this.repairAlg = repairAlg;
            this.recheckDelay = recheckDelay;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Fix mode.
         */
        public boolean repair() {
            return repair;
        }

        /**
         * @return {@code true} if only partitions that did not pass validation during the last partition map exchange
         * will be checked and repaired.
         */
        public boolean fastCheck() {
            return fastCheck;
        }

        /**
         * @return Maximum number of threads that can be involved in reconciliation activities.
         */
        public int parallelism() {
            return parallelism;
        }

        /**
         * @return Batch size.
         */
        public int batchSize() {
            return batchSize;
        }

        /**
         * @return Recheck attempts.
         */
        public int recheckAttempts() {
            return recheckAttempts;
        }

        /**
         * @return Include sensitive.
         */
        public boolean includeSensitive() {
            return includeSensitive;
        }

        /**
         * @return Print to console.
         */
        public boolean locOutput() {
            return locOutput;
        }

        /**
         * @return Repair alg.
         */
        public RepairAlgorithm repairAlg() {
            return repairAlg;
        }

        /**
         * @return Recheck delay.
         */
        public int recheckDelay() {
            return recheckDelay;
        }
    }
}
