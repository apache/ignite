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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.OutputFormat;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTask;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskResult;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTask;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskArg;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskResult;
import org.apache.ignite.internal.processors.cache.verify.ContentionInfo;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTaskResult;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.IndexIntegrityCheckIssue;
import org.apache.ignite.internal.visor.verify.IndexValidationIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorContentionTask;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskArg;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskResult;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskResult;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.commandline.CommandArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.CommandHandler.NULL;
import static org.apache.ignite.internal.commandline.CommandHandler.ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG;
import static org.apache.ignite.internal.commandline.CommandLogger.g;
import static org.apache.ignite.internal.commandline.CommandLogger.j;
import static org.apache.ignite.internal.commandline.CommandLogger.op;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.OutputFormat.SINGLE_LINE;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.CONTENTION;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.DISTRIBUTION;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.IDLE_VERIFY;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.LIST;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.RESET_LOST_PARTITIONS;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg.USER_ATTRIBUTES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.DUMP;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.SKIP_ZEROS;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.CONFIG;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.GROUP;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.OUTPUT_FORMAT;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.SEQUENCE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.CACHES;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.GROUPS;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.SEQ;
import static org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder.DELIM;

public class CacheCommands extends Command<CacheArguments> {
    /** */
    private static final String NODE_ID = "nodeId";

    /** */
    private static final String OP_NODE_ID = op(NODE_ID);

    private CommandLogger logger;

    /** Validate indexes task name. */
    private static final String VALIDATE_INDEXES_TASK = "org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask";

    /** Find and delete garbarge task name. */
    private static final String FIND_AND_DELETE_GARBARGE_TASK = "org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTask";

    private CacheArguments cacheArgs;

    /**
     * Executes --cache subcommand.
     *
     * @param clientCfg Client configuration.
     */

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        if (cacheArgs.command() == CacheCommandList.HELP) {
            printCacheHelp();

            return null;
        }

        try (GridClient client = GridClientFactory.start(clientCfg)) {
            switch (cacheArgs.command()) {
                case IDLE_VERIFY:
                    cacheIdleVerify(client, cacheArgs, clientCfg);

                    break;

                case VALIDATE_INDEXES:
                    cacheValidateIndexes(client, cacheArgs, clientCfg);

                    break;

                case FIND_AND_DELETE_GARBAGE:
                    findAndDeleteGarbage(client, cacheArgs, clientCfg);

                    break;

                case CONTENTION:
                    cacheContention(client, cacheArgs, clientCfg);

                    break;

                case DISTRIBUTION:
                    cacheDistribution(client, cacheArgs, clientCfg);

                    break;

                case RESET_LOST_PARTITIONS:
                    cacheResetLostPartitions(client, cacheArgs, clientCfg);

                    break;

                default:
                    new CacheViewer(logger).cacheView(client, cacheArgs, clientCfg);

                    break;
            }
        }

        return null;
    }

   /**
     * Executes appropriate version of idle_verify check. Old version will be used if there are old nodes in the
     * cluster.
     *  @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerify(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        Collection<GridClientNode> nodes = client.compute().nodes(GridClientNode::connectable);

        boolean idleVerifyV2 = true;

        for (GridClientNode node : nodes) {
            String nodeVerStr = node.attribute(IgniteNodeAttributes.ATTR_BUILD_VER);

            IgniteProductVersion nodeVer = IgniteProductVersion.fromString(nodeVerStr);

            if (nodeVer.compareTo(VerifyBackupPartitionsTaskV2.V2_SINCE_VER) < 0) {
                idleVerifyV2 = false;

                break;
            }
        }

        if (cacheArgs.dump())
            cacheIdleVerifyDump(client, cacheArgs, clientCfg);
        else if (idleVerifyV2)
            cacheIdleVerifyV2(client, cacheArgs, clientCfg);
        else
            legacyCacheIdleVerify(client, cacheArgs, clientCfg);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerifyDump(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        VisorIdleVerifyDumpTaskArg arg = new VisorIdleVerifyDumpTaskArg(
            cacheArgs.caches(),
            cacheArgs.excludeCaches(),
            cacheArgs.isSkipZeros(),
            cacheArgs.getCacheFilterEnum(),
            cacheArgs.idleCheckCrc()
        );

        String path = executeTask(client, VisorIdleVerifyDumpTask.class, arg, clientCfg);

        logger.log("VisorIdleVerifyDumpTask successfully written output to '" + path + "'");
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerifyV2(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        IdleVerifyResultV2 res = executeTask(
            client,
            VisorIdleVerifyTaskV2.class,
            new VisorIdleVerifyTaskArg(cacheArgs.caches(), cacheArgs.excludeCaches(), cacheArgs.idleCheckCrc()),
            clientCfg);

        res.print(System.out::print);
    }


    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheValidateIndexes(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        VisorValidateIndexesTaskArg taskArg = new VisorValidateIndexesTaskArg(
            cacheArgs.caches(),
            cacheArgs.nodeId() != null ? Collections.singleton(cacheArgs.nodeId()) : null,
            cacheArgs.checkFirst(),
            cacheArgs.checkThrough()
        );

        VisorValidateIndexesTaskResult taskRes = executeTaskByNameOnNode(
            client, VALIDATE_INDEXES_TASK, taskArg, null, clientCfg);

        boolean errors = printErrors(taskRes.exceptions(), "Index validation failed on nodes:");

        for (Map.Entry<UUID, VisorValidateIndexesJobResult> nodeEntry : taskRes.results().entrySet()) {
            if (!nodeEntry.getValue().hasIssues())
                continue;

            errors = true;

            logger.log("Index issues found on node " + nodeEntry.getKey() + ":");

            Collection<IndexIntegrityCheckIssue> integrityCheckFailures = nodeEntry.getValue().integrityCheckFailures();

            if (!integrityCheckFailures.isEmpty()) {
                for (IndexIntegrityCheckIssue is : integrityCheckFailures)
                    logger.logWithIndent(is);
            }

            Map<PartitionKey, ValidateIndexesPartitionResult> partRes = nodeEntry.getValue().partitionResult();

            for (Map.Entry<PartitionKey, ValidateIndexesPartitionResult> e : partRes.entrySet()) {
                ValidateIndexesPartitionResult res = e.getValue();

                if (!res.issues().isEmpty()) {
                    logger.logWithIndent(j(" ", e.getKey(), e.getValue()));

                    for (IndexValidationIssue is : res.issues())
                        logger.logWithIndent(is, 2);
                }
            }

            Map<String, ValidateIndexesPartitionResult> idxRes = nodeEntry.getValue().indexResult();

            for (Map.Entry<String, ValidateIndexesPartitionResult> e : idxRes.entrySet()) {
                ValidateIndexesPartitionResult res = e.getValue();

                if (!res.issues().isEmpty()) {
                    logger.logWithIndent(j(" ", "SQL Index", e.getKey(), e.getValue()));

                    for (IndexValidationIssue is : res.issues())
                        logger.logWithIndent(is, 2);
                }
            }
        }

        if (!errors)
            logger.log("no issues found.");
        else
            logger.log("issues found (listed above).");

        logger.nl();
    }


    private boolean printErrors(Map<UUID, Exception> exceptions, String s) {
        if (!F.isEmpty(exceptions)) {
            logger.log(s);

            for (Map.Entry<UUID, Exception> e : exceptions.entrySet()) {
                logger.logWithIndent("Node ID: " + e.getKey());

                logger.logWithIndent("Exception message:");
                logger.logWithIndent(e.getValue().getMessage(), 2);
                logger.nl();
            }

            return true;
        }

        return false;
    }


    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheContention(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        VisorContentionTaskArg taskArg = new VisorContentionTaskArg(
            cacheArgs.minQueueSize(), cacheArgs.maxPrint());

        UUID nodeId = cacheArgs.nodeId() == null ? BROADCAST_UUID : cacheArgs.nodeId();

        VisorContentionTaskResult res = executeTaskByNameOnNode(
            client, VisorContentionTask.class.getName(), taskArg, nodeId, clientCfg);

        if (!F.isEmpty(res.exceptions())) {
            logger.log("Contention check failed on nodes:");

            for (Map.Entry<UUID, Exception> e : res.exceptions().entrySet()) {
                logger.log("Node ID: " + e.getKey());

                logger.log("Exception message:");
                logger.log(e.getValue().getMessage());
                logger.nl();
            }
        }

        for (ContentionInfo info : res.getInfos())
            info.print();
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void legacyCacheIdleVerify(GridClient client, CacheArguments cacheArgs, GridClientConfiguration clientCfg) throws GridClientException {
        VisorIdleVerifyTaskResult res = executeTask(
            client,
            VisorIdleVerifyTask.class,
            new VisorIdleVerifyTaskArg(cacheArgs.caches(), cacheArgs.excludeCaches(), cacheArgs.idleCheckCrc()),
            clientCfg);

        Map<PartitionKey, List<PartitionHashRecord>> conflicts = res.getConflicts();

        if (conflicts.isEmpty()) {
            logger.log("idle_verify check has finished, no conflicts have been found.");
            logger.nl();
        }
        else {
            logger.log("idle_verify check has finished, found " + conflicts.size() + " conflict partitions.");
            logger.nl();

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : conflicts.entrySet()) {
                logger.log("Conflict partition: " + entry.getKey());

                logger.log("Partition instances: " + entry.getValue());
            }
        }
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheDistribution(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        CacheDistributionTaskArg taskArg = new CacheDistributionTaskArg(cacheArgs.caches(), cacheArgs.getUserAttributes());

        UUID nodeId = cacheArgs.nodeId() == null ? BROADCAST_UUID : cacheArgs.nodeId();

        CacheDistributionTaskResult res = executeTaskByNameOnNode(client, CacheDistributionTask.class.getName(), taskArg, nodeId, clientCfg);

        res.print(System.out);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void cacheResetLostPartitions(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        CacheResetLostPartitionsTaskArg taskArg = new CacheResetLostPartitionsTaskArg(cacheArgs.caches());

        CacheResetLostPartitionsTaskResult res = executeTaskByNameOnNode(client, CacheResetLostPartitionsTask.class.getName(), taskArg, null, clientCfg);

        res.print(System.out);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param clientCfg Client configuration.
     */
    private void findAndDeleteGarbage(
        GridClient client,
        CacheArguments cacheArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        VisorFindAndDeleteGarbargeInPersistenceTaskArg taskArg = new VisorFindAndDeleteGarbargeInPersistenceTaskArg(
            cacheArgs.groups(),
            cacheArgs.delete(),
            cacheArgs.nodeId() != null ? Collections.singleton(cacheArgs.nodeId()) : null
        );

        VisorFindAndDeleteGarbargeInPersistenceTaskResult taskRes = executeTaskByNameOnNode(
            client, FIND_AND_DELETE_GARBARGE_TASK, taskArg, null, clientCfg);

        printErrors(taskRes.exceptions(), "Scanning for garbage failed on nodes:");

        for (Map.Entry<UUID, VisorFindAndDeleteGarbargeInPersistenceJobResult> nodeEntry : taskRes.result().entrySet()) {
            if (!nodeEntry.getValue().hasGarbarge()) {
                logger.log("Node "+ nodeEntry.getKey() + " - garbage not found.");

                continue;
            }

            logger.log("Garbarge found on node " + nodeEntry.getKey() + ":");

            VisorFindAndDeleteGarbargeInPersistenceJobResult value = nodeEntry.getValue();

            Map<Integer, Map<Integer, Long>> grpPartErrorsCount = value.checkResult();

            if (!grpPartErrorsCount.isEmpty()) {
                for (Map.Entry<Integer, Map<Integer, Long>> entry : grpPartErrorsCount.entrySet()) {
                    for (Map.Entry<Integer, Long> e : entry.getValue().entrySet()) {
                        logger.logWithIndent("Group=" + entry.getKey() +
                            ", partition=" + e.getKey() +
                            ", count of keys=" + e.getValue());
                    }
                }
            }

            logger.nl();
        }
    }

    /** */
    private void printCacheHelp() {
        logger.logWithIndent("The '" + CACHE + " subcommand' is used to get information about and perform actions with caches. The command has the following syntax:");
        logger.nl();
        logger.logWithIndent(CommandLogger.j(" ", CommandHandler.UTILITY_NAME, j(" ", getCommonOptions())) + " " + CACHE + " [subcommand] <subcommand_parameters>");
        logger.nl();
        logger.logWithIndent("The subcommands that take " + OP_NODE_ID + " as an argument ('" + LIST + "', '" + CONTENTION + "' and '" + VALIDATE_INDEXES + "') will be executed on the given node or on all server nodes if the option is not specified. Other commands will run on a random server node.");
        logger.nl();
        logger.nl();
        logger.logWithIndent("Subcommands:");

        String CACHES = "cacheName1,...,cacheNameN";

        String GROUPS = "groupName1,...,groupNameN";

        usageCache(LIST, "regexPattern", op(or(GROUP, SEQUENCE)), OP_NODE_ID, op(CONFIG), op(OUTPUT_FORMAT, MULTI_LINE));
        usageCache(CONTENTION, "minQueueSize", OP_NODE_ID, op("maxPrint"));
        usageCache(IDLE_VERIFY, op(DUMP), op(SKIP_ZEROS), op(CHECK_CRC),
            op(or(g(EXCLUDE_CACHES, CACHES), g(CACHE_FILTER, or(CacheFilterEnum.values())), CACHES)));
        usageCache(VALIDATE_INDEXES, op(CACHES), OP_NODE_ID, op(or(CHECK_FIRST + " N", CHECK_THROUGH + " K")));
        usageCache(DISTRIBUTION, or(NODE_ID, CommandHandler.NULL), op(CACHES), op(USER_ATTRIBUTES, "attrName1,...,attrNameN"));
        usageCache(RESET_LOST_PARTITIONS, CACHES);
        usageCache(FIND_AND_DELETE_GARBAGE, op(GROUPS), OP_NODE_ID, op(FindAndDeleteGarbageArg.DELETE));

        logger.nl();
    }


    /**
     * Print cache command usage with default indention.
     *
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    private void usageCache(CacheCommandList cmd, String... args) {
        usageCache(1, cmd, args);
    }

    /**
     * Print cache command usage.
     *
     * @param indentsNum Number of indents.
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    private void usageCache(int indentsNum, CacheCommandList cmd, String... args) {
        logger.logWithIndent(DELIM, indentsNum);
        logger.nl();
        logger.logWithIndent(j(" ", CACHE, cmd, j(" ", args)), indentsNum++);
        logger.nl();
        logger.logWithIndent(cmd.description(), indentsNum);
        logger.nl();

        Map<String, String> paramsDesc = createCacheArgsDesc(cmd);

        if (!paramsDesc.isEmpty()) {
            logger.logWithIndent("Parameters:", indentsNum);

            usageCacheParams(paramsDesc, indentsNum + 1);

            logger.nl();
        }
    }

    /**
     * Print cache command arguments usage.
     *
     * @param paramsDesc Cache command arguments description.
     * @param indentsNum Number of indents.
     */
    private void usageCacheParams(Map<String, String> paramsDesc, int indentsNum) {
        int maxParamLen = paramsDesc.keySet().stream().max(Comparator.comparingInt(String::length)).get().length();

        for (Map.Entry<String, String> param : paramsDesc.entrySet())
            logger.logWithIndent(extendToLen(param.getKey(), maxParamLen) + "  " + "- " + param.getValue(), indentsNum);
    }

    /**
     * Appends spaces to end of input string for extending to needed length.
     *
     * @param s Input string.
     * @param targetLen Needed length.
     * @return String with appended spaces on the end.
     */
    private String extendToLen(String s, int targetLen) {
        assert targetLen >= 0;
        assert s.length() <= targetLen;

        if (s.length() == targetLen)
            return s;

        SB sb = new SB(targetLen);

        sb.a(s);

        for (int i = 0; i < targetLen - s.length(); i++)
            sb.a(" ");

        return sb.toString();
    }


    /**
     * Gets cache command arguments description by cache command.
     *
     * @param cmd Cache command.
     * @return Cache command arguments description.
     */
    private Map<String, String> createCacheArgsDesc(CacheCommandList cmd) {
        Map<String, String> map = U.newLinkedHashMap(16);
        switch (cmd) {
            case LIST:
                map.put(CONFIG.toString(), "print all configuration parameters for each cache.");
                map.put(OUTPUT_FORMAT + " " + MULTI_LINE, "print configuration parameters per line. This option has effect only when used with " + CONFIG + " and without " + op(or(GROUP, SEQUENCE)) + ".");
                map.put(GROUP.toString(), "print information about groups.");
                map.put(SEQUENCE.toString(), "print information about sequences.");

                break;
            case VALIDATE_INDEXES:
                map.put(CHECK_FIRST + " N", "validate only the first N keys");
                map.put(CHECK_THROUGH + " K", "validate every Kth key");

                break;

            case IDLE_VERIFY:
                map.put(CHECK_CRC.toString(), "check the CRC-sum of pages stored on disk before verifying data consistency in partitions between primary and backup nodes.");

                break;
        }

        return map;
    }

    /**
     * Parses and validates cache arguments.
     *
     * @return --cache subcommand arguments in case validation is successful.
     * @param argIter Argument iterator.
     */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            throw new IllegalArgumentException("Arguments are expected for --cache subcommand, " +
                "run '--cache help' for more info.");
        }

        CacheArguments cacheArgs = new CacheArguments();

        String str = argIter.nextArg("").toLowerCase();

        CacheCommandList cmd = CacheCommandList.of(str);

        if (cmd == null)
            cmd = CacheCommandList.HELP;

        cacheArgs.command(cmd);

        switch (cmd) {
            case HELP:
                break;

            case IDLE_VERIFY:
                int idleVerifyArgsCnt = 3;

                while (argIter.hasNextSubArg() && idleVerifyArgsCnt-- > 0) {
                    String nextArg = argIter.nextArg("");

                    IdleVerifyCommandArg arg = CommandArgUtils.of(nextArg, IdleVerifyCommandArg.class);

                    if (arg == null) {
                        if (cacheArgs.excludeCaches() != null || cacheArgs.getCacheFilterEnum() != CacheFilterEnum.ALL)
                            throw new IllegalArgumentException(ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG);

                        cacheArgs.caches(argIter.parseStringSet(nextArg));
                    }
                    else {
                        switch (arg) {
                            case DUMP:
                                cacheArgs.dump(true);

                                break;

                            case SKIP_ZEROS:
                                cacheArgs.skipZeros(true);

                                break;

                            case CHECK_CRC:
                                cacheArgs.idleCheckCrc(true);

                                break;

                            case CACHE_FILTER:
                                if (cacheArgs.caches() != null || cacheArgs.excludeCaches() != null)
                                    throw new IllegalArgumentException(ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG);

                                String filter = argIter.nextArg("The cache filter should be specified. The following " +
                                    "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                                cacheArgs.setCacheFilterEnum(CacheFilterEnum.valueOf(filter.toUpperCase()));

                                break;

                            case EXCLUDE_CACHES:
                                if (cacheArgs.caches() != null || cacheArgs.getCacheFilterEnum() != CacheFilterEnum.ALL)
                                    throw new IllegalArgumentException(ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG);

                                cacheArgs.excludeCaches(argIter.nextStringSet("caches, which will be excluded."));

                                break;
                        }
                    }
                }
                break;

            case CONTENTION:
                cacheArgs.minQueueSize(Integer.parseInt(argIter.nextArg("Min queue size expected")));

                if (argIter.hasNextSubArg())
                    cacheArgs.nodeId(UUID.fromString(argIter.nextArg("")));

                if (argIter.hasNextSubArg())
                    cacheArgs.maxPrint(Integer.parseInt(argIter.nextArg("")));
                else
                    cacheArgs.maxPrint(10);

                break;

            case VALIDATE_INDEXES: {
                int argsCnt = 0;

                while (argIter.hasNextSubArg() && argsCnt++ < 4) {
                    String nextArg = argIter.nextArg("");

                    ValidateIndexesCommandArg arg = CommandArgUtils.of(nextArg, ValidateIndexesCommandArg.class);

                    if (arg == CHECK_FIRST || arg == CHECK_THROUGH) {
                        if (!argIter.hasNextSubArg())
                            throw new IllegalArgumentException("Numeric value for '" + nextArg + "' parameter expected.");

                        int numVal;

                        String numStr = argIter.nextArg("");

                        try {
                            numVal = Integer.parseInt(numStr);
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Not numeric value was passed for '" + nextArg + "' parameter: " + numStr
                            );
                        }

                        if (numVal <= 0)
                            throw new IllegalArgumentException("Value for '" + nextArg + "' property should be positive.");

                        if (arg == CHECK_FIRST)
                            cacheArgs.checkFirst(numVal);
                        else
                            cacheArgs.checkThrough(numVal);

                        continue;
                    }

                    try {
                        cacheArgs.nodeId(UUID.fromString(nextArg));

                        continue;
                    }
                    catch (IllegalArgumentException ignored) {
                        //No-op.
                    }

                    cacheArgs.caches(argIter.parseStringSet(nextArg));
                }

                break;
            }

            case FIND_AND_DELETE_GARBAGE: {
                int argsCnt = 0;

                while (argIter.hasNextSubArg() && argsCnt++ < 3) {
                    String nextArg = argIter.nextArg("");

                    FindAndDeleteGarbageArg arg = CommandArgUtils.of(nextArg, FindAndDeleteGarbageArg.class);

                    if (arg == FindAndDeleteGarbageArg.DELETE) {
                        cacheArgs.delete(true);

                        continue;
                    }

                    try {
                        cacheArgs.nodeId(UUID.fromString(nextArg));

                        continue;
                    }
                    catch (IllegalArgumentException ignored) {
                        //No-op.
                    }

                    cacheArgs.groups(argIter.parseStringSet(nextArg));
                }

                break;
            }

            case DISTRIBUTION:
                String nodeIdStr = argIter.nextArg("Node id expected or null");
                if (!NULL.equals(nodeIdStr))
                    cacheArgs.nodeId(UUID.fromString(nodeIdStr));

                while (argIter.hasNextSubArg()) {
                    String nextArg = argIter.nextArg("");

                    DistributionCommandArg arg = CommandArgUtils.of(nextArg, DistributionCommandArg.class);

                    if (arg == USER_ATTRIBUTES) {
                        nextArg = argIter.nextArg("User attributes are expected to be separated by commas");

                        Set<String> userAttrs = new HashSet<>();

                        for (String userAttribute : nextArg.split(","))
                            userAttrs.add(userAttribute.trim());

                        cacheArgs.setUserAttributes(userAttrs);

                        nextArg = (argIter.hasNextSubArg()) ? argIter.nextArg("") : null;

                    }

                    if (nextArg != null)
                        cacheArgs.caches(argIter.parseStringSet(nextArg));
                }

                break;

            case RESET_LOST_PARTITIONS:
                cacheArgs.caches(argIter.nextStringSet("Cache name"));

                break;

            case LIST:
                cacheArgs.regex(argIter.nextArg("Regex is expected"));

                VisorViewCacheCmd cacheCmd = CACHES;

                OutputFormat outputFormat = SINGLE_LINE;

                while (argIter.hasNextSubArg()) {
                    String nextArg = argIter.nextArg("").toLowerCase();

                    ListCommandArg arg = CommandArgUtils.of(nextArg, ListCommandArg.class);
                    if (arg != null) {
                        switch (arg) {
                            case GROUP:
                                cacheCmd = GROUPS;

                                break;

                            case SEQUENCE:
                                cacheCmd = SEQ;

                                break;

                            case OUTPUT_FORMAT:
                                String tmp2 = argIter.nextArg("output format must be defined!").toLowerCase();

                                outputFormat = OutputFormat.fromConsoleName(tmp2);

                                break;

                            case CONFIG:
                                cacheArgs.fullConfig(true);

                                break;
                        }
                    }
                    else
                        cacheArgs.nodeId(UUID.fromString(nextArg));
                }

                cacheArgs.cacheCommand(cacheCmd);
                cacheArgs.outputFormat(outputFormat);

                break;

            default:
                throw new IllegalArgumentException("Unknown --cache subcommand " + cmd);
        }

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument of --cache subcommand: " + argIter.peekNextArg());

        this.cacheArgs = cacheArgs;
    }

    @Override public CacheArguments arg() {
        return cacheArgs;
    }
}
