package org.apache.ignite.internal.commandline.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.visor.verify.IndexIntegrityCheckIssue;
import org.apache.ignite.internal.visor.verify.IndexValidationIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.j;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;

public class CacheValidateIndexes extends Command<CacheValidateIndexes.Arguments> {
    /** Validate indexes task name. */
    private static final String VALIDATE_INDEXES_TASK = "org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask";

    public class Arguments {
         /** Caches. */
        private Set<String> caches;

        /** Node id. */
        private UUID nodeId;

        /** validate_indexes 'checkFirst' argument */
        private int checkFirst = -1;

        /** validate_indexes 'checkThrough' argument */
        private int checkThrough = -1;

        public Arguments(Set<String> caches, UUID nodeId, int checkFirst, int checkThrough) {
            this.caches = caches;
            this.nodeId = nodeId;
            this.checkFirst = checkFirst;
            this.checkThrough = checkThrough;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Max number of entries to be checked.
         */
        public int checkFirst() {
            return checkFirst;
        }

        /**
         * @return Number of entries to check through.
         */
        public int checkThrough() {
            return checkThrough;
        }


        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }
    }

    private Arguments args;

    @Override public Arguments arg() {
        return args;
    }

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        VisorValidateIndexesTaskArg taskArg = new VisorValidateIndexesTaskArg(
            args.caches(),
            args.nodeId() != null ? Collections.singleton(args.nodeId()) : null,
            args.checkFirst(),
            args.checkThrough()
        );

        try (GridClient client = startClient(clientCfg);) {
            VisorValidateIndexesTaskResult taskRes = executeTaskByNameOnNode(
                client, VALIDATE_INDEXES_TASK, taskArg, null, clientCfg);

            boolean errors = logger.printErrors(taskRes.exceptions(), "Index validation failed on nodes:");

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

            return taskRes;
        }
    }

    @Override public void parseArguments(CommandArgIterator argIter) {
        int checkFirst = -1;
        int checkThrough = -1;
        UUID nodeId = null;
        Set<String> caches = null;

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
                    checkFirst = numVal;
                else
                    checkThrough = numVal;

                continue;
            }

            try {
                nodeId = UUID.fromString(nextArg);

                continue;
            }
            catch (IllegalArgumentException ignored) {
                //No-op.
            }

            caches = argIter.parseStringSet(nextArg);
        }

        args = new Arguments(caches, nodeId, checkFirst, checkThrough);
    }
}
