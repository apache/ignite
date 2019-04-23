package org.apache.ignite.internal.commandline.cache;

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
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTask;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTaskResult;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;

public class FindAndDeleteGarbage extends Command<FindAndDeleteGarbage.Arguments> {
    public static class Arguments {
        /** Groups. */
        private Set<String> groups;

        /** Node id. */
        private UUID nodeId;

        /** Delete garbage flag. */
        private boolean delete;

        public Arguments(Set<String> groups, UUID nodeId, boolean delete) {
            this.groups = groups;
            this.nodeId = nodeId;
            this.delete = delete;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }


        public Set<String> groups() {
            return groups;
        }

        public boolean delete() {
            return delete;
        }
    }


    private Arguments args;

    @Override public Arguments arg() {
        return args;
    }

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        VisorFindAndDeleteGarbargeInPersistenceTaskArg taskArg = new VisorFindAndDeleteGarbargeInPersistenceTaskArg(
            args.groups(),
            args.delete(),
            args.nodeId() != null ? Collections.singleton(args.nodeId()) : null
        );

        try (GridClient client = startClient(clientCfg);) {
            VisorFindAndDeleteGarbargeInPersistenceTaskResult taskRes = executeTask(
                client, VisorFindAndDeleteGarbargeInPersistenceTask.class, taskArg, clientCfg);

            logger.printErrors(taskRes.exceptions(), "Scanning for garbage failed on nodes:");

            for (Map.Entry<UUID, VisorFindAndDeleteGarbargeInPersistenceJobResult> nodeEntry : taskRes.result().entrySet()) {
                if (!nodeEntry.getValue().hasGarbarge()) {
                    logger.log("Node " + nodeEntry.getKey() + " - garbage not found.");

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

            return taskRes;
        }
    }

    @Override public void parseArguments(CommandArgIterator argIter) {
        boolean delete = false;
        UUID nodeId = null;
        Set<String> groups = null;

        int argsCnt = 0;

        while (argIter.hasNextSubArg() && argsCnt++ < 3) {
            String nextArg = argIter.nextArg("");

            FindAndDeleteGarbageArg arg = CommandArgUtils.of(nextArg, FindAndDeleteGarbageArg.class);

            if (arg == FindAndDeleteGarbageArg.DELETE) {
                delete = true;

                continue;
            }

            try {
                nodeId = UUID.fromString(nextArg);

                continue;
            }
            catch (IllegalArgumentException ignored) {
                //No-op.
            }

            groups = argIter.parseStringSet(nextArg);
        }

        args = new Arguments(groups, nodeId, delete);
    }
}
