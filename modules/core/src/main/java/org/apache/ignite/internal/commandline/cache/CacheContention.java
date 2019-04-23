package org.apache.ignite.internal.commandline.cache;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.cache.verify.ContentionInfo;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.verify.VisorContentionTask;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskArg;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskResult;

import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

public class CacheContention extends Command<CacheContention.Arguments> {
    public class Arguments {
        /** Node id. */
        private UUID nodeId;

        /** Min queue size. */
        private int minQueueSize;

        /** Max print. */
        private int maxPrint;

        public Arguments(UUID nodeId, int minQueueSize, int maxPrint) {
            this.nodeId = nodeId;
            this.minQueueSize = minQueueSize;
            this.maxPrint = maxPrint;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Min queue size.
         */
        public int minQueueSize() {
            return minQueueSize;
        }
        /**
         * @return Max print.
         */
        public int maxPrint() {
            return maxPrint;
        }
    }

    private Arguments cacheArgs;

    @Override public Arguments arg() {
        return cacheArgs;
    }

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        VisorContentionTaskArg taskArg = new VisorContentionTaskArg(
            cacheArgs.minQueueSize(), cacheArgs.maxPrint());

        UUID nodeId = cacheArgs.nodeId() == null ? BROADCAST_UUID : cacheArgs.nodeId();

        VisorContentionTaskResult res;

        try (GridClient client = startClient(clientCfg);) {
            res = executeTaskByNameOnNode(client, VisorContentionTask.class.getName(), taskArg, nodeId, clientCfg);
        }

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

        return res;
    }

    @Override public void parseArguments(CommandArgIterator argIter) {
        int minQueueSize = Integer.parseInt(argIter.nextArg("Min queue size expected"));

        UUID nodeId = null;

        if (argIter.hasNextSubArg())
            nodeId = UUID.fromString(argIter.nextArg(""));

        int maxPrint = 10;

        if (argIter.hasNextSubArg())
            maxPrint = Integer.parseInt(argIter.nextArg(""));

        cacheArgs = new Arguments(nodeId, minQueueSize, maxPrint);
    }
}
