package org.apache.ignite.internal.commandline.cache;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTask;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.NULL;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg.USER_ATTRIBUTES;

public class CacheDistribution extends Command<CacheDistribution.Arguments> {
    public class Arguments {
        /** Caches. */
        private Set<String> caches;

        /** Node id. */
        private UUID nodeId;

        /** Additional user attributes in result. Set of attribute names whose values will be searched in ClusterNode.attributes(). */
        private Set<String> userAttributes;

        public Arguments(Set<String> caches, UUID nodeId, Set<String> userAttributes) {
            this.caches = caches;
            this.nodeId = nodeId;
            this.userAttributes = userAttributes;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Additional user attributes in result. Set of attribute names whose values will be searched in ClusterNode.attributes().
         */
        public Set<String> getUserAttributes() {
            return userAttributes;
        }
    }

    private Arguments args;

    @Override public Arguments arg() {
        return args;
    }

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        CacheDistributionTaskArg taskArg = new CacheDistributionTaskArg(args.caches(), args.getUserAttributes());

        UUID nodeId = args.nodeId() == null ? BROADCAST_UUID : args.nodeId();

        CacheDistributionTaskResult res;

        try(GridClient client = startClient(clientCfg)) {
            res = executeTaskByNameOnNode(client, CacheDistributionTask.class.getName(), taskArg, nodeId, clientCfg);
        }

        res.print(System.out);

        return res;
    }

    @Override public void parseArguments(CommandArgIterator argIter) {
        UUID nodeId = null;
        Set<String> caches = null;
        Set<String> userAttributes = null;


        String nodeIdStr = argIter.nextArg("Node id expected or null");

        if (!NULL.equals(nodeIdStr))
            nodeId= UUID.fromString(nodeIdStr);


        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            DistributionCommandArg arg = CommandArgUtils.of(nextArg, DistributionCommandArg.class);

            if (arg == USER_ATTRIBUTES) {
                nextArg = argIter.nextArg("User attributes are expected to be separated by commas");

                userAttributes = new HashSet<>();

                for (String userAttribute : nextArg.split(","))
                    userAttributes.add(userAttribute.trim());

                nextArg = (argIter.hasNextSubArg()) ? argIter.nextArg("") : null;

            }

            if (nextArg != null)
                caches = argIter.parseStringSet(nextArg);
        }

        args = new Arguments(caches, nodeId, userAttributes);
    }
}
