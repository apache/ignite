package org.apache.ignite.internal.commandline.cache;

import java.util.Set;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTask;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskArg;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskResult;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

public class ResetLostPartitions extends Command<Set<String>> {
    private Set<String> caches;

    @Override public Set<String> arg() {
        return caches;
    }

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        CacheResetLostPartitionsTaskArg taskArg = new CacheResetLostPartitionsTaskArg(caches);

        try (GridClient client = startClient(clientCfg)) {
            CacheResetLostPartitionsTaskResult res =
                executeTaskByNameOnNode(client, CacheResetLostPartitionsTask.class.getName(), taskArg, null, clientCfg);

            res.print(System.out);

            return res;
        }
    }

    @Override public void parseArguments(CommandArgIterator argIter) {
        caches = argIter.nextStringSet("Cache names");
    }
}
