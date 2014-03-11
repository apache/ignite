/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.api;

import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example demonstrates use of Java remote client API. To execute
 * this example you should start an instance of {@link ClientCacheExampleNodeStartup}
 * class which will start up a GridGain node with proper configuration.
 * <p>
 * After node has been started this example creates a client and performs several cache
 * puts and executes a test task.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 * <p>
 * Before running this example you must start at least one remote node using
 * {@link ClientCacheExampleNodeStartup}.
 */
@GridOnlyAvailableIn(DATA_GRID)
public class ClientCacheExample {
    /** Grid node address to connect to. */
    private static final String SERVER_ADDRESS = "127.0.0.1";

    /** Count of keys to be stored in this example. */
    public static final int KEYS_CNT = 10;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridClientException If example execution failed.
     */
    public static void main(String[] args) throws GridClientException {
        System.out.println();
        System.out.println(">>> Client cache example started.");

        try (GridClient client = createClient()) {
            // Show grid topology.
            System.out.println(">>> Client created, current grid topology: " + client.compute().nodes());

            // Random node ID.
            final UUID randNodeId = client.compute().nodes().iterator().next().nodeId();

            // Get client projection of grid partitioned cache.
            GridClientData rmtCache = client.data("partitioned");

            Collection<String> keys = new ArrayList<>(KEYS_CNT);

            // Put some values to the cache.
            for (int i = 0; i < KEYS_CNT; i++) {
                String key = String.valueOf(i);

                // Put request will go exactly to the primary node for this key.
                rmtCache.put(key, "val-" + i);

                UUID nodeId = rmtCache.affinity(key);

                System.out.println(">>> Storing key " + key + " on node " + nodeId);

                keys.add(key);
            }

            // Pin a remote node for communication. All further communication
            // on returned projection will happen through this pinned node.
            GridClientData prj = rmtCache.pinNodes(client.compute().node(randNodeId));

            // Request batch from our local node in pinned mode.
            Map<String, Object> vals = prj.getAll(keys);

            for (Map.Entry<String, Object> entry : vals.entrySet())
                System.out.println(">>> Loaded cache entry [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

            // After nodes are pinned the list of pinned nodes may be retrieved.
            System.out.println(">>> Pinned nodes: " + prj.pinnedNodes());

            // Keys may be stored asynchronously.
            GridClientFuture<Boolean> futPut = rmtCache.putAsync(String.valueOf(0), "new value for 0");

            System.out.println(">>> Result of asynchronous put: " + (futPut.get() ? "success" : "failure"));

            Map<UUID, Map<String, String>> keyVals = new HashMap<>();

            // Batch puts are also supported.
            // Here we group key-value pairs by their affinity node ID to ensure
            // the least amount of network trips possible.
            for (int i = 0; i < KEYS_CNT; i++) {
                String key = String.valueOf(i);

                UUID nodeId = rmtCache.affinity(key);

                Map<String, String> m = keyVals.get(nodeId);

                if (m == null)
                    keyVals.put(nodeId, m = new HashMap<>());

                m.put(key, "val-" + i);
            }

            for (Map<String, String> kvMap : keyVals.values())
                // Affinity-aware bulk put operation - it will connect to the
                // affinity node for provided keys.
                rmtCache.putAll(kvMap);

            // Asynchronous batch put is available as well.
            Collection<GridClientFuture<?>> futs = new LinkedList<>();

            for (Map<String, String> kvMap : keyVals.values()) {
                GridClientFuture<?> futPutAll = rmtCache.putAllAsync(kvMap);

                futs.add(futPutAll);
            }

            // Wait for all futures to complete.
            for (GridClientFuture<?> fut : futs)
                fut.get();

            // Of course there's getting value by key functionality.
            String key = String.valueOf(0);

            System.out.println(">>> Value for key " + key + " is " + rmtCache.get(key));

            // Asynchronous gets, too.
            GridClientFuture<String> futVal = rmtCache.getAsync(key);

            System.out.println(">>> Asynchronous value for key " + key + " is " + futVal.get());

            // Multiple values can be fetched at once. Here we batch our get
            // requests by affinity nodes to ensure least amount of network trips.
            for (Map.Entry<UUID, Map<String, String>> nodeEntry : keyVals.entrySet()) {
                UUID nodeId = nodeEntry.getKey();
                Collection<String> keyCol = nodeEntry.getValue().keySet();

                // Since all keys in our getAll(...) call are mapped to the same primary node,
                // grid cache client will pick this node for the request, so we only have one
                // network trip here.
                System.out.println(">>> Values from node [nodeId=" + nodeId + ", values=" + rmtCache.getAll(keyCol) + ']');
            }

            // Multiple values may be retrieved asynchronously, too.
            // Here we retrieve all keys at ones. Since this request
            // will be sent to some grid node, this node may not be
            // the primary node for all keys and additional network
            // trips will have to be made within grid.
            GridClientFuture futVals = rmtCache.getAllAsync(keys);

            System.out.println(">>> Asynchronous values for keys are " + futVals.get());

            // Contents of cache may be removed one by one synchronously.
            // Again, this operation is affinity aware and only the primary
            // node for the key is contacted.
            boolean res = rmtCache.remove(String.valueOf(0));

            System.out.println(">>> Result of removal: " + (res ? "success" : "failure"));

            // ... and asynchronously.
            GridClientFuture<Boolean> futRes = rmtCache.removeAsync(String.valueOf(1));

            System.out.println(">>> Result of asynchronous removal is: " + (futRes.get() ? "success" : "failure"));

            // Multiple entries may be removed at once synchronously...
            rmtCache.removeAll(Arrays.asList(String.valueOf(2), String.valueOf(3)));

            // ... and asynchronously.
            GridClientFuture<?> futResAll = rmtCache.removeAllAsync(Arrays.asList(String.valueOf(3), String.valueOf(4)));

            futResAll.get();

            // Values may also be replaced.
            res = rmtCache.replace(String.valueOf(0), "new value for 0");

            System.out.println(">>> Result for replace for nonexistent key is " + (res ? "success" : "failure"));

            // Asynchronous replace is supported, too.
            futRes = rmtCache.replaceAsync(String.valueOf(0), "newest value for 0");

            System.out.println(">>> Result for asynchronous replace for nonexistent key is " +
                (futRes.get() ? "success" : "failure"));

            // Compare and set are implemented, too.
            res = rmtCache.cas(String.valueOf(0), "new value for 0", null);

            System.out.println(">>> Result for put using cas for key that didn't have value yet is " +
                (res ? "success" : "failure"));

            // CAS can be asynchronous.
            futRes = rmtCache.casAsync(String.valueOf(0), "newest value for 0", "new value for 0");

            System.out.println(">>> Result for put using asynchronous cas is " + (futRes.get() ? "success" : "failure"));

            // It's possible to obtain cache metrics using data client API.
            System.out.println(">>> Cache metrics : " + rmtCache.metrics());

            // Global and per key metrics retrieval can be asynchronous, too.
            GridClientFuture futMetrics = rmtCache.metricsAsync();

            System.out.println(">>> Cache asynchronous metrics : " + futMetrics.get());
        }
    }

    /**
     * This method will create a client configured to interact with cache.
     * Note that this method expects that first node will bind rest binary protocol on default port.
     * It also expects that partitioned cache is configured in grid.
     *
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createClient() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        GridClientDataConfiguration cacheCfg = new GridClientDataConfiguration();

        // Set remote cache name.
        cacheCfg.setName("partitioned");

        // Set client partitioned affinity for this cache.
        cacheCfg.setAffinity(new GridClientPartitionAffinity());

        cfg.setDataConfigurations(Collections.singletonList(cacheCfg));

        // Point client to a local node. Note that this server is only used
        // for initial connection. After having established initial connection
        // client will make decisions which grid node to use based on collocation
        // with key affinity or load balancing.
        cfg.setServers(Collections.singletonList(SERVER_ADDRESS + ':' + GridConfiguration.DFLT_TCP_PORT));

        return GridClientFactory.start(cfg);
    }
}
