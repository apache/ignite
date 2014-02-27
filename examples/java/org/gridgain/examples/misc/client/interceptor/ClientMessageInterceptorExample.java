// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.interceptor;

import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.product.*;

import java.math.*;
import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example creates a client and performs few cache operations using
 * custom object representation, which is transformed by
 * {@link ClientBigIntegerMessageInterceptor}. Check remote node output
 * for transformation reports.
 * <p>
 * Before running this example you must start at least one remote node using
 * {@link ClientMessageInterceptorExampleNodeStartup}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class ClientMessageInterceptorExample {
    /** Grid node address to connect to. */
    private static final String SERVER_ADDRESS = "127.0.0.1";

    /** Example value to send over the REST protocol. */
    private static final BigInteger BIG_VALUE = new BigInteger("1234567890000000000");

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println(">>> Client message interceptor example started.");

        try (GridClient client = createClient()) {
            GridClientData rmtCache = client.data("partitioned");

            System.out.println(">>>");
            System.out.println(">>> Sending BigInteger value over REST as byte array: " + BIG_VALUE);

            rmtCache.put("key", BIG_VALUE.toByteArray());

            Object val = rmtCache.get("key");

            BigInteger obj = new BigInteger((byte[])val);

            assert Arrays.equals(BIG_VALUE.toByteArray(), (byte[])val);

            System.out.println(">>>");
            System.out.println(">>> Received BigInteger value over REST as byte array: " + obj);
        }
    }

    /**
     * This method will create a client with default configuration. Note that this method expects that
     * first node will bind rest binary protocol on default port. It also expects that partitioned cache is
     * configured in grid.
     *
     * @return Client instance.
     * @throws org.gridgain.client.GridClientException If client could not be created.
     */
    private static GridClient createClient() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        GridClientDataConfiguration cacheCfg = new GridClientDataConfiguration();

        // Set remote cache name.
        cacheCfg.setName("partitioned");

        // Set client partitioned affinity for this cache.
        cacheCfg.setAffinity(new GridClientPartitionedAffinity());

        cfg.setDataConfigurations(Collections.singletonList(cacheCfg));

        // Point client to a local node. Note that this server is only used
        // for initial connection. After having established initial connection
        // client will make decisions which grid node to use based on collocation
        // with key affinity or load balancing.
        cfg.setServers(Collections.singletonList(SERVER_ADDRESS + ':' + GridConfiguration.DFLT_TCP_PORT));

        return GridClientFactory.start(cfg);
    }
}
