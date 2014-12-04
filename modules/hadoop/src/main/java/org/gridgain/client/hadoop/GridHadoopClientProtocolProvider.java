/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.protocol.*;
import org.apache.ignite.lang.*;
import org.gridgain.client.*;
import org.gridgain.client.marshaller.optimized.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.client.GridClientProtocol.*;
import static org.gridgain.client.hadoop.GridHadoopClientProtocol.*;


/**
 * Grid Hadoop client protocol provider.
 */
public class GridHadoopClientProtocolProvider extends ClientProtocolProvider {
    /** Clients. */
    private static final ConcurrentHashMap<String, IgniteFuture<GridClient>> cliMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public ClientProtocol create(Configuration conf) throws IOException {
        if (FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
            String addr = conf.get(MRConfig.MASTER_ADDRESS);

            if (F.isEmpty(addr))
                throw new IOException("Failed to create client protocol because server address is not specified (is " +
                    MRConfig.MASTER_ADDRESS + " property set?).");

            if (F.eq(addr, "local"))
                throw new IOException("Local execution mode is not supported, please point " +
                    MRConfig.MASTER_ADDRESS + " to real GridGain node.");

            return createProtocol(addr, conf);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ClientProtocol create(InetSocketAddress addr, Configuration conf) throws IOException {
        if (FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME)))
            return createProtocol(addr.getHostString() + ":" + addr.getPort(), conf);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close(ClientProtocol cliProto) throws IOException {
        // No-op.
    }

    /**
     * Internal protocol creation routine.
     *
     * @param addr Address.
     * @param conf Configuration.
     * @return Client protocol.
     * @throws IOException If failed.
     */
    private static ClientProtocol createProtocol(String addr, Configuration conf) throws IOException {
        return new GridHadoopClientProtocol(conf, client(addr));
    }

    /**
     * Create client.
     *
     * @param addr Endpoint address.
     * @return Client.
     * @throws IOException If failed.
     */
    private static GridClient client(String addr) throws IOException {
        try {
            IgniteFuture<GridClient> fut = cliMap.get(addr);

            if (fut == null) {
                GridFutureAdapter<GridClient> fut0 = new GridFutureAdapter<>();

                IgniteFuture<GridClient> oldFut = cliMap.putIfAbsent(addr, fut0);

                if (oldFut != null)
                    return oldFut.get();
                else {
                    GridClientConfiguration cliCfg = new GridClientConfiguration();

                    cliCfg.setProtocol(TCP);
                    cliCfg.setServers(Collections.singletonList(addr));
                    cliCfg.setMarshaller(new GridClientOptimizedMarshaller());
                    cliCfg.setDaemon(true);

                    try {
                        GridClient cli = GridClientFactory.start(cliCfg);

                        fut0.onDone(cli);

                        return cli;
                    }
                    catch (GridClientException e) {
                        fut0.onDone(e);

                        throw new IOException("Failed to establish connection with GridGain node: " + addr, e);
                    }
                }
            }
            else
                return fut.get();
        }
        catch (GridException e) {
            throw new IOException("Failed to establish connection with GridGain node: " + addr, e);
        }
    }
}
