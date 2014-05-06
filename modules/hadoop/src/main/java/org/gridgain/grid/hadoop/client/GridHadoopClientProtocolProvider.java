/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop.client;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.protocol.*;
import org.gridgain.client.*;
import org.gridgain.client.marshaller.optimized.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.hadoop.client.GridHadoopClientProtocol.*;

/**
 * Grid Hadoop client protocol provider.
 */
@SuppressWarnings("UnusedDeclaration")
public class GridHadoopClientProtocolProvider extends ClientProtocolProvider {
    /** {@inheritDoc} */
    @Override public ClientProtocol create(Configuration conf) throws IOException {
        if (GridHadoopClientProtocol.PROP_FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
            String host = conf.get(PROP_SRV_HOST);
            int port = conf.getInt(PROP_SRV_PORT, DFLT_SRV_PORT);

            return create0(host, port, conf);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ClientProtocol create(InetSocketAddress addr, Configuration conf) throws IOException {
        if (GridHadoopClientProtocol.PROP_FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME)))
            return create0(addr.getHostName(), addr.getPort(), conf);

        return null;
    }

    /**
     * Internal protocol creation routine.
     *
     * @param host Host.
     * @param port Port.
     * @param conf Configuration.
     * @return Client protocol.
     * @throws IOException If failed.
     */
    private ClientProtocol create0(String host, int port, Configuration conf) throws IOException {
        String addr = host + ":" + port;

        try {
            // TODO: Client caching (Like in YARNRunner)? Static?
            GridClientConfiguration cliCfg = new GridClientConfiguration();

            cliCfg.setProtocol(GridClientProtocol.TCP);
            cliCfg.setServers(Collections.singletonList(addr));
            cliCfg.setMarshaller(new GridClientOptimizedMarshaller());

            GridClient cli = GridClientFactory.start(cliCfg);

            return new GridHadoopClientProtocol(conf, cli);
        }
        catch (GridClientException e) {
            throw new IOException("Failed to establish connection with GridGain node: " + addr, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close(ClientProtocol cliProto) throws IOException {
        assert cliProto instanceof GridHadoopClientProtocol;

        ((GridHadoopClientProtocol)cliProto).close();
    }
}
