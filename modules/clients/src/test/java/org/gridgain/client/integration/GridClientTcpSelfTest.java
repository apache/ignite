/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.util.typedef.*;

import java.nio.*;

/**
 * Tests TCP protocol.
 */
public class GridClientTcpSelfTest extends GridClientAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return HOST + ":" + BINARY_PORT;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return null;
    }

    /**
     * Checks if incorrect marshaller configuration leads to
     * handshake error.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeFailed() throws Exception {
        GridClientConfiguration cfg = clientConfiguration();

        cfg.setMarshaller(new GridClientMarshaller() {
            @Override public ByteBuffer marshal(Object obj, int off) {
                throw new UnsupportedOperationException();
            }

            @Override public <T> T unmarshal(byte[] bytes) {
                throw new UnsupportedOperationException();
            }
        });

        GridClient c = GridClientFactory.start(cfg);

        Exception err = null;

        try {
            c.compute().refreshTopology(false, false);
        }
        catch (Exception e) {
            err = e;
        }

        assertNotNull(err);
        assertTrue(X.hasCause(err, GridClientHandshakeException.class));
    }
}
