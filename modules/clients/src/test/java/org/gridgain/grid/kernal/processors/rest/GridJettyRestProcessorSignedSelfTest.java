/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import sun.misc.*;

import java.net.*;
import java.security.*;

/**
 *
 */
public class GridJettyRestProcessorSignedSelfTest extends GridJettyRestProcessorAbstractSelfTest {
    /** */
    protected static final String REST_SECRET_KEY = "secret-key";

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getClientConnectionConfiguration() != null;

        cfg.getClientConnectionConfiguration().setRestSecretKey(REST_SECRET_KEY);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int restPort() {
        return 8092;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnauthorized() throws Exception {
        String addr = "http://" + LOC_HOST + ":" + restPort() + "/gridgain?cmd=top";

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        // Request has not been signed.
        conn.connect();

        assert ((HttpURLConnection)conn).getResponseCode() == 401;

        // Request with authentication info.
        addr = "http://" + LOC_HOST + ":" + restPort() + "/gridgain?cmd=top";

        url = new URL(addr);

        conn = url.openConnection();

        conn.setRequestProperty("X-Signature", signature());

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }

    /**
     * @return Signature.
     * @throws Exception If failed.
     */
    @Override protected String signature() throws Exception {
        long ts = U.currentTimeMillis();

        String s = ts + ":" + REST_SECRET_KEY;

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            BASE64Encoder enc = new BASE64Encoder();

            md.update(s.getBytes());

            String hash = enc.encode(md.digest());

            return ts + ":" + hash;
        }
        catch (NoSuchAlgorithmException e) {
            throw new Exception("Failed to create authentication signature.", e);
        }
    }
}
