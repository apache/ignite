/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import net.spy.memcached.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;

/**
 */
public class GridMemcacheRestProcessorTest extends GridCommonAbstractTest {
    /** Client. */
    private MemcachedClientIF client;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = new MemcachedClient(new BinaryConnectionFactory(),
                F.asList(new InetSocketAddress("127.0.0.1", 11211)));

        assert client.flush().get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetBulk() throws Exception {
        assert client.add("key1", 0, 1).get();
        assert client.add("key2", 0, 2).get();

        Map<String, Object> map = client.getBulk("key1", "key2");

        assert map.size() == 2;
        assert map.get("key1").equals(1);
        assert map.get("key2").equals(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppend() throws Exception {
        assert client.add("key", 0, "val").get();

        assert client.append(0, "key", "_1").get();

        assert "val_1".equals(client.get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepend() throws Exception {
        assert client.add("key", 0, "val").get();

        assert client.prepend(0, "key", "1_").get();

        assert "1_val".equals(client.get("key"));
    }
}
