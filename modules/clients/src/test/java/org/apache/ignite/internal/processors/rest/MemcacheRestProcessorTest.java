/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest;

import java.net.InetSocketAddress;
import java.util.Map;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class MemcacheRestProcessorTest extends GridCommonAbstractTest {
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