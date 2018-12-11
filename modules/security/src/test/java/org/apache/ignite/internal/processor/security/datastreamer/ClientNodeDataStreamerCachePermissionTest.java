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

package org.apache.ignite.internal.processor.security.datastreamer;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cache permissions for Data Streamer on Client node.
 */
public class ClientNodeDataStreamerCachePermissionTest extends AbstractSecurityTest {
    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE = "FORBIDDEN_TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server", allowAllPermissionSet()).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration().setName(CACHE_NAME),
                new CacheConfiguration().setName(FORBIDDEN_CACHE)
            );
    }

    /**
     * @return Node client mode.
     */
    protected boolean isClient() {
        return true;
    }

    /**
     * @throws Exception If fail.
     */
    public void test() throws Exception {
        Ignite node = startGrid("test_node",
            builder().defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, SecurityPermission.CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, SecurityPermission.CACHE_READ)
                .build(), isClient());

        allowed(node, s -> s.addData("k", 1));
        forbidden(node, s -> s.addData("k", 1));

        allowed(node, s -> s .addData(singletonMap("key", 2)));
        forbidden(node, s -> s.addData(singletonMap("key", 2)));

        Map.Entry<String, Integer> entry = entry();

        allowed(node, s -> s.addData(entry));
        forbidden(node, s -> s.addData(entry));

        allowed(node, s -> s.addData(singletonList(entry())));
        forbidden(node, s -> s.addData(singletonList(entry())));

    }

    /**
     * @param node Node.
     * @param c Consumer.
     */
    private void allowed(Ignite node, Consumer<IgniteDataStreamer<String, Integer>> c) {
        try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(CACHE_NAME)) {
            c.accept(s);
        }
    }

    /**
     * @param node Node.
     * @param c Consumer.
     */
    private void forbidden(Ignite node, Consumer<IgniteDataStreamer<String, Integer>> c) {
        try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(FORBIDDEN_CACHE)) {
            c.accept(s);

            fail("Should not happen");
        }
        catch (Throwable e) {
            assertThat(X.cause(e, SecurityException.class), notNullValue());
        }
    }
}
