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

package org.apache.ignite.internal.processors.cache;

import java.io.File;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cache metadata exchange when marshaller directory is not shared between server and client.
 */
public class IgniteMarshallerCacheSeparateDirectoryTest extends GridCommonAbstractTest {
    /** */
    public static final String KEY = "key";

    /** */
    public static final String SERVER = "server";

    /** */
    public static final String CLIENT = "client";

    /** */
    private boolean ccfgOnClient;

    /** */
    private boolean ccfgOnServer;

    /** */
    private boolean indexedTypes;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setWorkDirectory(workDir(igniteInstanceName).getAbsolutePath());

        if (igniteInstanceName.equals(SERVER) && ccfgOnServer ||
                igniteInstanceName.equals(CLIENT) && ccfgOnClient) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setBackups(1);

            if (indexedTypes)
                ccfg.setIndexedTypes(String.class, TestClass.class);

            cfg.setCacheConfiguration(ccfg);
        }

        if (igniteInstanceName.equals(CLIENT))
            cfg.setClientMode(true);

        return cfg;
    }

    /** */
    private File workDir(String igniteInstanceName) {
        return new File(U.getIgniteHome() + File.separator + igniteInstanceName + "-work");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(workDir(SERVER));
        U.delete(workDir(CLIENT));

        super.afterTest();
    }

    /** */
    @Test
    public void testRegular() throws Exception {
        run(true, true, false, AccessMode.SERVER, AccessMode.CLIENT);
    }

    /** */
    @Test
    public void testIndexed() throws Exception {
        run(true, true, true, AccessMode.CLIENT, AccessMode.SERVER);
    }

    /** */
    @Test
    public void testIndexedClosure() throws Exception {
        run(true, true, true, AccessMode.CLOSURE, AccessMode.CLOSURE);
    }

    /** */
    @Test
    public void testClientCcfgIndexedGetClosure() throws Exception {
        run(true, false, true, AccessMode.CLIENT, AccessMode.CLOSURE);
    }

    /** */
    @Test
    public void testClientCcfgGetClosure() throws Exception {
        run(true, false, false, AccessMode.CLIENT, AccessMode.CLOSURE);
    }

    /** */
    @Test
    public void testServerCcfgIndexedGetClosure() throws Exception {
        run(false, true, true, AccessMode.CLIENT, AccessMode.CLOSURE);
    }

    /** */
    @Test
    public void testServerCcfgIndexed() throws Exception {
        run(false, true, true, AccessMode.SERVER, AccessMode.CLIENT);
    }

    /** */
    @Test
    public void testClientCcfgIndexedClosure() throws Exception {
        run(true, false, true, AccessMode.CLOSURE, AccessMode.CLOSURE);
    }

    /** */
    @Test
    public void testClientCcfgIndexed() throws Exception {
        run(true, false, false, AccessMode.CLIENT, AccessMode.SERVER);
    }

    /** */
    @Test
    public void testClientCcfgIndexedPutClosure() throws Exception {
        run(false, true, true, AccessMode.CLOSURE, AccessMode.CLIENT);
    }

    /** */
    @Test
    public void testServerCcfgPutClosure() throws Exception {
        run(true, false, false, AccessMode.CLOSURE, AccessMode.SERVER);
    }

    /** */
    private void run(boolean ccfgOnClient, boolean ccfgOnServer, boolean indexedTypes,
                     AccessMode putMode, AccessMode getMode) throws Exception {
        this.ccfgOnClient = ccfgOnClient;
        this.ccfgOnServer = ccfgOnServer;
        this.indexedTypes = indexedTypes;

        Ignite server = startGrid(SERVER);

        Ignite client = startGrid(CLIENT);

        if (putMode == AccessMode.CLOSURE) {
            client.compute().run(new IgniteRunnable() {
                @Override public void run() {
                    Ignition.ignite(SERVER).cache(DEFAULT_CACHE_NAME).put(KEY, new TestClass());
                }
            });
        }
        else
            (putMode == AccessMode.SERVER ? server : client).cache(DEFAULT_CACHE_NAME).put(KEY, new TestClass());

        Object val;

        if (getMode == AccessMode.CLOSURE) {
            val = client.compute().call(new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    return Ignition.ignite(SERVER).cache(DEFAULT_CACHE_NAME).get(KEY);
                }
            });
        }
        else
            val = (putMode == AccessMode.SERVER ? server : client).cache(DEFAULT_CACHE_NAME).get(KEY);

        assertNotNull(val);
        assertTrue(val.toString().contains("TestClass"));
    }

    /** */
    private enum AccessMode {
        SERVER,
        CLIENT,
        CLOSURE;
    }

    /** */
    static class TestClass {
        @QuerySqlField
        private int f = 42;
    }
}
