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

package org.apache.ignite.p2p;

import java.lang.reflect.Method;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.p2p.P2PScanQueryUndeployTest.PREDICATE_CLASSNAME;
import static org.apache.ignite.p2p.SharedDeploymentTest.RUN_LAMBDA;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests user error (not server node failure) in case compute task compiled in unsupported bytecode version. */
public class P2PUnsupportedClassVersionTest extends GridCommonAbstractTest {
    /** */
    public static final String ENTRY_PROC_CLS_NAME = "org.apache.ignite.tests.p2p.CacheDeploymentBinaryEntryProcessor";

    /** */
    private static ListeningTestLogger lsnrLog;

    /** */
    private static Ignite srv;

    /** */
    private static Ignite cli;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true)
            .setCommunicationSpi(new SendComputeWithHigherClassVersionSpi())
            .setFailureHandler(new StopNodeFailureHandler())
            .setGridLogger(lsnrLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        lsnrLog = new ListeningTestLogger(log);

        srv = startGrid("server");
        cli = startClientGrid("client");
    }

    /** */
    @Test
    public void testEntryProcessor() throws Exception {
        IgniteCache<String, String> cache = cli.getOrCreateCache("my-cache");

        cache.put("1", "1");

        LogListener errMsgLsnr = errorMessageListener(ENTRY_PROC_CLS_NAME);

        CacheEntryProcessor<String, String, Boolean> proc = (CacheEntryProcessor<String, String, Boolean>)
            getExternalClassLoader().loadClass(ENTRY_PROC_CLS_NAME).newInstance();

        assertThrowsWithCause(() -> cache.invoke("1", proc), IgniteCheckedException.class);

        assertTrue(errMsgLsnr.check());

        // Check node is alive.
        cache.put("2", "2");
    }

    /** */
    @Test
    public void testCompute() throws Exception {
        Class<?> lambdaFactoryCls = getExternalClassLoader().loadClass(RUN_LAMBDA);

        Method method = lambdaFactoryCls.getMethod("lambda");

        IgniteCallable<Integer> lambda = (IgniteCallable<Integer>)method.invoke(lambdaFactoryCls);

        LogListener errMsgLsnr = errorMessageListener(RUN_LAMBDA);

        assertThrowsWithCause(
            () -> cli.compute(cli.cluster().forServers()).broadcast(lambda),
            IgniteDeploymentException.class
        );

        assertTrue(errMsgLsnr.check());

        // Check node is alive.
        cli.createCache("Can_create_cache_after_compute_fail");
    }

    /** */
    @Test
    public void testScanQuery() throws Exception {
        IgniteCache<String, String> cache = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put("3", "3");

        LogListener errMsgLsnr = errorMessageListener(PREDICATE_CLASSNAME);

        assertThrowsWithCause(() -> {
            try {
                cache.query(new ScanQuery<>((IgniteBiPredicate<Integer, Integer>)
                    getExternalClassLoader().loadClass(PREDICATE_CLASSNAME).newInstance()
                )).getAll();
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new IgniteException(e);
            }
        }, IgniteCheckedException.class);

        assertTrue(errMsgLsnr.check());

        // Check node is alive.
        cache.put("4", "4");
    }

    /** Custom communication SPI for simulating {@link UnsupportedClassVersionError} on server node. */
    private static class SendComputeWithHigherClassVersionSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            try {
                if (msg instanceof GridIoMessage) {
                    Message msg0 = ((GridIoMessage)msg).message();

                    if (msg0 instanceof GridDeploymentResponse)
                        incComputeClassVersion((GridDeploymentResponse)msg0);
                }
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private void incComputeClassVersion(GridDeploymentResponse resp) {
            GridByteArrayList byteSrc = U.field(resp, "byteSrc");

            // Assert byte array contains class file.
            assertEquals(0xCAFEBABE, byteSrc.getInt(0));

            // Assert minor version and first byte of major class version is zero.
            assertEquals(0, byteSrc.get(4));
            assertEquals(0, byteSrc.get(5));
            assertEquals(0, byteSrc.get(6));

            byte majorClsVer = byteSrc.get(7);

            assertTrue(byteSrc.get(7) > 0);

            byteSrc.set(7, (byte)(majorClsVer + 1));
        }
    }

    /** */
    private LogListener errorMessageListener(String clsName) {
        LogListener errMsgLsnr = LogListener
            .matches(UnsupportedClassVersionError.class.getName() + ": " + clsName.replace(".", "/"))
            .build();

        lsnrLog.registerListener(errMsgLsnr);

        return errMsgLsnr;
    }
}
