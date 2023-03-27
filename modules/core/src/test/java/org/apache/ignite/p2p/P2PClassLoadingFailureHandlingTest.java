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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureHandlerWithCallback;
import org.apache.ignite.internal.managers.deployment.P2PClassNotFoundException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/** */
@SuppressWarnings("ThrowableNotThrown")
@GridCommonTest(group = "P2P")
public class P2PClassLoadingFailureHandlingTest extends GridCommonAbstractTest {

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /***/
    private Ignite client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setConsistentId(igniteInstanceName);

        if (igniteInstanceName.startsWith("client"))
            cfg.setClientMode(true);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new FailureHandlerWithCallback(ctx -> failure.set(ctx.error()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid("server");
        client = startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /***/
    @Test
    public void streamReceiverP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        client.createCache(CACHE_NAME);

        StreamReceiver<Object, Object> receiver = instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.StreamReceiverCausingP2PClassLoadProblem");

        Throwable ex = assertThrows(log, () -> {
            try (IgniteDataStreamer<Object, Object> streamer = client.dataStreamer(CACHE_NAME)) {
                streamer.receiver(receiver);
                streamer.addData(1, "1");
            }
        }, IgniteException.class, "Failed to finish operation");
        assertThatCauseIsP2PClassLoadingIssue(ex);

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    private void assertThatCauseIsP2PClassLoadingIssue(Throwable ex) {
        assertTrue(X.hasCause(ex, P2PClassNotFoundException.class));
    }

    /***/
    @NotNull
    private <T> T instantiateClassLoadedWithExternalClassLoader(String className)
        throws ReflectiveOperationException {
        Class<?> clazz = getExternalClassLoader().loadClass(className);
        return (T)clazz.getConstructor().newInstance();
    }

    /***/
    private void assertThatFailureHandlerIsNotCalled() throws InterruptedException {
        letFailurePropagateToFailureHandler();

        StringWriter stringWriter = new StringWriter();
        if (failure.get() != null) {
            failure.get().printStackTrace(new PrintWriter(stringWriter));
        }

        assertNull("Failure handler should not be called, but it was with " + stringWriter, failure.get());
    }

    /***/
    @Test
    public void computeTaskP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        ComputeTask<Object, Object> task = instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.ComputeTaskCausingP2PClassLoadProblem");

        Throwable ex = assertThrows(log, () -> client.compute(client.cluster().forRemotes()).execute(task, null),
            IgniteException.class, "Remote job threw user exception");
        P2PClassNotFoundException p2PClassNotFoundException = X.cause(ex, P2PClassNotFoundException.class);
        assertThat(p2PClassNotFoundException, is(notNullValue()));
        assertThat(p2PClassNotFoundException.getMessage(), startsWith("Failed to peer load class"));

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    @Test
    public void serviceP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        Service svc = instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.ServiceCausingP2PClassLoadProblem"
        );
        ServiceConfiguration serviceConfig = new ServiceConfiguration()
            .setName("p2p-classloading-failure")
            .setTotalCount(1)
            .setService(svc);

        assertThrows(log, () -> client.services().deploy(serviceConfig), IgniteException.class,
            "Failed to deploy some services");

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    @Test
    public void entryProcessorP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        IgniteCache<Integer, String> cache = client.createCache(CACHE_NAME);
        cache.put(1, "1");

        EntryProcessor<Integer, String, String> processor = instantiateCacheEntryProcessorCausingP2PClassLoadProblem();

        assertThrows(log, () -> cache.invoke(1, processor), CacheException.class,
            "Failed to unmarshal object");

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    @NotNull
    private CacheEntryProcessor<Integer, String, String> instantiateCacheEntryProcessorCausingP2PClassLoadProblem()
        throws ReflectiveOperationException {
        return instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.CacheEntryProcessorCausingP2PClassLoadProblem");
    }

    /***/
    @Test
    public void cacheEntryProcessorP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        IgniteCache<Integer, String> cache = client.createCache(CACHE_NAME);
        cache.put(1, "1");

        CacheEntryProcessor<Integer, String, String> processor = instantiateCacheEntryProcessorCausingP2PClassLoadProblem();

        assertThrows(log, () -> cache.invoke(1, processor), CacheException.class,
            "Failed to unmarshal object");

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    @Test
    public void continuousQueryRemoteFilterP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        IgniteCache<Integer, String> cache = client.createCache(CACHE_NAME);

        ContinuousQuery<Integer, String> query = new ContinuousQuery<>();
        query.setLocalListener(entry -> {});
        query.setRemoteFilterFactory(instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.RemoteFilterFactoryCausingP2PClassLoadProblem"
        ));

        Throwable ex = assertThrows(log, () -> {
            try (QueryCursor<Cache.Entry<Integer, String>> ignored = cache.query(query)) {
                cache.put(1, "1");
            }
        }, IgniteException.class, "Failed to update keys");
        assertThatCauseIsP2PClassLoadingIssue(ex);

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    @Test
    public void continuousQueryRemoteTransformerP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        IgniteCache<Integer, String> cache = client.createCache(CACHE_NAME);

        ContinuousQueryWithTransformer<Integer, String, String> query = new ContinuousQueryWithTransformer<>();
        query.setLocalListener(entry -> {});
        query.setRemoteTransformerFactory(instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.RemoteTransformerFactoryCausingP2PClassLoadProblem"
        ));

        Throwable ex = assertThrows(log, () -> {
            try (QueryCursor<Cache.Entry<Integer, String>> ignored = cache.query(query)) {
                cache.put(1, "1");
            }
        }, IgniteException.class, "Failed to update keys");
        assertThatCauseIsP2PClassLoadingIssue(ex);

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    @Test
    public void remoteEventListenerP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        IgniteEvents events = client.events(client.cluster().forRemotes());
        events.remoteListen(
            (nodeId, event) -> true,
            instantiateClassLoadedWithExternalClassLoader(
                "org.apache.ignite.tests.p2p.classloadproblem.RemoteEventFilterCausingP2PClassLoadProblem"
            ),
            EventType.EVT_CACHE_OBJECT_PUT
        );

        IgniteCache<Integer, String> cache = client.createCache(CACHE_NAME);

        cache.put(1, "1");

        assertThatFailureHandlerIsNotCalled();
    }

    /***/
    private void letFailurePropagateToFailureHandler() throws InterruptedException {
        Thread.sleep(100);
    }

    /***/
    @Test
    public void remoteMessageListenerP2PClassLoadingProblemShouldNotCauseFailureHandling() throws Exception {
        IgniteMessaging messaging = client.message(client.cluster().forRemotes());
        String topic = "test-topic";
        messaging.remoteListen(topic, instantiateClassLoadedWithExternalClassLoader(
            "org.apache.ignite.tests.p2p.classloadproblem.RemoteMessagingListenerCausingP2PClassLoadProblem"
        ));

        messaging.send(topic, "Hello");

        assertThatFailureHandlerIsNotCalled();
    }
}
