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

package org.apache.ignite.internal.managers;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.timeout.GridSpiTimeoutObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiNoop;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Convenience adapter for grid managers.
 *
 * @param <T> SPI wrapped by this manager.
 *
 */
public abstract class GridManagerAdapter<T extends IgniteSpi> implements GridManager {
    /** Kernal context. */
    @GridToStringExclude
    protected final GridKernalContext ctx;

    /** Logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /** Set of SPIs for this manager. */
    @GridToStringExclude
    private final T[] spis;

    /** Checks is SPI implementation is {@code NO-OP} or not. */
    private final boolean enabled;

    /** */
    private final Map<IgniteSpi, Boolean> spiMap = new IdentityHashMap<>();

    /**
     * @param ctx Kernal context.
     * @param spis Specific SPI instance.
     */
    protected GridManagerAdapter(GridKernalContext ctx, T... spis) {
        assert spis != null;
        assert spis.length > 0;
        assert ctx != null;

        this.ctx = ctx;
        this.spis = spis;

        boolean enabled = false;

        for (int i = 0; i < spis.length; i++) {
            if (!U.hasAnnotation(this.spis[i].getClass(), IgniteSpiNoop.class))
                enabled = true;
        }

        this.enabled = enabled;

        log = ctx.log(getClass());
    }

    /**
     * Gets wrapped SPI.
     *
     * @return Wrapped SPI.
     */
    protected final T getSpi() {
        return spis[0];
    }

    /**
     * @param name SPI name
     * @return SPI for given name. If {@code null} or empty, then 1st SPI on the list
     *      is returned.
     */
    protected final T getSpi(@Nullable String name) {
        if (F.isEmpty(name))
            return spis[0];

        // Loop through SPI's, not proxies, because
        // proxy.getName() is more expensive than spi.getName().
        for (T t : spis) {
            if (t.getName().equals(name))
                return t;
        }

        throw new IgniteException("Failed to find SPI for name: " + name);
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return enabled;
    }

    /**
     * @return Configured SPI's.
     */
    protected final T[] getSpis() {
        return spis;
    }

    /**
     * @param spi SPI whose internal objects need to be injected.
     * @throws IgniteCheckedException If injection failed.
     */
    private void inject(IgniteSpi spi) throws IgniteCheckedException {
        if (spi instanceof IgniteSpiAdapter) {
            Collection<Object> injectables = ((IgniteSpiAdapter)spi).injectables();

            if (!F.isEmpty(injectables))
                for (Object o : injectables)
                    ctx.resource().injectGeneric(o);
        }
    }

    /**
     * @param spi SPI whose internal objects need to be injected.
     * @throws IgniteCheckedException If injection failed.
     */
    private void cleanup(IgniteSpi spi) throws IgniteCheckedException {
        if (spi instanceof IgniteSpiAdapter) {
            Collection<Object> injectables = ((IgniteSpiAdapter)spi).injectables();

            if (!F.isEmpty(injectables))
                for (Object o : injectables)
                    ctx.resource().cleanupGeneric(o);
        }
    }

    /** {@inheritDoc} */
    @Override public void onBeforeSpiStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onAfterSpiStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        for (T t : spis)
            t.onClientDisconnected(reconnectFut);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        for (T t : spis)
            t.onClientReconnected(clusterRestarted);

        return null;
    }

    /**
     * Starts wrapped SPI.
     *
     * @throws IgniteCheckedException If wrapped SPI could not be started.
     */
    protected final void startSpi() throws IgniteCheckedException {
        Collection<String> names = U.newHashSet(spis.length);

        for (T spi : spis) {
            if (spi instanceof IgniteSpiAdapter)
                ((IgniteSpiAdapter)spi).onBeforeStart();

            // Save SPI to map to make sure to stop it properly.
            Boolean res = spiMap.put(spi, Boolean.TRUE);

            assert res == null;

            // Inject all spi resources.
            ctx.resource().inject(spi);

            // Inject SPI internal objects.
            inject(spi);

            try {
                Map<String, Object> retval = spi.getNodeAttributes();

                if (retval != null) {
                    for (Map.Entry<String, Object> e : retval.entrySet()) {
                        if (ctx.hasNodeAttribute(e.getKey()))
                            throw new IgniteCheckedException("SPI attribute collision for attribute [spi=" + spi +
                                ", attr=" + e.getKey() + ']' +
                                ". Attribute set by one SPI implementation has the same name (name collision) as " +
                                "attribute set by other SPI implementation. Such overriding is not allowed. " +
                                "Please check your Ignite configuration and/or SPI implementation to avoid " +
                                "attribute name collisions.");

                        ctx.addNodeAttribute(e.getKey(), e.getValue());
                    }
                }
            }
            catch (IgniteSpiException e) {
                throw new IgniteCheckedException("Failed to get SPI attributes.", e);
            }

            // Print-out all SPI parameters only in DEBUG mode.
            if (log.isDebugEnabled())
                log.debug("Starting SPI: " + spi);

            if (names.contains(spi.getName()))
                throw new IgniteCheckedException("Duplicate SPI name (need to explicitly configure 'setName()' property): " +
                    spi.getName());

            names.add(spi.getName());

            if (log.isDebugEnabled())
                log.debug("Starting SPI implementation: " + spi.getClass().getName());

            onBeforeSpiStart();

            try {
                spi.spiStart(ctx.gridName());
            }
            catch (IgniteSpiException e) {
                throw new IgniteCheckedException("Failed to start SPI: " + spi, e);
            }

            onAfterSpiStart();

            if (log.isDebugEnabled())
                log.debug("SPI module started OK: " + spi.getClass().getName());
        }
    }

    /**
     * Stops wrapped SPI.
     *
     * @throws IgniteCheckedException If underlying SPI could not be stopped.
     */
    protected final void stopSpi() throws IgniteCheckedException {
        for (T spi : spis) {
            if (spiMap.remove(spi) == null) {
                if (log.isDebugEnabled())
                    log.debug("Will not stop SPI since it has not been started by this manager: " + spi);

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("Stopping SPI: " + spi);

            try {
                spi.spiStop();

                if (log.isDebugEnabled())
                    log.debug("SPI module stopped OK: " + spi.getClass().getName());
            }
            catch (IgniteSpiException e) {
                throw new IgniteCheckedException("Failed to stop SPI: " + spi, e);
            }

            try {
                cleanup(spi);

                ctx.resource().cleanup(spi);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to remove injected resources from SPI (ignoring): " + spi, e);
            }
        }
    }

    /**
     * @return Uniformly formatted ack string.
     */
    protected final String startInfo() {
        return "Manager started ok: " + getClass().getName();
    }

    /**
     * @return Uniformly formatted ack string.
     */
    protected final String stopInfo() {
        return "Manager stopped ok: " + getClass().getName();
    }

    /** {@inheritDoc} */
    @Override public final void onKernalStart() throws IgniteCheckedException {
        for (final IgniteSpi spi : spis) {
            try {
                spi.onContextInitialized(new IgniteSpiContext() {
                    @Override public boolean isStopping() {
                        return ctx.isStopping();
                    }

                    @Override public Collection<ClusterNode> remoteNodes() {
                        return ctx.discovery().remoteNodes();
                    }

                    @Override public Collection<ClusterNode> nodes() {
                        return ctx.discovery().allNodes();
                    }

                    @Override public ClusterNode localNode() {
                        return ctx.discovery().localNode();
                    }

                    @Override public Collection<ClusterNode> remoteDaemonNodes() {
                        final Collection<ClusterNode> all = ctx.discovery().daemonNodes();

                        return !localNode().isDaemon() ?
                            all :
                            F.view(all, new IgnitePredicate<ClusterNode>() {
                                @Override public boolean apply(ClusterNode n) {
                                    return n.isDaemon();
                                }
                            });
                    }

                    @Nullable @Override public ClusterNode node(UUID nodeId) {
                        A.notNull(nodeId, "nodeId");

                        return ctx.discovery().node(nodeId);
                    }

                    @Override public boolean pingNode(UUID nodeId) {
                        A.notNull(nodeId, "nodeId");

                        try {
                            return ctx.discovery().pingNode(nodeId);
                        }
                        catch (IgniteCheckedException e) {
                            throw U.convertException(e);
                        }
                    }

                    @Override public void send(ClusterNode node, Serializable msg, String topic)
                        throws IgniteSpiException {
                        A.notNull(node, "node");
                        A.notNull(msg, "msg");
                        A.notNull(topic, "topic");

                        try {
                            if (msg instanceof Message)
                                ctx.io().send(node, topic, (Message)msg, SYSTEM_POOL);
                            else
                                ctx.io().sendUserMessage(Collections.singletonList(node), msg, topic, false, 0, false);
                        }
                        catch (IgniteCheckedException e) {
                            throw unwrapException(e);
                        }
                    }

                    @Override public void addLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
                        A.notNull(topic, "topic");
                        A.notNull(p, "p");

                        ctx.io().addUserMessageListener(topic, p);
                    }

                    @Override public void removeLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
                        A.notNull(topic, "topic");
                        A.notNull(topic, "p");

                        ctx.io().removeUserMessageListener(topic, p);
                    }

                    @SuppressWarnings("deprecation")
                    @Override public void addMessageListener(GridMessageListener lsnr, String topic) {
                        A.notNull(lsnr, "lsnr");
                        A.notNull(topic, "topic");

                        ctx.io().addMessageListener(topic, lsnr);
                    }

                    @SuppressWarnings("deprecation")
                    @Override public boolean removeMessageListener(GridMessageListener lsnr, String topic) {
                        A.notNull(lsnr, "lsnr");
                        A.notNull(topic, "topic");

                        return ctx.io().removeMessageListener(topic, lsnr);
                    }

                    @Override public void addLocalEventListener(GridLocalEventListener lsnr, int... types) {
                        A.notNull(lsnr, "lsnr");

                        ctx.event().addLocalEventListener(lsnr, types);
                    }

                    @Override public boolean removeLocalEventListener(GridLocalEventListener lsnr) {
                        A.notNull(lsnr, "lsnr");

                        return ctx.event().removeLocalEventListener(lsnr);
                    }

                    @Override public boolean isEventRecordable(int... types) {
                        for (int t : types)
                            if (!ctx.event().isRecordable(t))
                                return false;

                        return true;
                    }

                    @Override public void recordEvent(Event evt) {
                        A.notNull(evt, "evt");

                        if (ctx.event().isRecordable(evt.type()))
                            ctx.event().record(evt);
                    }

                    @Override public void registerPort(int port, IgnitePortProtocol proto) {
                        ctx.ports().registerPort(port, proto, spi.getClass());
                    }

                    @Override public void deregisterPort(int port, IgnitePortProtocol proto) {
                        ctx.ports().deregisterPort(port, proto, spi.getClass());
                    }

                    @Override public void deregisterPorts() {
                        ctx.ports().deregisterPorts(spi.getClass());
                    }

                    @Nullable @Override public <K, V> V get(String cacheName, K key) {
                        return ctx.cache().<K, V>jcache(cacheName).get(key);
                    }

                    @Nullable @Override public <K, V> V put(String cacheName, K key, V val, long ttl) {
                        try {
                            if (ttl > 0) {
                                ExpiryPolicy plc = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                                IgniteCache<K, V> cache = ctx.cache().<K, V>publicJCache(cacheName).withExpiryPolicy(plc);

                                return cache.getAndPut(key, val);
                            }
                            else
                                return ctx.cache().<K, V>jcache(cacheName).getAndPut(key, val);
                        }
                        catch (IgniteCheckedException e) {
                            throw CU.convertToCacheException(e);
                        }
                    }

                    @Nullable @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) {
                        try {
                            if (ttl > 0) {
                                ExpiryPolicy plc = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                                IgniteCache<K, V> cache = ctx.cache().<K, V>publicJCache(cacheName).withExpiryPolicy(plc);

                                return cache.getAndPutIfAbsent(key, val);
                            }
                            else
                                return ctx.cache().<K, V>jcache(cacheName).getAndPutIfAbsent(key, val);
                        }
                        catch (IgniteCheckedException e) {
                            throw CU.convertToCacheException(e);
                        }
                    }

                    @Nullable @Override public <K, V> V remove(String cacheName, K key) {
                        return ctx.cache().<K, V>jcache(cacheName).getAndRemove(key);
                    }

                    @Override public <K> boolean containsKey(String cacheName, K key) {
                        return ctx.cache().cache(cacheName).containsKey(key);
                    }

                    @Override public int partition(String cacheName, Object key) {
                        return ctx.cache().cache(cacheName).affinity().partition(key);
                    }

                    @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
                        for (GridComponent comp : ctx) {
                            IgniteNodeValidationResult err = comp.validateNode(node);

                            if (err != null)
                                return err;
                        }

                        return null;
                    }

                    @Override public Collection<SecuritySubject> authenticatedSubjects() {
                        try {
                            return ctx.security().authenticatedSubjects();
                        }
                        catch (IgniteCheckedException e) {
                            throw U.convertException(e);
                        }
                    }

                    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
                        try {
                            return ctx.security().authenticatedSubject(subjId);
                        }
                        catch (IgniteCheckedException e) {
                            throw U.convertException(e);
                        }
                    }

                    @Override public MessageFormatter messageFormatter() {
                        return ctx.io().formatter();
                    }

                    @Override public MessageFactory messageFactory() {
                        return ctx.io().messageFactory();
                    }

                    @Override public boolean tryFailNode(UUID nodeId, @Nullable String warning) {
                        return ctx.discovery().tryFailNode(nodeId, warning);
                    }

                    @Override public void failNode(UUID nodeId, @Nullable String warning) {
                        ctx.discovery().failNode(nodeId, warning);
                    }

                    @Override public void addTimeoutObject(IgniteSpiTimeoutObject obj) {
                        ctx.timeout().addTimeoutObject(new GridSpiTimeoutObject(obj));
                    }

                    @Override public void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
                        ctx.timeout().removeTimeoutObject(new GridSpiTimeoutObject(obj));
                    }

                    /**
                     * @param e Exception to handle.
                     * @return GridSpiException Converted exception.
                     */
                    private IgniteSpiException unwrapException(IgniteCheckedException e) {
                        // Avoid double-wrapping.
                        if (e.getCause() instanceof IgniteSpiException)
                            return (IgniteSpiException)e.getCause();

                        return new IgniteSpiException("Failed to execute SPI context method.", e);
                    }
                });
            }
            catch (IgniteSpiException e) {
                throw new IgniteCheckedException("Failed to initialize SPI context.", e);
            }
        }

        onKernalStart0();
    }

    /** {@inheritDoc} */
    @Override public final void onKernalStop(boolean cancel) {
        onKernalStop0(cancel);

        for (IgniteSpi spi : spis)
            spi.onContextDestroyed();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Serializable collectDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(UUID joiningNodeId, UUID rmtNodeId, Serializable data) {
        // No-op.
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void onKernalStart0() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void onKernalStop0(boolean cancel) {
        // No-op.
    }

    /**
     * Throws exception with uniform error message if given parameter's assertion condition
     * is {@code false}.
     *
     * @param cond Assertion condition to check.
     * @param condDesc Description of failed condition. Note that this description should include
     *      JavaBean name of the property (<b>not</b> a variable name) as well condition in
     *      Java syntax like, for example:
     *      <pre name="code" class="java">
     *      ...
     *      assertParameter(dirPath != null, "dirPath != null");
     *      ...
     *      </pre>
     *      Note that in case when variable name is the same as JavaBean property you
     *      can just copy Java condition expression into description as a string.
     * @throws IgniteCheckedException Thrown if given condition is {@code false}
     */
    protected final void assertParameter(boolean cond, String condDesc) throws IgniteCheckedException {
        if (!cond)
            throw new IgniteCheckedException("Grid configuration parameter failed condition check: " + condDesc);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public final String toString() {
        return S.toString(GridManagerAdapter.class, this, "name", getClass().getName());
    }
}
