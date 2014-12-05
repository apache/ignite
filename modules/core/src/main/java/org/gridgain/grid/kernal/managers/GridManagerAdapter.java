/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static java.util.Arrays.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

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
            if (!U.hasAnnotation(this.spis[i].getClass(), GridSpiNoop.class))
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

        throw new GridRuntimeException("Failed to find SPI for name: " + name);
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

    /** {@inheritDoc} */
    @Override public final void addSpiAttributes(Map<String, Object> attrs) throws GridException {
        for (T spi : spis) {
            // Inject all spi resources.
            ctx.resource().inject(spi);

            // Inject SPI internal objects.
            inject(spi);

            try {
                Map<String, Object> retval = spi.getNodeAttributes();

                if (retval != null) {
                    for (Map.Entry<String, Object> e : retval.entrySet()) {
                        if (attrs.containsKey(e.getKey()))
                            throw new GridException("SPI attribute collision for attribute [spi=" + spi +
                                ", attr=" + e.getKey() + ']' +
                                ". Attribute set by one SPI implementation has the same name (name collision) as " +
                                "attribute set by other SPI implementation. Such overriding is not allowed. " +
                                "Please check your GridGain configuration and/or SPI implementation to avoid " +
                                "attribute name collisions.");

                        attrs.put(e.getKey(), e.getValue());
                    }
                }
            }
            catch (IgniteSpiException e) {
                throw new GridException("Failed to get SPI attributes.", e);
            }
        }
    }

    /**
     * @param spi SPI whose internal objects need to be injected.
     * @throws GridException If injection failed.
     */
    private void inject(IgniteSpi spi) throws GridException {
        if (spi instanceof IgniteSpiAdapter) {
            Collection<Object> injectables = ((IgniteSpiAdapter)spi).injectables();

            if (!F.isEmpty(injectables))
                for (Object o : injectables)
                    ctx.resource().injectGeneric(o);
        }
    }

    /**
     * @param spi SPI whose internal objects need to be injected.
     * @throws GridException If injection failed.
     */
    private void cleanup(IgniteSpi spi) throws GridException {
        if (spi instanceof IgniteSpiAdapter) {
            Collection<Object> injectables = ((IgniteSpiAdapter)spi).injectables();

            if (!F.isEmpty(injectables))
                for (Object o : injectables)
                    ctx.resource().cleanupGeneric(o);
        }
    }

    /**
     * Starts wrapped SPI.
     *
     * @throws GridException If wrapped SPI could not be started.
     */
    protected final void startSpi() throws GridException {
        Collection<String> names = U.newHashSet(spis.length);

        for (T spi : spis) {
            // Print-out all SPI parameters only in DEBUG mode.
            if (log.isDebugEnabled())
                log.debug("Starting SPI: " + spi);

            if (names.contains(spi.getName()))
                throw new GridException("Duplicate SPI name (need to explicitly configure 'setName()' property): " +
                    spi.getName());

            names.add(spi.getName());

            if (log.isDebugEnabled())
                log.debug("Starting SPI implementation: " + spi.getClass().getName());

            try {
                spi.spiStart(ctx.gridName());
            }
            catch (IgniteSpiException e) {
                throw new GridException("Failed to start SPI: " + spi, e);
            }

            if (log.isDebugEnabled())
                log.debug("SPI module started OK: " + spi.getClass().getName());
        }
    }

    /**
     * Stops wrapped SPI.
     *
     * @throws GridException If underlying SPI could not be stopped.
     */
    protected final void stopSpi() throws GridException {
        for (T spi : spis) {
            if (log.isDebugEnabled())
                log.debug("Stopping SPI: " + spi);

            try {
                spi.spiStop();

                if (log.isDebugEnabled())
                    log.debug("SPI module stopped OK: " + spi.getClass().getName());
            }
            catch (IgniteSpiException e) {
                throw new GridException("Failed to stop SPI: " + spi, e);
            }

            try {
                cleanup(spi);

                ctx.resource().cleanup(spi);
            }
            catch (GridException e) {
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
    @Override public final void onKernalStart() throws GridException {
        for (final IgniteSpi spi : spis) {
            try {
                spi.onContextInitialized(new GridSpiContext() {
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

                        return ctx.discovery().pingNode(nodeId);
                    }

                    @Override public void send(ClusterNode node, Serializable msg, String topic)
                        throws IgniteSpiException {
                        A.notNull(node, "node");
                        A.notNull(msg, "msg");
                        A.notNull(topic, "topic");

                        try {
                            if (msg instanceof GridTcpCommunicationMessageAdapter)
                                ctx.io().send(node, topic, (GridTcpCommunicationMessageAdapter)msg, SYSTEM_POOL);
                            else
                                ctx.io().sendUserMessage(asList(node), msg, topic, false, 0);
                        }
                        catch (GridException e) {
                            throw unwrapException(e);
                        }
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

                    @Override public void recordEvent(IgniteEvent evt) {
                        A.notNull(evt, "evt");

                        if (ctx.event().isRecordable(evt.type()))
                            ctx.event().record(evt);
                    }

                    @Override public void registerPort(int port, GridPortProtocol proto) {
                        ctx.ports().registerPort(port, proto, spi.getClass());
                    }

                    @Override public void deregisterPort(int port, GridPortProtocol proto) {
                        ctx.ports().deregisterPort(port, proto, spi.getClass());
                    }

                    @Override public void deregisterPorts() {
                        ctx.ports().deregisterPorts(spi.getClass());
                    }

                    @Nullable @Override public <K, V> V get(String cacheName, K key) throws GridException {
                        return ctx.cache().<K, V>cache(cacheName).get(key);
                    }

                    @Nullable @Override public <K, V> V put(String cacheName, K key, V val, long ttl)
                        throws GridException {
                        GridCacheEntry<K, V> e = ctx.cache().<K, V>cache(cacheName).entry(key);

                        assert e != null;

                        e.timeToLive(ttl);

                        return e.set(val);
                    }

                    @Nullable @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl)
                        throws GridException {
                        GridCacheEntry<K, V> e = ctx.cache().<K, V>cache(cacheName).entry(key);

                        assert e != null;

                        e.timeToLive(ttl);

                        return e.setIfAbsent(val);
                    }

                    @Nullable @Override public <K, V> V remove(String cacheName, K key) throws GridException {
                        return ctx.cache().<K, V>cache(cacheName).remove(key);
                    }

                    @Override public <K> boolean containsKey(String cacheName, K key) {
                        return ctx.cache().cache(cacheName).containsKey(key);
                    }

                    @Override public void writeToSwap(String spaceName, Object key, @Nullable Object val,
                        @Nullable ClassLoader ldr) throws GridException {
                        assert ctx.swap().enabled();

                        ctx.swap().write(spaceName, key, val, ldr);
                    }

                    @Nullable @Override public <T> T readFromOffheap(String spaceName, int part, Object key,
                        byte[] keyBytes, @Nullable ClassLoader ldr) throws GridException {
                        return ctx.offheap().getValue(spaceName, part, key, keyBytes, ldr);
                    }

                    @Override public boolean removeFromOffheap(@Nullable String spaceName, int part, Object key,
                        @Nullable byte[] keyBytes) throws GridException {
                        return ctx.offheap().removex(spaceName, part, key, keyBytes);
                    }

                    @Override public void writeToOffheap(@Nullable String spaceName, int part, Object key,
                        @Nullable byte[] keyBytes, Object val, @Nullable byte[] valBytes, @Nullable ClassLoader ldr)
                        throws GridException {
                        ctx.offheap().put(spaceName, part, key, keyBytes, valBytes != null ? valBytes :
                            ctx.config().getMarshaller().marshal(val));
                    }

                    @SuppressWarnings({"unchecked"})
                    @Nullable @Override public <T> T readFromSwap(String spaceName, GridSwapKey key,
                        @Nullable ClassLoader ldr) throws GridException {
                        assert ctx.swap().enabled();

                        return ctx.swap().readValue(spaceName, key, ldr);
                    }

                    @Override public int partition(String cacheName, Object key) {
                        return ctx.cache().cache(cacheName).affinity().partition(key);
                    }

                    @Override public void removeFromSwap(String spaceName, Object key,
                        @Nullable ClassLoader ldr) throws GridException {
                        assert ctx.swap().enabled();

                        ctx.swap().remove(spaceName, key, null, ldr);
                    }

                    @Override public GridNodeValidationResult validateNode(ClusterNode node) {
                        for (GridComponent comp : ctx) {
                            GridNodeValidationResult err = comp.validateNode(node);

                            if (err != null)
                                return err;
                        }

                        return null;
                    }

                    @Override public boolean writeDelta(UUID nodeId, Object msg, ByteBuffer buf) {
                        for (MessageCallback patcher : ctx.plugins().extensions(MessageCallback.class)) {
                            if (!patcher.onSend(nodeId, msg, buf))
                                return false;
                        }

                        return true;
                    }

                    @Override public boolean readDelta(UUID nodeId, Class<?> msgCls, ByteBuffer buf) {
                        for (MessageCallback patcher : ctx.plugins().extensions(MessageCallback.class)) {
                            if (!patcher.onReceive(nodeId, msgCls, buf))
                                return false;
                        }

                        return true;
                    }

                    @Override public Collection<GridSecuritySubject> authenticatedSubjects() throws GridException {
                        return ctx.grid().security().authenticatedSubjects();
                    }

                    @Override public GridSecuritySubject authenticatedSubject(UUID subjId) throws GridException {
                        return ctx.grid().security().authenticatedSubject(subjId);
                    }

                    @SuppressWarnings("unchecked")
                    @Nullable @Override public <V> V readValueFromOffheapAndSwap(@Nullable String spaceName,
                        Object key, @Nullable ClassLoader ldr) throws GridException {
                        GridCache<Object, V> cache = ctx.cache().cache(spaceName);

                        GridCacheContext cctx = ((GridCacheProxyImpl)cache).context();

                        if (cctx.isNear())
                            cctx = cctx.near().dht().context();

                        GridCacheSwapEntry e = cctx.swap().read(key);

                        return e != null ? (V)e.value() : null;
                    }

                    @Override public GridTcpMessageFactory messageFactory() {
                        return ctx.messageFactory();
                    }

                    /**
                     * @param e Exception to handle.
                     * @return GridSpiException Converted exception.
                     */
                    private IgniteSpiException unwrapException(GridException e) {
                        // Avoid double-wrapping.
                        if (e.getCause() instanceof IgniteSpiException)
                            return (IgniteSpiException)e.getCause();

                        return new IgniteSpiException("Failed to execute SPI context method.", e);
                    }
                });
            }
            catch (IgniteSpiException e) {
                throw new GridException("Failed to initialize SPI context.", e);
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
    @Override @Nullable public Object collectDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(Object data) {
        // No-op.
    }

    /**
     * @throws GridException If failed.
     */
    protected void onKernalStart0() throws GridException {
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
     * @throws GridException Thrown if given condition is {@code false}
     */
    protected final void assertParameter(boolean cond, String condDesc) throws GridException {
        if (!cond)
            throw new GridException("Grid configuration parameter failed condition check: " + condDesc);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public final String toString() {
        return S.toString(GridManagerAdapter.class, this, "name", getClass().getName());
    }
}
