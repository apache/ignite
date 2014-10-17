/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.scheduler.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.*;
import org.springframework.context.*;

import java.io.*;
import java.util.*;

/**
 * Grid Spring bean allows to bypass {@link GridGain} methods.
 * In other words, this bean class allows to inject new grid instance from
 * Spring configuration file directly without invoking static
 * {@link GridGain} methods. This class can be wired directly from
 * Spring and can be referenced from within other Spring beans.
 * By virtue of implementing {@link DisposableBean} and {@link InitializingBean}
 * interfaces, {@code GridSpringBean} automatically starts and stops underlying
 * grid instance.
 * <p>
 * <h1 class="header">Spring Configuration Example</h1>
 * Here is a typical example of describing it in Spring file:
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.gridgain.grid.GridSpringBean"&gt;
 *     &lt;property name="configuration"&gt;
 *         &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration"&gt;
 *             &lt;property name="gridName" value="mySpringGrid"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * Or use default configuration:
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.gridgain.grid.GridSpringBean"/&gt;
 * </pre>
 * <h1 class="header">Java Example</h1>
 * Here is how you may access this bean from code:
 * <pre name="code" class="java">
 * AbstractApplicationContext ctx = new FileSystemXmlApplicationContext("/path/to/spring/file");
 *
 * // Register Spring hook to destroy bean automatically.
 * ctx.registerShutdownHook();
 *
 * Grid grid = (Grid)ctx.getBean("mySpringBean");
 * </pre>
 * <p>
 */
public class GridSpringBean extends GridMetadataAwareAdapter implements Grid, DisposableBean, InitializingBean,
    ApplicationContextAware, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Grid g;

    /** */
    private GridConfiguration cfg;

    /** */
    private ApplicationContext appCtx;

    /** {@inheritDoc} */
    @Override public GridConfiguration configuration() {
        return cfg;
    }

    /**
     * Sets grid configuration.
     *
     * @param cfg Grid configuration.
     */
    public void setConfiguration(GridConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        appCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Grid grid() {
        assert g != null;

        return g;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        // If there were some errors when afterPropertiesSet() was called.
        if (g != null) {
            // Do not cancel started tasks, wait for them.
            G.stop(g.name(), false);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        if (cfg == null)
            cfg = new GridConfiguration();

        g = GridGainSpring.start(cfg, appCtx);
    }

    /** {@inheritDoc} */
    @Override public GridLogger log() {
        assert cfg != null;

        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridNodeLocalMap<K, V> nodeLocalMap() {
        assert g != null;

        return g.nodeLocalMap();
    }

    /** {@inheritDoc} */
    @Override public GridProduct product() {
        assert g != null;

        return g.product();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forLocal() {
        assert g != null;

        return g.forLocal();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDr dr() {
        assert g != null;

        return g.dr();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCache<?, ?>> caches() {
        assert g != null;

        return g.caches();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridStreamer> streamers() {
        assert g != null;

        return g.streamers();
    }

    /** {@inheritDoc} */
    @Override public GridNode localNode() {
        assert g != null;

        return g.localNode();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNode node(UUID nid) {
        assert g != null;

        return g.node(nid);
    }

    /** {@inheritDoc} */
    @Override public void stopNodes() throws GridException {
        assert g != null;

        g.stopNodes();
    }

    /** {@inheritDoc} */
    @Override public void restartNodes() throws GridException {
        assert g != null;

        g.restartNodes();
    }

    /** {@inheritDoc} */
    @Override public GridCompute compute() {
        assert g != null;

        return g.compute();
    }

    /** {@inheritDoc} */
    @Override public GridServices services() {
        assert g != null;

        return g.services();
    }

    /** {@inheritDoc} */
    @Override public GridMessaging message() {
        assert g != null;

        return g.message();
    }

    /** {@inheritDoc} */
    @Override public GridEvents events() {
        assert g != null;

        return g.events();
    }

    /** {@inheritDoc} */
    @Override public GridScheduler scheduler() {
        assert g != null;

        return g.scheduler();
    }

    /** {@inheritDoc} */
    @Override public GridSecurity security() {
        assert g != null;

        return g.security();
    }

    /** {@inheritDoc} */
    @Override public GridPortables portables() {
        assert g != null;

        return g.portables();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodes(Collection<? extends GridNode> nodes) {
        assert g != null;

        return g.forNodes(nodes);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeIds(Collection<UUID> ids) {
        assert g != null;

        return g.forNodeIds(ids);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeId(UUID nodeId, UUID... nodeIds) {
        assert g != null;

        return g.forNodeId(nodeId, nodeIds);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNode(GridNode node, GridNode... nodes) {
        assert g != null;

        return g.forNode(node, nodes);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(GridNode node, GridNode... nodes) {
        assert g != null;

        return g.forOthers(node);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(GridProjection prj) {
        assert g != null;

        return g.forOthers(prj);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forPredicate(GridPredicate<GridNode> p) {
        assert g != null;

        return g.forPredicate(p);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forAttribute(String name, @Nullable String val) {
        assert g != null;

        return g.forAttribute(name, val);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forCache(String cacheName, @Nullable String... cacheNames) {
        assert g != null;

        return g.forCache(cacheName, cacheNames);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forStreamer(String streamerName,
        @Nullable String... streamerNames) {
        assert g != null;

        return g.forStreamer(streamerName, streamerNames);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRemotes() {
        assert g != null;

        return g.forRemotes();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forHost(GridNode node) {
        assert g != null;

        return g.forHost(node);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forDaemons() {
        assert g != null;

        return g.forDaemons();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRandom() {
        assert g != null;

        return g.forRandom();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOldest() {
        assert g != null;

        return g.forOldest();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forYoungest() {
        assert g != null;

        return g.forYoungest();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> nodes() {
        assert g != null;

        return g.nodes();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNode node() {
        assert g != null;

        return g.node();
    }

    /** {@inheritDoc} */
    @Override public GridPredicate<GridNode> predicate() {
        assert g != null;

        return g.predicate();
    }

    /** {@inheritDoc} */
    @Override public GridProjectionMetrics metrics() throws GridException {
        assert g != null;

        return g.metrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> topology(long topVer) {
        return g.topology(topVer);
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        return g.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        assert g != null;

        return g.pingNode(nodeId);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        assert g != null;

        return g.name();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCache<K, V> cache(String name) {
        assert g != null;

        return g.cache(name);
    }

    /** {@inheritDoc} */
    @Override public <K> Map<GridNode, Collection<K>> mapKeysToNodes(String cacheName,
        @Nullable Collection<? extends K> keys) throws GridException {
        assert g != null;

        return g.mapKeysToNodes(cacheName, keys);
    }

    /** {@inheritDoc} */
    @Override public <K> GridNode mapKeyToNode(String cacheName, K key) throws GridException {
        assert g != null;

        return g.mapKeyToNode(cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Collection<GridTuple3<String, Boolean, String>>> startNodes(File file,
        boolean restart, int timeout, int maxConn) throws GridException {
        assert g != null;

        return g.startNodes(file, restart, timeout, maxConn);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Collection<GridTuple3<String, Boolean, String>>> startNodes(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts, boolean restart, int timeout,
        int maxConn) throws GridException {
        assert g != null;

        return g.startNodes(hosts, dflts, restart, timeout, maxConn);
    }

    /** {@inheritDoc} */
    @Override public void stopNodes(Collection<UUID> ids) throws GridException {
        assert g != null;

        g.stopNodes(ids);
    }

    /** {@inheritDoc} */
    @Override public void restartNodes(Collection<UUID> ids) throws GridException {
        assert g != null;

        g.restartNodes(ids);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridDataLoader<K, V> dataLoader(@Nullable String cacheName) {
        assert g != null;

        return g.dataLoader(cacheName);
    }

    /** {@inheritDoc} */
    @Override public GridGgfs ggfs(String name) {
        assert g != null;

        return g.ggfs(name);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfs> ggfss() {
        assert g != null;

        return g.ggfss();
    }

    /** {@inheritDoc} */
    @Override public GridHadoop hadoop() {
        assert g != null;

        return g.hadoop();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridStreamer streamer(@Nullable String name) {
        assert g != null;

        return g.streamer(name);
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        assert g != null;

        g.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        g.close();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSpringBean.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(g);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        g = (Grid)in.readObject();

        cfg = g.configuration();
    }
}
