/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.testframework.junits;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.IgniteMarshaller;
import org.apache.ignite.plugin.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.internal.product.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Ignite mock.
 */
public class GridTestIgnite implements Ignite {
    /** Ignite name */
    private final String name;

    /** Local host. */
    private final String locHost;

    /** */
    private final UUID nodeId;

    /** */
    private IgniteMarshaller marshaller;

    /** */
    private final MBeanServer jmx;

    /** */
    private final String home;

    /**
     * Mock values
     *
     * @param name Name.
     * @param locHost Local host.
     * @param nodeId Node ID.
     * @param marshaller Marshaller.
     * @param jmx Jmx Bean Server.
     * @param home Ignite home.
     */
    public GridTestIgnite(
        String name, String locHost, UUID nodeId, IgniteMarshaller marshaller, MBeanServer jmx, String home) {
        this.locHost = locHost;
        this.nodeId = nodeId;
        this.marshaller = marshaller;
        this.jmx = jmx;
        this.home = home;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setMarshaller(marshaller);
        cfg.setNodeId(nodeId);
        cfg.setMBeanServer(jmx);
        cfg.setGridGainHome(home);
        cfg.setLocalHost(locHost);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup prj) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup prj) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup prj) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteManaged managed() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteManaged managed(ClusterGroup prj) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup prj) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteProduct product() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridSecurity security() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePortables portables() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCache<K, V> cache(@Nullable String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCache<?, ?>> caches() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> jcache(@Nullable String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataLoader<K, V> dataLoader(@Nullable String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFs fileSystem(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFs> fileSystems() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteStreamer streamer(@Nullable String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteStreamer> streamers() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {}
}
