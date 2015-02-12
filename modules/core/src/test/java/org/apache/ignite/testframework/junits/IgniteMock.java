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

package org.apache.ignite.testframework.junits;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.internal.product.*;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Ignite mock.
 */
public class IgniteMock implements Ignite {
    /** Ignite name */
    private final String name;

    /** Local host. */
    private final String locHost;

    /** */
    private final UUID nodeId;

    /** */
    private Marshaller marshaller;

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
    public IgniteMock(
        String name, String locHost, UUID nodeId, Marshaller marshaller, MBeanServer jmx, String home) {
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
        cfg.setIgniteHome(home);
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
    @Override public IgniteCompute compute(ClusterGroup grp) {
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
    @Override public IgniteEvents events(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
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
    @Override public void close() {}

    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        @Nullable T initVal,
        boolean create)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteCountDownLatch countDownLatch(String name,
        int cnt,
        boolean autoDel,
        boolean create)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteQueue<T> queue(String name,
        int cap,
        CollectionConfiguration cfg)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteSet<T> set(String name,
        CollectionConfiguration cfg)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> CacheAffinity<K> affinity(String cacheName) {
        return null;
    }
}
