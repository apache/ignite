/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy SPI context for offline utilities.
 */
public class StandaloneSpiContext implements IgniteSpiContext {
    /** {@inheritDoc} */
    @Override public void addLocalEventListener(GridLocalEventListener lsnr, int... types) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void addMessageListener(GridMessageListener lsnr, String topic) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void addLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void recordEvent(Event evt) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void registerPort(int port, IgnitePortProtocol proto) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void deregisterPort(int port, IgnitePortProtocol proto) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void deregisterPorts() {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public <K, V> V get(String cacheName, K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V put(String cacheName, K key, V val, long ttl) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V remove(String cacheName, K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> boolean containsKey(String cacheName, K key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int partition(String cacheName, Object key) {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode node(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> remoteNodes() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removeLocalEventListener(GridLocalEventListener lsnr) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isEventRecordable(int... types) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void removeLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public boolean removeMessageListener(GridMessageListener lsnr, String topic) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void send(ClusterNode node, Serializable msg, String topic) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node, DiscoveryDataBag discoData) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public MessageFormatter messageFormatter() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public MessageFactoryProvider messageFactory() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isStopping() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean tryFailNode(UUID nodeId, @Nullable String warning) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addTimeoutObject(IgniteSpiTimeoutObject obj) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> nodeAttributes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean communicationFailureResolveSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void resolveCommunicationFailure(ClusterNode node, Exception err) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public ReadOnlyMetricRegistry getOrCreateMetricRegistry(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void removeMetricRegistry(String name) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Iterable<ReadOnlyMetricRegistry> metricRegistries() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void addMetricRegistryCreationListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
        // No-op.
    }
}
