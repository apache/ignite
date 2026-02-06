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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.discovery.SecurityAwareCustomMessageWrapper;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class NodeSecurityContextPropagationTest extends GridCommonAbstractTest {
    /** */
    private static final Collection<UUID> TEST_MESSAGE_ACCEPTED_NODES = new HashSet<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeOrHaltFailureHandler())
            .setLocalEventListeners(
                Collections.singletonMap(e -> {
                    DiscoveryCustomEvent discoEvt = (DiscoveryCustomEvent)e;

                    if (discoEvt.customMessage() instanceof TestDiscoveryAcknowledgeMessage)
                        TEST_MESSAGE_ACCEPTED_NODES.add(discoEvt.node().id());

                    return true;
                }, new int[] {EVT_DISCOVERY_CUSTOM_EVT})
            )
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100L * 1024 * 1024)))
            .setAuthenticationEnabled(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi())
            .setIpFinder(new TcpDiscoveryVmIpFinder()
                .setAddresses(Collections.singleton("127.0.0.1:47500")));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx ignite = super.startGrid(idx);

        wrapRingMessageWorkerQueue(ignite);

        return ignite;
    }

    /** */
    @Test
    public void testProcessCustomDiscoveryMessageFromLeftNode() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteEx cli = startClientGrid(11);

        IgniteEx srv = startGrid(1);

        CountDownLatch cliLefEvtProcessedByCoordinator = new CountDownLatch(1);

        crd.events().localListen(
            evt -> {
                cliLefEvtProcessedByCoordinator.countDown();

                return true;
            },
            EVT_NODE_LEFT
        );

        discoveryRingMessageWorkerQueue(srv).block();

        long pollingTimeout = U.field(discoveryRingMessageWorker(srv), "pollingTimeout");

        // We need to wait for any active BlockingDeque#poll operation to complete.
        U.sleep(5 * pollingTimeout);

        cli.context().discovery().sendCustomEvent(new TestDiscoveryMessage());

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> msg instanceof TestDiscoveryMessage), getTestTimeout());

        runAsync(() -> stopGrid(11));

        cliLefEvtProcessedByCoordinator.await();

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> msg instanceof TcpDiscoveryNodeLeftMessage), getTestTimeout());

        runAsync(() -> startGrid(2));

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> isDiscoveryNodeAddedMessage(msg, 2)), getTestTimeout());

        runAsync(() -> startGrid(3));

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> isDiscoveryNodeAddedMessage(msg, 3)), getTestTimeout());

        discoveryRingMessageWorkerQueue(srv).unblock();

        waitForCondition(
            () -> grid(0).cluster().nodes().size() == 4
                && TEST_MESSAGE_ACCEPTED_NODES.contains(grid(2).cluster().localNode().id()),
            getTestTimeout());
    }

    /** */
    private boolean isDiscoveryNodeAddedMessage(Object msg, int joiningNdeIdx) {
        return msg instanceof TcpDiscoveryNodeAddedMessage &&
            Objects.equals(getTestIgniteInstanceName(joiningNdeIdx),
                ((TcpDiscoveryNodeAddedMessage)msg).node().attribute(ATTR_IGNITE_INSTANCE_NAME));
    }

    /** */
    private BlockingDequeWrapper<TcpDiscoveryAbstractMessage> discoveryRingMessageWorkerQueue(IgniteEx ignite) {
        return U.field(discoveryRingMessageWorker(ignite), "queue");
    }

    /** */
    private boolean anyReceivedMessageMatch(IgniteEx ignite, Predicate<Object> predicate) {
        for (TcpDiscoveryAbstractMessage msg : discoveryRingMessageWorkerQueue(ignite)) {
            Object unwrappedMsg = msg;

            if (msg instanceof TcpDiscoveryCustomEventMessage) {
                TcpDiscoveryCustomEventMessage msg1 = (TcpDiscoveryCustomEventMessage)msg;

                DiscoveryCustomMessage customMsg = msg1.message();

                // Skip unmarshalled message.
                if (customMsg == null) {
                    try {
                        msg1.finishUnmarhal(Marshallers.jdk(), U.gridClassLoader());

                        customMsg = msg1.message();
                    }
                    catch (IgniteCheckedException e) {
                        throw new RuntimeException(e);
                    }
                }

                assert customMsg instanceof SecurityAwareCustomMessageWrapper;

                unwrappedMsg = GridTestUtils.unwrap(customMsg);
            }

            if (predicate.test(unwrappedMsg))
                return true;
        }

        return false;
    }

    /** */
    private void wrapRingMessageWorkerQueue(IgniteEx ignite) throws Exception {
        Object discoMsgWorker = discoveryRingMessageWorker(ignite);

        BlockingDeque<TcpDiscoveryAbstractMessage> queue = U.field(discoMsgWorker, "queue");

        BlockingDequeWrapper<TcpDiscoveryAbstractMessage> wrapper = new BlockingDequeWrapper<>(queue);

        FieldUtils.writeField(discoMsgWorker, "queue", wrapper, true);
    }

    /** */
    private Object discoveryRingMessageWorker(IgniteEx ignite) {
        DiscoverySpi[] discoverySpis = U.field(ignite.context().discovery(), "spis");

        Object impl = U.field(discoverySpis[0], "impl");

        return U.field(impl, "msgWorker");
    }

    /** */
    public static class TestDiscoveryMessage extends AbstractTestDiscoveryMessage {
        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return new TestDiscoveryAcknowledgeMessage();
        }
    }

    /** */
    public static class TestDiscoveryAcknowledgeMessage extends AbstractTestDiscoveryMessage { }

    /** */
    public abstract static class AbstractTestDiscoveryMessage implements DiscoveryCustomMessage {
        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return IgniteUuid.randomUuid();
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(
            GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache
        ) {
            throw new UnsupportedOperationException();
        }
    }

    /** */
    public static class BlockingDequeWrapper<T> implements BlockingDeque<T> {
        /** */
        private volatile boolean isBlocked;

        /** */
        private final BlockingDeque<T> delegate;

        /** */
        public BlockingDequeWrapper(BlockingDeque<T> delegate) {
            this.delegate = delegate;
        }

        /** */
        public void block() {
            isBlocked = true;
        }

        /** */
        public void unblock() {
            isBlocked = false;
        }

        /** {@inheritDoc} */
        @Override public T poll(long timeout, TimeUnit unit) throws InterruptedException {
            return isBlocked ? null : delegate.poll(timeout, unit);
        }

        /** {@inheritDoc} */
        @Override public int remainingCapacity() {
            return delegate.remainingCapacity();
        }

        /** {@inheritDoc} */
        @NotNull @Override public T element() {
            return delegate.element();
        }

        /** {@inheritDoc} */
        @Override public T peek() {
            return delegate.peek();
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            return delegate.remove(o);
        }

        /** {@inheritDoc} */
        @Override public boolean containsAll(@NotNull Collection<?> c) {
            return delegate.containsAll(c);
        }

        /** {@inheritDoc} */
        @Override public boolean addAll(@NotNull Collection<? extends T> c) {
            return delegate.addAll(c);
        }

        /** {@inheritDoc} */
        @Override public boolean removeAll(@NotNull Collection<?> c) {
            return delegate.removeAll(c);
        }

        /** {@inheritDoc} */
        @Override public boolean retainAll(@NotNull Collection<?> c) {
            return delegate.retainAll(c);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            delegate.clear();
        }

        /** {@inheritDoc} */
        @Override public void addFirst(T t) {
            delegate.addFirst(t);
        }

        /** {@inheritDoc} */
        @Override public void addLast(@NotNull T t) {
            delegate.addLast(t);
        }

        /** {@inheritDoc} */
        @Override public boolean offerFirst(@NotNull T t) {
            return delegate.offerFirst(t);
        }

        /** {@inheritDoc} */
        @Override public boolean offerLast(@NotNull T t) {
            return delegate.offerLast(t);
        }

        /** {@inheritDoc} */
        @Override public T removeFirst() {
            return delegate.removeFirst();
        }

        /** {@inheritDoc} */
        @Override public T removeLast() {
            return delegate.removeLast();
        }

        /** {@inheritDoc} */
        @Override public T pollFirst() {
            return delegate.pollFirst();
        }

        /** {@inheritDoc} */
        @Override public T pollLast() {
            return delegate.pollLast();
        }

        /** {@inheritDoc} */
        @Override public T getFirst() {
            return delegate.getFirst();
        }

        /** {@inheritDoc} */
        @Override public T getLast() {
            return delegate.getLast();
        }

        /** {@inheritDoc} */
        @Override public T peekFirst() {
            return delegate.peekFirst();
        }

        /** {@inheritDoc} */
        @Override public T peekLast() {
            return delegate.peekLast();
        }

        /** {@inheritDoc} */
        @Override public void putFirst(@NotNull T t) throws InterruptedException {
            delegate.putFirst(t);
        }

        /** {@inheritDoc} */
        @Override public void putLast(@NotNull T t) throws InterruptedException {
            delegate.putLast(t);
        }

        /** {@inheritDoc} */
        @Override public boolean offerFirst(@NotNull T t, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.offerFirst(t, timeout, unit);
        }

        /** {@inheritDoc} */
        @Override public boolean offerLast(@NotNull T t, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.offerLast(t, timeout, unit);
        }

        /** {@inheritDoc} */
        @NotNull @Override public T takeFirst() throws InterruptedException {
            return delegate.takeFirst();
        }

        /** {@inheritDoc} */
        @NotNull @Override public T takeLast() throws InterruptedException {
            return delegate.takeLast();
        }

        /** {@inheritDoc} */
        @Nullable @Override public T pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.pollFirst(timeout, unit);
        }

        /** {@inheritDoc} */
        @Nullable @Override public T pollLast(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.pollLast(timeout, unit);
        }

        /** {@inheritDoc} */
        @Override public boolean removeFirstOccurrence(Object o) {
            return delegate.removeFirstOccurrence(o);
        }

        /** {@inheritDoc} */
        @Override public boolean removeLastOccurrence(Object o) {
            return delegate.removeLastOccurrence(o);
        }

        /** {@inheritDoc} */
        @Override public boolean add(T t) {
            return delegate.add(t);
        }

        /** {@inheritDoc} */
        @Override public boolean offer(@NotNull T t) {
            return delegate.offer(t);
        }

        /** {@inheritDoc} */
        @Override public void put(@NotNull T t) throws InterruptedException {
            delegate.put(t);
        }

        /** {@inheritDoc} */
        @Override public boolean offer(@NotNull T t, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.offer(t, timeout, unit);
        }

        /** {@inheritDoc} */
        @NotNull @Override public T remove() {
            return delegate.remove();
        }

        /** {@inheritDoc} */
        @Override public T poll() {
            return delegate.poll();
        }

        /** {@inheritDoc} */
        @NotNull @Override public T take() throws InterruptedException {
            return delegate.take();
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            return delegate.contains(o);
        }

        /** {@inheritDoc} */
        @Override public int drainTo(@NotNull Collection<? super T> c) {
            return delegate.drainTo(c);
        }

        /** {@inheritDoc} */
        @Override public int drainTo(@NotNull Collection<? super T> c, int maxElements) {
            return delegate.drainTo(c, maxElements);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return delegate.size();
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return delegate.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public Iterator<T> iterator() {
            return delegate.iterator();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Object[] toArray() {
            return delegate.toArray();
        }

        /** {@inheritDoc} */
        @NotNull @Override public <T1> T1[] toArray(@NotNull T1[] a) {
            return delegate.toArray(a);
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> descendingIterator() {
            return delegate.descendingIterator();
        }

        /** {@inheritDoc} */
        @Override public void push(@NotNull T t) {
            delegate.push(t);
        }

        /** {@inheritDoc} */
        @Override public T pop() {
            return delegate.pop();
        }
    }
}
