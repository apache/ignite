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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite events proxy for ignite instance at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteEventsProcessProxy implements IgniteEvents {
    /** Ignite proxy. */
    private final transient IgniteProcessProxy igniteProxy;

    /** Grid id. */
    private final UUID gridId;

    /**
     * @param igniteProxy Ignite proxy.
     */
    public IgniteEventsProcessProxy(IgniteProcessProxy igniteProxy) {
        this.igniteProxy = igniteProxy;

        gridId = igniteProxy.getId();
    }

    /**
     * @return Events instance.
     */
    private IgniteEvents events() {
        return Ignition.ignite(gridId).events();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup clusterGroup() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> List<T> remoteQuery(IgnitePredicate<T> p, long timeout,
        @Nullable int... types) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> UUID remoteListen(@Nullable IgniteBiPredicate<UUID, T> locLsnr,
        @Nullable IgnitePredicate<T> rmtFilter, @Nullable int... types) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> UUID remoteListen(int bufSize, long interval, boolean autoUnsubscribe,
        @Nullable IgniteBiPredicate<UUID, T> locLsnr, @Nullable IgnitePredicate<T> rmtFilter,
        @Nullable int... types) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void stopRemoteListen(UUID opId) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> T waitForLocal(@Nullable IgnitePredicate<T> filter,
        @Nullable int... types) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> Collection<T> localQuery(IgnitePredicate<T> p, @Nullable int... types) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void recordLocal(Event evt) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void localListen(final IgnitePredicate<? extends Event> lsnr, final int... types) {
        igniteProxy.remoteCompute().run(new IgniteRunnable() {
            @Override public void run() {
                events().localListen(lsnr, types);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean stopLocalListen(IgnitePredicate<? extends Event> lsnr, @Nullable int... types) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void enableLocal(int... types) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void disableLocal(int... types) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public int[] enabledEvents() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isEnabled(int type) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents withAsync() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }
}