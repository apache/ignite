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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridTimerTask;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 *
 */
public class ClusterProcessor extends GridProcessorAdapter {
    /** */
    private static final String ATTR_UPDATE_NOTIFIER_STATUS = "UPDATE_NOTIFIER_STATUS";

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_DELAY = 1000 * 60 * 60; // Every hour.

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_CONN_TIMEOUT = 10 * 1000; // 10 seconds.

    /** */
    private IgniteClusterImpl cluster;

    /** */
    private final AtomicBoolean notifyEnabled = new AtomicBoolean();

    /** */
    @GridToStringExclude
    private Timer updateNtfTimer;

    /** Version checker. */
    @GridToStringExclude
    private GridUpdateNotifier verChecker;

    /**
     * @param ctx Kernal context.
     */
    public ClusterProcessor(GridKernalContext ctx) {
        super(ctx);

        notifyEnabled.set(IgniteSystemProperties.getBoolean(IGNITE_UPDATE_NOTIFIER,
            Boolean.parseBoolean(IgniteProperties.get("ignite.update.notifier.enabled.by.default"))));

        cluster = new IgniteClusterImpl(ctx);
    }

    /**
     * @return Cluster.
     */
    public IgniteClusterImpl get() {
        return cluster;
    }

    /**
     * @return Client reconnect future.
     */
    public IgniteFuture<?> clientReconnectFuture() {
        IgniteFuture<?> fut = cluster.clientReconnectFuture();

        return fut != null ? fut : new IgniteFinishedFutureImpl<>();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.CLUSTER_PROC;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable collectDiscoveryData(UUID nodeId) {
        HashMap<String, Object> map = new HashMap<>();

        map.put(ATTR_UPDATE_NOTIFIER_STATUS, notifyEnabled.get());

        return map;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onDiscoveryDataReceived(UUID joiningNodeId, UUID rmtNodeId, Serializable data) {
        if (joiningNodeId.equals(ctx.localNodeId())) {
            Map<String, Object> map = (Map<String, Object>)data;

            if (map != null && map.containsKey(ATTR_UPDATE_NOTIFIER_STATUS))
                notifyEnabled.set((Boolean)map.get(ATTR_UPDATE_NOTIFIER_STATUS));
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (notifyEnabled.get()) {
            try {
                verChecker = new GridUpdateNotifier(ctx.gridName(),
                    VER_STR,
                    ctx.gateway(),
                    ctx.plugins().allProviders(),
                    false);

                updateNtfTimer = new Timer("ignite-update-notifier-timer", true);

                // Setup periodic version check.
                updateNtfTimer.scheduleAtFixedRate(
                    new UpdateNotifierTimerTask((IgniteKernal)ctx.grid(), verChecker, notifyEnabled),
                    0, PERIODIC_VER_CHECK_DELAY);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to create GridUpdateNotifier: " + e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // Cancel update notification timer.
        if (updateNtfTimer != null)
            updateNtfTimer.cancel();

        if (verChecker != null)
            verChecker.stop();

    }

    /**
     * Disables update notifier.
     */
    public void disableUpdateNotifier() {
        notifyEnabled.set(false);
    }

    /**
     * @return Update notifier status.
     */
    public boolean updateNotifierEnabled() {
        return notifyEnabled.get();
    }

    /**
     * @return Latest version string.
     */
    public String latestVersion() {
        return verChecker != null ? verChecker.latestVersion() : null;
    }

    /**
     * Update notifier timer task.
     */
    private static class UpdateNotifierTimerTask extends GridTimerTask {
        /** Reference to kernal. */
        private final WeakReference<IgniteKernal> kernalRef;

        /** Logger. */
        private final IgniteLogger log;

        /** Version checker. */
        private final GridUpdateNotifier verChecker;

        /** Whether this is the first run. */
        private boolean first = true;

        /** */
        private final AtomicBoolean notifyEnabled;

        /**
         * Constructor.
         *
         * @param kernal Kernal.
         * @param verChecker Version checker.
         */
        private UpdateNotifierTimerTask(IgniteKernal kernal, GridUpdateNotifier verChecker,
            AtomicBoolean notifyEnabled) {
            kernalRef = new WeakReference<>(kernal);

            log = kernal.context().log(UpdateNotifierTimerTask.class);

            this.verChecker = verChecker;
            this.notifyEnabled = notifyEnabled;
        }

        /** {@inheritDoc} */
        @Override public void safeRun() throws InterruptedException {
            if (!notifyEnabled.get())
                return;

            if (!first) {
                IgniteKernal kernal = kernalRef.get();

                if (kernal != null)
                    verChecker.topologySize(kernal.cluster().nodes().size());
            }

            verChecker.checkForNewVersion(log);

            // Just wait for 10 secs.
            Thread.sleep(PERIODIC_VER_CHECK_CONN_TIMEOUT);

            // Just wait another 60 secs in order to get
            // version info even on slow connection.
            for (int i = 0; i < 60 && verChecker.latestVersion() == null; i++)
                Thread.sleep(1000);

            // Report status if one is available.
            // No-op if status is NOT available.
            verChecker.reportStatus(log);

            if (first) {
                first = false;

                verChecker.reportOnlyNew(true);
            }
        }
    }
}
