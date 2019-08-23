/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.compute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.stream.Collectors.toSet;

/**
 * Holder class to store information in node local map between data collector task executions.
 */
public class VisorComputeMonitoringHolder {
    /** Task monitoring events holder key. */
    private static final String COMPUTE_MONITORING_HOLDER_KEY = "VISOR_COMPUTE_MONITORING_KEY";

    /** Visors that collect events (instance key -> event types + expire flag) */
    private final Map<String, EventsSession> listeners = new HashMap<>();

    /** If cleanup process not scheduled. */
    private boolean cleanupScheduled = true;

    /** Timeout between disable events check. */
    private static final int CLEANUP_TIMEOUT = 2 * 60 * 1000;

    /**
     * Get holder instance.
     *
     * @param ignite Grid.
     */
    public static VisorComputeMonitoringHolder getInstance(IgniteEx ignite) {
        ConcurrentMap<String, VisorComputeMonitoringHolder> storage = ignite.cluster().nodeLocalMap();

        VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

        if (holder == null) {
            holder = new VisorComputeMonitoringHolder();

            VisorComputeMonitoringHolder holderOld = storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holder);

            return holderOld == null ? holder : holderOld;
        }

        return holder;
    }

    /**
     * Start collect events.
     *
     * @param ignite Grid.
     * @param visorKey unique Visor instance key.
     * @param types Events to enable.
     */
    public void startCollect(IgniteEx ignite, String visorKey, int[] types) {
        synchronized (listeners) {
            if (cleanupScheduled)
                scheduleCleanupJob(ignite);

            listeners.compute(visorKey, (k, v) -> {
                if (v == null)
                    return new EventsSession(types);

                v.addEvents(types);

                return v;
            });

            ignite.events().enableLocal(types);
        }
    }

    /**
     * Disable collect events.
     *
     * @param ignite Grid.
     * @param visorKey Unique Visor instance key.
     */
    public void stopCollect(IgniteEx ignite, String visorKey) {
        synchronized (listeners) {
            EventsSession ses = listeners.remove(visorKey);

            if (ses != null)
                tryDisableEvents(ignite, ses.types());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorComputeMonitoringHolder.class, this);
    }

    /**
     * Check if collection of events may be disabled.
     *
     * @param ignite Grid.
     */
    private void tryDisableEvents(IgniteEx ignite, Set<Integer> expiredEvts) {
        Set<Integer> activeEvts = listeners.values().stream().flatMap(s -> s.types.stream()).collect(toSet());

        expiredEvts.removeAll(activeEvts);

        if (!expiredEvts.isEmpty()) {
            int[] types = expiredEvts.stream().mapToInt(Integer::intValue).toArray();

            ignite.events().disableLocal(types);
        }
    }

    /**
     * Check if collect events may be disable.
     *
     * @param ignite Grid.
     */
    private void removeExpired(IgniteEx ignite) {
        Set<EventsSession> expiredSes = listeners.values().stream()
            .filter(EventsSession::isExpired)
            .collect(toSet());

        if (expiredSes.isEmpty())
            return;

        listeners.values().removeAll(expiredSes);

        Set<Integer> expiredEvts = expiredSes.stream().flatMap((s) -> s.types.stream()).collect(toSet());

        tryDisableEvents(ignite, expiredEvts);
    }

    /**
     * Schedule cleanup process for events monitoring.
     *
     * @param ignite grid.
     */
    private void scheduleCleanupJob(final IgniteEx ignite) {
        cleanupScheduled = ignite.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(CLEANUP_TIMEOUT) {
            @Override public void onTimeout() {
                synchronized (listeners) {
                    removeExpired(ignite);

                    for (EventsSession v : listeners.values())
                        v.markExpired();

                    if (listeners.isEmpty())
                        cleanupScheduled = false;
                    else
                        scheduleCleanupJob(ignite);
                }
            }
        });
    }

    /**
     * Session that activate events.
     */
    private static final class EventsSession {
        /** Flag to mark expired session. */
        private boolean expired;

        /** Event types. */
        private Set<Integer> types;

        /**
         * @param types Event types.
         */
        EventsSession(int[] types) {
            expired = true;
            this.types = Arrays.stream(types).boxed().collect(toSet());
        }

        /**
         * @return Flag is session marked as expired.
         */
        boolean isExpired() {
            return expired;
        }

        /**
         * @return Event types.
         */
        Set<Integer> types() {
            return types;
        }

        /**
         * @param types Types.
         */
        void addEvents(int[] types) {
            for (int type : types)
                this.types.add(type);

            expired = false;
        }


        /**
         * Mark session as expired.
         */
        void markExpired() {
            expired = false;
        }
    }
}
