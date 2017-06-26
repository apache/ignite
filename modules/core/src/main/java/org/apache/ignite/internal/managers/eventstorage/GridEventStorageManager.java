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

package org.apache.ignite.internal.managers.eventstorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.platform.PlatformEventFilterListener;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.GPR;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.eventstorage.EventStorageSpi;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVTS_ALL;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY_ALL;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.internal.GridTopic.TOPIC_EVENT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 * Grid event storage SPI manager.
 */
public class GridEventStorageManager extends GridManagerAdapter<EventStorageSpi> {
    /** Local event listeners. */
    private final ConcurrentMap<Integer, Set<EventListener>> lsnrs = new ConcurrentHashMap8<>();

    /** Busy lock to control activity of threads. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Is local node daemon? */
    private final boolean isDaemon;

    /** Recordable events arrays length. */
    private final int len;

    /** Marshaller. */
    private final Marshaller marsh;

    /** Request listener. */
    private RequestListener msgLsnr;

    /** Events types enabled in configuration. */
    private final int[] cfgInclEvtTypes;

    /** Events of these types should be recorded. */
    private volatile int[] inclEvtTypes;

    /** */
    private boolean stopped;

    /**
     * Maps event type to boolean ({@code true} for recordable events).
     * This array is used for listeners notification. It may be wider,
     * than {@link #userRecordableEvts} since it always contain internal
     * events which are required for system.
     */
    private volatile boolean[] recordableEvts;

    /**
     * Maps user recordable event type to boolean ({@code true} for recordable events).
     * This array is used for event recording with configured SPI. It may contain
     * less elements, than {@link #recordableEvts}, since it contains only those
     * events which are intended to be recorded with configured SPI.
     */
    private volatile boolean[] userRecordableEvts;

    /**
     * @param ctx Kernal context.
     */
    public GridEventStorageManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getEventStorageSpi());

        marsh = ctx.config().getMarshaller();

        isDaemon = ctx.isDaemon();

        int[] cfgInclEvtTypes0 = ctx.config().getIncludeEventTypes();

        if (F.isEmpty(cfgInclEvtTypes0))
            cfgInclEvtTypes = U.EMPTY_INTS;
        else {
            cfgInclEvtTypes0 = copy(cfgInclEvtTypes0);

            Arrays.sort(cfgInclEvtTypes0);

            if (cfgInclEvtTypes0[0] < 0)
                throw new IllegalArgumentException("Invalid event type: " + cfgInclEvtTypes0[0]);

            cfgInclEvtTypes = compact(cfgInclEvtTypes0, cfgInclEvtTypes0.length);
        }

        // Initialize recordable events arrays.
        int maxIdx = 0;

        for (int type : EVTS_ALL) {
            if (type > maxIdx)
                maxIdx = type;
        }

        // Javadoc to GridEventType states that all types in range from 1 to 1000
        // are reserved for internal Ignite events.
        assert maxIdx <= 1000 : "Invalid max index: " + maxIdx;

        // We don't want to pre-process passed in types,
        // but use them directly as indexes.
        // So, we need to allocate bigger array.
        len = maxIdx + 1;

        boolean[] recordableEvts = new boolean[len];
        boolean[] userRecordableEvts = new boolean[len];

        Collection<Integer> inclEvtTypes0 = new HashSet<>(U.toIntList(cfgInclEvtTypes));

        // Internal events are always "recordable" for notification
        // purposes (regardless of whether they were enabled or disabled).
        // However, won't be sent down to SPI level if user specifically excluded them.
        for (int type : EVTS_ALL) {
            boolean userRecordable = inclEvtTypes0.remove(type);

            if (userRecordable)
                userRecordableEvts[type] = true;

            // Internal event or user recordable event.
            if (isInternalEvent(type) || userRecordable)
                recordableEvts[type] = true;

            if (log.isDebugEnabled())
                log.debug("Event recordable status [type=" + U.gridEventName(type) +
                    ", recordable=" + recordableEvts[type] +
                    ", userRecordable=" + userRecordableEvts[type] + ']');
        }

        this.recordableEvts = recordableEvts;
        this.userRecordableEvts = userRecordableEvts;

        int[] inclEvtTypes = U.toIntArray(inclEvtTypes0);

        Arrays.sort(inclEvtTypes);

        this.inclEvtTypes = inclEvtTypes;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        int lsnrsCnt = 0;

        for (Set<EventListener> lsnrs0 : lsnrs.values())
            lsnrsCnt += lsnrs0.size();

        X.println(">>>");
        X.println(">>> Event storage manager memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>  Total listeners: " + lsnrsCnt);
        X.println(">>>  Recordable events size: " + recordableEvts.length);
        X.println(">>>  User recordable events size: " + userRecordableEvts.length);
    }

    /**
     * Enters busy state in which manager cannot be stopped.
     *
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (!busyLock.readLock().tryLock())
            return false;

        if (stopped) {
            busyLock.readLock().unlock();

            return false;
        }

        return true;
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public void onKernalStop0(boolean cancel) {
        busyLock.writeLock().lock();

        try {
            if (msgLsnr != null)
                ctx.io().removeMessageListener(
                    TOPIC_EVENT,
                    msgLsnr);

            msgLsnr = null;

            lsnrs.clear();

            stopped = true;
        }
        finally {
            busyLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        Map<IgnitePredicate<? extends Event>, int[]> evtLsnrs = ctx.config().getLocalEventListeners();

        if (evtLsnrs != null) {
            for (IgnitePredicate<? extends Event> lsnr : evtLsnrs.keySet())
                addLocalEventListener(lsnr, evtLsnrs.get(lsnr));
        }

        startSpi();

        msgLsnr = new RequestListener();

        ctx.io().addMessageListener(TOPIC_EVENT, msgLsnr);

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * Records event if it's recordable.
     *
     * @param evt Event to record.
     */
    public void record(Event evt) {
        record0(evt);
    }

    /**
     * Records discovery events.
     *
     * @param evt Event to record.
     * @param discoCache Discovery cache.
     */
    public void record(DiscoveryEvent evt, DiscoCache discoCache) {
        record0(evt, discoCache);
    }

    /**
     * Records event if it's recordable.
     *
     * @param evt Event to record.
     * @param params Additional parameters.
     */
    private void record0(Event evt, Object... params) {
        assert evt != null;

        if (!enterBusy())
            return;

        try {
            int type = evt.type();

            if (!isRecordable(type)) {
                LT.warn(log, "Trying to record event without checking if it is recordable: " +
                    U.gridEventName(type));
            }

            // Override user recordable settings for daemon node.
            if ((isDaemon || isUserRecordable(type)) && !isHiddenEvent(type))
                try {
                    getSpi().record(evt);
                }
                catch (IgniteSpiException e) {
                    U.error(log, "Failed to record event: " + evt, e);
                }

            if (isRecordable(type))
                notifyListeners(lsnrs.get(evt.type()), evt, params);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Gets types of enabled user-recordable events.
     *
     * @return Array of types of enabled user-recordable events.
     */
    public int[] enabledEvents() {
        boolean[] userRecordableEvts0 = userRecordableEvts;

        int[] enabledEvts = new int[len];
        int enabledEvtsLen = 0;

        for (int type = 0; type < len; type++) {
            if (userRecordableEvts0[type])
                enabledEvts[enabledEvtsLen++] = type;
        }

        return U.unique(enabledEvts, enabledEvtsLen, inclEvtTypes, inclEvtTypes.length);
    }

    /**
     * Enables provided events.
     *
     * @param types Events to enable.
     */
    public synchronized void enableEvents(int[] types) {
        assert types != null;

        ctx.security().authorize(null, SecurityPermission.EVENTS_ENABLE, null);

        boolean[] userRecordableEvts0 = userRecordableEvts;
        boolean[] recordableEvts0 = recordableEvts;
        int[] inclEvtTypes0 = inclEvtTypes;

        int[] userTypes = new int[types.length];
        int userTypesLen = 0;

        for (int type : types) {
            if (type < len) {
                userRecordableEvts0[type] = true;
                recordableEvts0[type] = true;
            }
            else
                userTypes[userTypesLen++] = type;
        }

        if (userTypesLen > 0) {
            Arrays.sort(userTypes, 0, userTypesLen);

            userTypes = compact(userTypes, userTypesLen);

            inclEvtTypes0 = U.unique(inclEvtTypes0, inclEvtTypes0.length, userTypes, userTypesLen);
        }

        // Volatile write.
        // The below line is intentional to ensure a volatile write is
        // made to the array, since it is exist access via unsynchronized blocks.
        userRecordableEvts = userRecordableEvts0;
        recordableEvts = recordableEvts0;
        inclEvtTypes = inclEvtTypes0;
    }

    /**
     * Disables provided events.
     *
     * @param types Events to disable.
     */
    @SuppressWarnings("deprecation")
    public synchronized void disableEvents(int[] types) {
        assert types != null;

        ctx.security().authorize(null, SecurityPermission.EVENTS_DISABLE, null);

        boolean[] userRecordableEvts0 = userRecordableEvts;
        boolean[] recordableEvts0 = recordableEvts;
        int[] inclEvtTypes0 = inclEvtTypes;

        int[] userTypes = new int[types.length];
        int userTypesLen = 0;

        for (int type : types) {
            if (binarySearch(cfgInclEvtTypes, type)) {
                U.warn(log, "Can't disable event since it was enabled in configuration: " + U.gridEventName(type));

                continue;
            }

            if (type < len) {
                userRecordableEvts0[type] = false;

                if (!isInternalEvent(type))
                    recordableEvts0[type] = false;
            }
            else
                userTypes[userTypesLen++] = type;
        }

        if (userTypesLen > 0) {
            Arrays.sort(userTypes, 0, userTypesLen);

            userTypes = compact(userTypes, userTypesLen);

            inclEvtTypes0 = U.difference(inclEvtTypes0, inclEvtTypes0.length, userTypes, userTypesLen);
        }

        // Volatile write.
        // The below line is intentional to ensure a volatile write is
        // made to the array, since it is exist access via unsynchronized blocks.
        userRecordableEvts = userRecordableEvts0;
        recordableEvts = recordableEvts0;
        inclEvtTypes = inclEvtTypes0;
    }

    /**
     * Removes duplicates in non-decreasing array.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return Arrays with removed duplicates.
     */
    private int[] compact(int[] arr, int len) {
        assert arr != null;
        assert U.isNonDecreasingArray(arr, len);

        if (arr.length <= 1)
            return U.copyIfExceeded(arr, len);

        int newLen = 1;

        for (int i = 1; i < len; i++) {
            if (arr[i] != arr[newLen - 1])
                arr[newLen++] = arr[i];
        }

        return U.copyIfExceeded(arr, len);
    }

    /**
     * Checks whether or not this event is a hidden system event.
     * <p>
     * Hidden events are NEVER sent to SPI level. They serve purpose of local
     * notification for the local node.
     *
     * @param type Event type to check.
     * @return {@code true} if this is a system hidden event.
     */
    private boolean isHiddenEvent(int type) {
        return type == EVT_NODE_METRICS_UPDATED || type == EVT_DISCOVERY_CUSTOM_EVT;
    }

    /**
     * Checks whether or not this event is an internal event.
     * <p>
     * Internal event types are always recordable for notification purposes
     * but may not be sent down to SPI level for storage and subsequent querying.
     *
     * @param type Event type.
     * @return {@code true} if this is an internal event.
     */
    private boolean isInternalEvent(int type) {
        return type == EVT_DISCOVERY_CUSTOM_EVT || F.contains(EVTS_DISCOVERY_ALL, type);
    }

    /**
     * Checks if the event type is user-recordable.
     *
     * @param type Event type to check.
     * @return {@code true} if passed event should be recorded, {@code false} - otherwise.
     */
    public boolean isUserRecordable(int type) {
        assert type > 0 : "Invalid event type: " + type;

        return type < len ? userRecordableEvts[type] : isUserRecordable0(type);
    }

    /**
     * Checks whether this event type should be recorded. Note that internal event types are
     * always recordable for notification purposes but may not be sent down to SPI level for
     * storage and subsequent querying.
     *
     * @param type Event type to check.
     * @return Whether or not this event type should be recorded.
     */
    public boolean isRecordable(int type) {
        assert type > 0 : "Invalid event type: " + type;

        return type < len ? recordableEvts[type] : isUserRecordable0(type);
    }

    /**
     * Checks whether all provided events are user-recordable.
     * <p>
     * Note that this method supports only predefined Ignite events.
     *
     * @param types Event types.
     * @return Whether all events are recordable.
     * @throws IllegalArgumentException If {@code types} contains user event type.
     */
    public boolean isAllUserRecordable(int[] types) {
        assert types != null;

        boolean[] userRecordableEvts0 = userRecordableEvts;

        for (int type : types) {
            if (type < 0 || type >= len)
                throw new IllegalArgumentException("Invalid event type: " + type);

            if (!userRecordableEvts0[type])
                return false;
        }

        return true;
    }

    /**
     * Checks if the event type is user-recordable against grid configuration.
     *
     * @param type Event type to check.
     * @return {@code true} if passed event should be recorded, {@code false} - otherwise.
     */
    private boolean isUserRecordable0(int type) {
        return binarySearch(inclEvtTypes, type);
    }

    /**
     * @param arr Sorted array to search in.
     * @param val Value.
     * @return {@code True} if value has been found.
     */
    private boolean binarySearch(@Nullable int[] arr, int val) {
        if (F.isEmpty(arr))
            return false;

        // If length is relatively small, full iteration is faster.
        return arr.length <= 128 ? F.contains(arr, val) : Arrays.binarySearch(arr, val) >= 0;
    }

    /**
     * Adds local user event listener.
     *
     * @param lsnr User listener to add.
     * @param types Event types to subscribe listener for.
     */
    public void addLocalEventListener(IgnitePredicate<? extends Event> lsnr, int[] types) {
        assert lsnr != null;

        try {
            ctx.resource().injectGeneric(lsnr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to inject resources to event listener: " + lsnr, e);
        }

        addEventListener(new UserListenerWrapper(lsnr), types);
    }

    /**
     * Adds local event listener. Note that this method specifically disallow an empty
     * array of event type to prevent accidental subscription for all system event that
     * may lead to a drastic performance decrease.
     *
     * @param lsnr Listener to add.
     * @param types Event types to subscribe listener for.
     */
    public void addLocalEventListener(GridLocalEventListener lsnr, int[] types) {
        assert lsnr != null;
        assert types != null;
        assert types.length > 0;

        addEventListener(new LocalListenerWrapper(lsnr), types);
    }

    /**
     * Adds local event listener.
     *
     * @param lsnr Listener to add.
     * @param type Event type to subscribe listener for.
     * @param types Additional event types to subscribe listener for.
     */
    public void addLocalEventListener(GridLocalEventListener lsnr, int type, @Nullable int... types) {
        assert lsnr != null;

        addEventListener(new LocalListenerWrapper(lsnr), type, types);
    }

    /**
     * Adds discovery event listener. Note that this method specifically disallow an empty
     * array of event type to prevent accidental subscription for all system event that
     * may lead to a drastic performance decrease.
     *
     * @param lsnr Listener to add.
     * @param types Event types to subscribe listener for.
     */
    public void addDiscoveryEventListener(DiscoveryEventListener lsnr, int[] types) {
        assert lsnr != null;
        assert types != null;
        assert types.length > 0;

        addEventListener(new DiscoveryListenerWrapper(lsnr), types);
    }

    /**
     * Adds discovery event listener.
     *
     * @param lsnr Listener to add.
     * @param type Event type to subscribe listener for.
     * @param types Additional event types to subscribe listener for.
     */
    public void addDiscoveryEventListener(DiscoveryEventListener lsnr, int type, @Nullable int... types) {
        assert lsnr != null;

        addEventListener(new DiscoveryListenerWrapper(lsnr), type, types);
    }

    /**
     * Adds local event listener. Note that this method specifically disallow an empty
     * array of event type to prevent accidental subscription for all system event that
     * may lead to a drastic performance decrease.
     *
     * @param lsnr Listener to add.
     * @param types Event types to subscribe listener for.
     */
    private void addEventListener(EventListener lsnr, int[] types) {
        if (!enterBusy())
            return;

        try {
            for (int t : types) {
                getOrCreate(lsnrs, t).add(lsnr);

                if (!isRecordable(t))
                    U.warn(log, "Added listener for disabled event type: " + U.gridEventName(t));
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Adds local event listener.
     *
     * @param lsnr Listener to add.
     * @param type Event type to subscribe listener for.
     * @param types Additional event types to subscribe listener for.
     */
    private void addEventListener(EventListener lsnr, int type, @Nullable int... types) {
        if (!enterBusy())
            return;

        try {
            getOrCreate(lsnrs, type).add(lsnr);

            if (!isRecordable(type))
                U.warn(log, "Added listener for disabled event type: " + U.gridEventName(type));

            if (types != null) {
                for (int t : types) {
                    getOrCreate(lsnrs, t).add(lsnr);

                    if (!isRecordable(t))
                        U.warn(log, "Added listener for disabled event type: " + U.gridEventName(t));
                }
            }
        }
        finally {
            leaveBusy();
        }
    }


    /**
     * @param lsnrs Listeners map.
     * @param type Event type.
     * @return Listeners for given event type.
     */
    private <T> Collection<T> getOrCreate(ConcurrentMap<Integer, Set<T>> lsnrs, Integer type) {
        Set<T> set = lsnrs.get(type);

        if (set == null) {
            set = new GridConcurrentLinkedHashSet<>();

            Set<T> prev = lsnrs.putIfAbsent(type, set);

            if (prev != null)
                set = prev;
        }

        assert set != null;

        return set;
    }

    /**
     * Removes user listener for specified events, if any. If no event types provided - it
     * removes the listener for all its registered events.
     *
     * @param lsnr User listener predicate.
     * @param types Event types.
     * @return Returns {@code true} if removed.
     */
    public boolean removeLocalEventListener(IgnitePredicate<? extends Event> lsnr, @Nullable int... types) {
        assert lsnr != null;

        return removeEventListener(new UserListenerWrapper(lsnr), types);
    }

    /**
     * Removes listener for specified events, if any. If no event types provided - it
     * remove the listener for all its registered events.
     *
     * @param lsnr Listener.
     * @param types Event types.
     * @return Returns {@code true} if removed.
     */
    public boolean removeLocalEventListener(GridLocalEventListener lsnr, @Nullable int... types) {
        assert lsnr != null;

        return removeEventListener(new LocalListenerWrapper(lsnr), types);
    }

    /**
     * Removes listener for specified events, if any. If no event types provided - it
     * remove the listener for all its registered events.
     *
     * @param lsnr Listener.
     * @param types Event types.
     * @return Returns {@code true} if removed.
     */
    public boolean removeDiscoveryEventListener(DiscoveryEventListener lsnr, @Nullable int... types) {
        assert lsnr != null;

        return removeEventListener(new DiscoveryListenerWrapper(lsnr), types);
    }

    /**
     * Removes listener for specified events, if any. If no event types provided - it
     * remove the listener for all its registered events.
     *
     * @param lsnr Listener.
     * @param types Event types.
     * @return Returns {@code true} if removed.
     */
    private boolean removeEventListener(EventListener lsnr, @Nullable int[] types) {
        assert lsnr != null;

        boolean found = false;

        if (F.isEmpty(types)) {
            for (Set<EventListener> set : lsnrs.values())
                if (set.remove(lsnr))
                    found = true;
        }
        else {
            assert types != null;

            for (int type : types) {
                Set<EventListener> set = lsnrs.get(type);

                if (set != null && set.remove(lsnr))
                    found = true;
            }
        }

        if (lsnr instanceof UserListenerWrapper)
        {
            IgnitePredicate p = ((UserListenerWrapper)lsnr).listener();

            if (p instanceof PlatformEventFilterListener)
                ((PlatformEventFilterListener)p).onClose();
        }

        return found;
    }

    /**
     *
     * @param p Optional predicate.
     * @param types Event types to wait for.
     * @return Event future.
     */
    public <T extends Event> IgniteInternalFuture<T> waitForEvent(@Nullable final IgnitePredicate<T> p,
        @Nullable int... types) {
        final GridFutureAdapter<T> fut = new GridFutureAdapter<>();

        addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                if (p == null || p.apply((T)evt)) {
                    fut.onDone((T)evt);

                    removeLocalEventListener(this);
                }
            }
        }, F.isEmpty(types) ? EventType.EVTS_ALL : types);

        return fut;
    }

    /**
     *
     * @param timeout Timeout.
     * @param c Optional continuation.
     * @param p Optional predicate.
     * @param types Event types to wait for.
     * @return Event.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public Event waitForEvent(long timeout, @Nullable Runnable c,
        @Nullable final IgnitePredicate<? super Event> p, int... types) throws IgniteCheckedException {
        assert timeout >= 0;

        final GridFutureAdapter<Event> fut = new GridFutureAdapter<>();

        addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                if (p == null || p.apply(evt)) {
                    fut.onDone(evt);

                    removeLocalEventListener(this);
                }
            }
        }, types);

        try {
            if (c != null)
                c.run();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        return fut.get(timeout);
    }

    /**
     * @param set Set of listeners.
     * @param evt Grid event.
     */
    private void notifyListeners(@Nullable Collection<EventListener> set, Event evt, Object[] params) {
        assert evt != null;

        if (!F.isEmpty(set)) {
            assert set != null;

            for (EventListener lsnr : set) {
                try {
                    ((ListenerWrapper)lsnr).onEvent(evt, params);
                }
                catch (Throwable e) {
                    U.error(log, "Unexpected exception in listener notification for event: " + evt, e);

                    if (e instanceof Error)
                        throw (Error)e;
                }
            }
        }
    }

    /**
     * @param p Grid event predicate.
     * @return Collection of grid events.
     */
    @SuppressWarnings("unchecked")
    public <T extends Event> Collection<T> localEvents(IgnitePredicate<T> p) throws IgniteCheckedException {
        assert p != null;

        if (getSpi() instanceof NoopEventStorageSpi) {
            throw new IgniteCheckedException(
                "Failed to query events because default no-op event storage SPI is used. " +
                "Consider configuring " + MemoryEventStorageSpi.class.getSimpleName() + " or another " +
                EventStorageSpi.class.getSimpleName() + " implementation via " +
                "IgniteConfiguration.setEventStorageSpi() configuration property.");
        }

        if (p instanceof PlatformEventFilterListener) {
            PlatformEventFilterListener p0 = (PlatformEventFilterListener)p;

            p0.initialize(ctx);

            try {
                return (Collection<T>)getSpi().localEvents(p0);
            }
            finally {
                p0.onClose();
            }
        }
        else
            return getSpi().localEvents(p);
    }

    /**
     * @param p Grid event predicate.
     * @param nodes Collection of nodes.
     * @param timeout Maximum time to wait for result, if {@code 0}, then wait until result is received.
     * @return Collection of events.
     */
    public <T extends Event> IgniteInternalFuture<List<T>> remoteEventsAsync(final IgnitePredicate<T> p,
        final Collection<? extends ClusterNode> nodes, final long timeout) {
        assert p != null;
        assert nodes != null;

        final GridFutureAdapter<List<T>> fut = new GridFutureAdapter<>();

        ctx.closure().runLocalSafe(new GPR() {
            @Override public void run() {
                try {
                    fut.onDone(query(p, nodes, timeout));
                }
                catch (IgniteCheckedException e) {
                    fut.onDone(e);
                }
            }
        }, true);

        return fut;
    }

    /**
     * @param p Grid event predicate.
     * @param nodes Collection of nodes.
     * @param timeout Maximum time to wait for result, if {@code 0}, then wait until result is received.
     * @return Collection of events.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "deprecation"})
    private <T extends Event> List<T> query(IgnitePredicate<T> p, Collection<? extends ClusterNode> nodes,
        long timeout) throws IgniteCheckedException {
        assert p != null;
        assert nodes != null;

        if (nodes.isEmpty()) {
            U.warn(log, "Failed to query events for empty nodes collection.");

            return Collections.emptyList();
        }

        GridIoManager ioMgr = ctx.io();

        final List<T> evts = new ArrayList<>();

        final AtomicReference<Throwable> err = new AtomicReference<>();

        final Set<UUID> uids = new HashSet<>();

        final Object qryMux = new Object();

        for (ClusterNode node : nodes)
            uids.add(node.id());

        GridLocalEventListener evtLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent;

                synchronized (qryMux) {
                    uids.remove(((DiscoveryEvent)evt).eventNode().id());

                    if (uids.isEmpty())
                        qryMux.notifyAll();
                }
            }
        };

        GridMessageListener resLsnr = new GridMessageListener() {
            @SuppressWarnings("deprecation")
            @Override public void onMessage(UUID nodeId, Object msg) {
                assert nodeId != null;
                assert msg != null;

                if (!(msg instanceof GridEventStorageMessage)) {
                    U.error(log, "Received unknown message: " + msg);

                    return;
                }

                GridEventStorageMessage res = (GridEventStorageMessage)msg;

                try {
                    if (res.eventsBytes() != null)
                        res.events(U.<Collection<Event>>unmarshal(marsh, res.eventsBytes(),
                            U.resolveClassLoader(ctx.config())));

                    if (res.exceptionBytes() != null)
                        res.exception(U.<Throwable>unmarshal(marsh, res.exceptionBytes(),
                            U.resolveClassLoader(ctx.config())));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to unmarshal events query response: " + msg, e);

                    return;
                }

                synchronized (qryMux) {
                    if (uids.remove(nodeId)) {
                        if (res.events() != null)
                            evts.addAll((Collection<T>)res.events());
                    }
                    else
                        U.warn(log, "Received duplicate response (ignoring) [nodeId=" + nodeId +
                            ", msg=" + res + ']');

                    if (res.exception() != null)
                        err.set(res.exception());

                    if (uids.isEmpty() || err.get() != null)
                        qryMux.notifyAll();
                }
            }
        };

        Object resTopic = TOPIC_EVENT.topic(IgniteUuid.fromUuid(ctx.localNodeId()));

        try {
            addLocalEventListener(evtLsnr, new int[] {
                EVT_NODE_LEFT,
                EVT_NODE_FAILED
            });

            ioMgr.addMessageListener(resTopic, resLsnr);

            byte[] serFilter = U.marshal(marsh, p);

            GridDeployment dep = ctx.deploy().deploy(p.getClass(), U.detectClassLoader(p.getClass()));

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy event filter: " + p);

            GridEventStorageMessage msg = new GridEventStorageMessage(
                resTopic,
                serFilter,
                p.getClass().getName(),
                dep.classLoaderId(),
                dep.deployMode(),
                dep.userVersion(),
                dep.participants());

            sendMessage(nodes, TOPIC_EVENT, msg, PUBLIC_POOL);

            if (timeout == 0)
                timeout = Long.MAX_VALUE;

            long now = U.currentTimeMillis();

            // Account for overflow of long value.
            long endTime = now + timeout <= 0 ? Long.MAX_VALUE : now + timeout;

            long delta = timeout;

            Collection<UUID> uidsCp = null;

            synchronized (qryMux) {
                try {
                    while (!uids.isEmpty() && err.get() == null && delta > 0) {
                        qryMux.wait(delta);

                        delta = endTime - U.currentTimeMillis();
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteCheckedException("Got interrupted while waiting for event query responses.", e);
                }

                if (err.get() != null)
                    throw new IgniteCheckedException("Failed to query events due to exception on remote node.", err.get());

                if (!uids.isEmpty())
                    uidsCp = new LinkedList<>(uids);
            }

            // Outside of synchronization.
            if (uidsCp != null) {
                for (Iterator<UUID> iter = uidsCp.iterator(); iter.hasNext();)
                    // Ignore nodes that have left the grid.
                    if (ctx.discovery().node(iter.next()) == null)
                        iter.remove();

                if (!uidsCp.isEmpty())
                    throw new IgniteCheckedException("Failed to receive event query response from following nodes: " +
                        uidsCp);
            }
        }
        finally {
            ioMgr.removeMessageListener(resTopic, resLsnr);

            removeLocalEventListener(evtLsnr);
        }

        return evts;
    }

    /**
     * Sends message accounting for local and remote nodes.
     *
     * @param nodes Nodes to receive event.
     * @param topic Topic to send the message to.
     * @param msg Event to be sent.
     * @param plc Type of processing.
     * @throws IgniteCheckedException If sending failed.
     */
    private void sendMessage(Collection<? extends ClusterNode> nodes, GridTopic topic,
        GridEventStorageMessage msg, byte plc) throws IgniteCheckedException {
        ClusterNode locNode = F.find(nodes, null, F.localNode(ctx.localNodeId()));

        Collection<? extends ClusterNode> rmtNodes = F.view(nodes, F.remoteNodes(ctx.localNodeId()));

        if (locNode != null)
            ctx.io().sendToGridTopic(locNode, topic, msg, plc);

        if (!rmtNodes.isEmpty()) {
            msg.responseTopicBytes(U.marshal(marsh, msg.responseTopic()));

            ctx.io().sendToGridTopic(rmtNodes, topic, msg, plc);
        }
    }

    /**
     * @param arr Array.
     * @return Array copy.
     */
    private int[] copy(int[] arr) {
        assert arr != null;

        return Arrays.copyOf(arr, arr.length);
    }

    /**
     * @param arr Array.
     * @return Array copy.
     */
    private boolean[] copy(boolean[] arr) {
        assert arr != null;

        return Arrays.copyOf(arr, arr.length);
    }

    /**
     *
     */
    private class RequestListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert nodeId != null;
            assert msg != null;

            if (!enterBusy())
                return;

            try {
                if (!(msg instanceof GridEventStorageMessage)) {
                    U.warn(log, "Received unknown message: " + msg);

                    return;
                }

                GridEventStorageMessage req = (GridEventStorageMessage)msg;

                ClusterNode node = ctx.discovery().node(nodeId);

                if (node == null) {
                    U.warn(log, "Failed to resolve sender node that does not exist: " + nodeId);

                    return;
                }

                if (log.isDebugEnabled())
                    log.debug("Received event query request: " + req);

                Throwable ex = null;

                IgnitePredicate<Event> filter = null;

                Collection<Event> evts;

                try {
                    if (req.responseTopicBytes() != null)
                        req.responseTopic(U.unmarshal(marsh, req.responseTopicBytes(), U.resolveClassLoader(ctx.config())));

                    GridDeployment dep = ctx.deploy().getGlobalDeployment(
                        req.deploymentMode(),
                        req.filterClassName(),
                        req.filterClassName(),
                        req.userVersion(),
                        nodeId,
                        req.classLoaderId(),
                        req.loaderParticipants(),
                        null);

                    if (dep == null)
                        throw new IgniteDeploymentCheckedException("Failed to obtain deployment for event filter " +
                            "(is peer class loading turned on?): " + req);

                    filter = U.unmarshal(marsh, req.filter(), U.resolveClassLoader(dep.classLoader(), ctx.config()));

                    // Resource injection.
                    ctx.resource().inject(dep, dep.deployedClass(req.filterClassName()), filter);

                    // Get local events.
                    evts = localEvents(filter);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to query events [nodeId=" + nodeId + ", filter=" + filter + ']', e);

                    evts = Collections.emptyList();

                    ex = e;
                }
                catch (Throwable e) {
                    U.error(log, "Failed to query events due to user exception [nodeId=" + nodeId +
                        ", filter=" + filter + ']', e);

                    evts = Collections.emptyList();

                    ex = e;

                    if (e instanceof Error)
                        throw (Error)e;
                }

                // Response message.
                GridEventStorageMessage res = new GridEventStorageMessage(evts, ex);

                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending event query response to node [nodeId=" + nodeId + "res=" + res + ']');

                    if (!ctx.localNodeId().equals(nodeId)) {
                        res.eventsBytes(U.marshal(marsh, res.events()));
                        res.exceptionBytes(U.marshal(marsh, res.exception()));
                    }

                    ctx.io().sendToCustomTopic(node, req.responseTopic(), res, PUBLIC_POOL);
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send event query response, node failed [node=" + nodeId + ']');
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send event query response to node [node=" + nodeId + ", res=" +
                        res + ']', e);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /** */
    private abstract static class ListenerWrapper implements EventListener {
        abstract void onEvent(Event evt, Object[] params);
    }

    /**
     * Wraps local listener
     */
    private static final class LocalListenerWrapper extends ListenerWrapper {
        /** */
        private final GridLocalEventListener lsnr;

        /**
         * @param lsnr Listener.
         */
        private LocalListenerWrapper(GridLocalEventListener lsnr) {
            this.lsnr = lsnr;
        }

        /** {@inheritDoc} */
        @Override void onEvent(Event evt, Object[] params) {
            lsnr.onEvent(evt);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            LocalListenerWrapper wrapper = (LocalListenerWrapper)o;

            return lsnr.equals(wrapper.lsnr);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return lsnr.hashCode();
        }
    }

    /**
     * Wraps discovery local listener
     */
    private static final class DiscoveryListenerWrapper extends ListenerWrapper {
        /** */
        private final DiscoveryEventListener lsnr;

        /**
         * @param lsnr Listener.
         */
        private DiscoveryListenerWrapper(DiscoveryEventListener lsnr) {
            this.lsnr = lsnr;
        }

        /** {@inheritDoc} */
        @Override void onEvent(Event evt, Object[] params) {
            // No checks there since only DiscoveryManager produses DiscoveryEvents
            // and it uses an overloaded method with additional parameters
            lsnr.onEvent((DiscoveryEvent)evt, (DiscoCache)params[0]);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            DiscoveryListenerWrapper wrapper = (DiscoveryListenerWrapper)o;

            return lsnr.equals(wrapper.lsnr);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return lsnr.hashCode();
        }
    }

    /**
     * Wraps user listener predicate provided via {@link IgniteEvents#localListen(IgnitePredicate, int...)}.
     */
    private final class UserListenerWrapper extends ListenerWrapper {
        /** */
        private final IgnitePredicate<Event> lsnr;

        /**
         * @param lsnr User listener predicate.
         */
        private UserListenerWrapper(IgnitePredicate<? extends Event> lsnr) {
            this.lsnr = (IgnitePredicate<Event>)lsnr;
        }

        /**
         * @return User listener.
         */
        private IgnitePredicate<? extends Event> listener() {
            return lsnr;
        }

        /** {@inheritDoc} */
        @Override void onEvent(Event evt, Object[] params) {
            if (!lsnr.apply(evt))
                removeEventListener(this, null);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            UserListenerWrapper that = (UserListenerWrapper)o;

            return lsnr.equals(that.lsnr);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return lsnr.hashCode();
        }
    }
}
