/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.eventstorage;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.eventstorage.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

/**
 * Grid event storage SPI manager.
 */
public class GridEventStorageManager extends GridManagerAdapter<GridEventStorageSpi> {
    /** */
    private static final int[] EMPTY = new int[0];

    /** Local event listeners. */
    private final ConcurrentMap<Integer, Set<GridLocalEventListener>> lsnrs = new ConcurrentHashMap8<>();

    /** Busy lock to control activity of threads. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Is local node daemon? */
    private final boolean isDaemon;

    /** Recordable events arrays length. */
    private final int len;

    /** Marshaller. */
    private final GridMarshaller marsh;

    /** Request listener. */
    private RequestListener msgLsnr;

    /** Events types enabled in configuration. */
    private final int[] cfgInclEvtTypes;

    /** Events of these types should be recorded. */
    private volatile int[] inclEvtTypes;

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
            cfgInclEvtTypes = EMPTY;
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
        // are reserved for internal GridGain events.
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

        for (Set<GridLocalEventListener> lsnrs0 : lsnrs.values())
            lsnrsCnt += lsnrs0.size();

        X.println(">>>");
        X.println(">>> Event storage manager memory stats [grid=" + ctx.gridName() + ']');
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
        return busyLock.readLock().tryLock();
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
        // Acquire write lock so that any new thread could not be started.
        busyLock.writeLock().lock();

        if (msgLsnr != null)
            ctx.io().removeMessageListener(TOPIC_EVENT, msgLsnr);

        msgLsnr = null;

        lsnrs.clear();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        Map<IgnitePredicate<? extends GridEvent>, int[]> evtLsnrs = ctx.config().getLocalEventListeners();

        if (evtLsnrs != null) {
            for (IgnitePredicate<? extends GridEvent> lsnr : evtLsnrs.keySet())
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
    public void record(GridEvent evt) {
        assert evt != null;

        if (!enterBusy())
            return;

        try {
            int type = evt.type();

            if (!isRecordable(type)) {
                LT.warn(log, null, "Trying to record event without checking if it is recordable: " +
                    U.gridEventName(type));
            }

            // Override user recordable settings for daemon node.
            if ((isDaemon || isUserRecordable(type)) && !isHiddenEvent(type))
                try {
                    getSpi().record(evt);
                }
                catch (GridSpiException e) {
                    U.error(log, "Failed to record event: " + evt, e);
                }

            if (isRecordable(type))
                notifyListeners(evt);
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

        ctx.security().authorize(null, GridSecurityPermission.EVENTS_ENABLE, null);

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

        ctx.security().authorize(null, GridSecurityPermission.EVENTS_DISABLE, null);

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
        return type == EVT_NODE_METRICS_UPDATED;
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
        return F.contains(EVTS_DISCOVERY_ALL, type);
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
     * Note that this method supports only predefined GridGain events.
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
    public void addLocalEventListener(IgnitePredicate<? extends GridEvent> lsnr, int[] types) {
        try {
            ctx.resource().injectGeneric(lsnr);
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to inject resources to event listener: " + lsnr, e);
        }

        addLocalEventListener(new UserListenerWrapper(lsnr), types);
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

        if (!enterBusy())
            return;

        try {
            for (int t : types) {
                getOrCreate(t).add(lsnr);

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
    public void addLocalEventListener(GridLocalEventListener lsnr, int type, @Nullable int... types) {
        assert lsnr != null;

        if (!enterBusy())
            return;

        try {
            getOrCreate(type).add(lsnr);

            if (!isRecordable(type))
                U.warn(log, "Added listener for disabled event type: " + U.gridEventName(type));

            if (types != null) {
                for (int t : types) {
                    getOrCreate(t).add(lsnr);

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
     * @param type Event type.
     * @return Listeners for given event type.
     */
    private Collection<GridLocalEventListener> getOrCreate(Integer type) {
        Set<GridLocalEventListener> set = lsnrs.get(type);

        if (set == null) {
            set = new GridConcurrentLinkedHashSet<>();

            Set<GridLocalEventListener> prev = lsnrs.putIfAbsent(type, set);

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
    public boolean removeLocalEventListener(IgnitePredicate<? extends GridEvent> lsnr, @Nullable int... types) {
        return removeLocalEventListener(new UserListenerWrapper(lsnr), types);
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

        boolean found = false;

        if (F.isEmpty(types)) {
            for (Set<GridLocalEventListener> set : lsnrs.values())
                if (set.remove(lsnr))
                    found = true;
        }
        else {
            assert types != null;

            for (int type : types) {
                Set<GridLocalEventListener> set = lsnrs.get(type);

                if (set != null && set.remove(lsnr))
                    found = true;
            }
        }

        return found;
    }

    /**
     *
     * @param p Optional predicate.
     * @param types Event types to wait for.
     * @return Event future.
     */
    public <T extends GridEvent> GridFuture<T> waitForEvent(@Nullable final IgnitePredicate<T> p,
        @Nullable int... types) {
        final GridFutureAdapter<T> fut = new GridFutureAdapter<>(ctx);

        addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                if (p == null || p.apply((T)evt)) {
                    fut.onDone((T)evt);

                    removeLocalEventListener(this);
                }
            }
        }, F.isEmpty(types) ? GridEventType.EVTS_ALL : types);

        return fut;
    }

    /**
     *
     * @param timeout Timeout.
     * @param c Optional continuation.
     * @param p Optional predicate.
     * @param types Event types to wait for.
     * @return Event.
     * @throws GridException Thrown in case of any errors.
     */
    public GridEvent waitForEvent(long timeout, @Nullable Runnable c,
        @Nullable final IgnitePredicate<? super GridEvent> p, int... types) throws GridException {
        assert timeout >= 0;

        final GridFutureAdapter<GridEvent> fut = new GridFutureAdapter<>(ctx);

        addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
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
            throw new GridException(e);
        }

        return fut.get(timeout);
    }

    /**
     * @param evt Event to notify about.
     */
    private void notifyListeners(GridEvent evt) {
        assert evt != null;

        notifyListeners(lsnrs.get(evt.type()), evt);
    }

    /**
     * @param set Set of listeners.
     * @param evt Grid event.
     */
    private void notifyListeners(@Nullable Collection<GridLocalEventListener> set, GridEvent evt) {
        assert evt != null;

        if (!F.isEmpty(set)) {
            assert set != null;

            for (GridLocalEventListener lsnr : set) {
                try {
                    lsnr.onEvent(evt);
                }
                catch (Throwable e) {
                    U.error(log, "Unexpected exception in listener notification for event: " + evt, e);
                }
            }
        }
    }

    /**
     * @param p Grid event predicate.
     * @return Collection of grid events.
     */
    public <T extends GridEvent> Collection<T> localEvents(IgnitePredicate<T> p) {
        assert p != null;

        return getSpi().localEvents(p);
    }

    /**
     * @param p Grid event predicate.
     * @param nodes Collection of nodes.
     * @param timeout Maximum time to wait for result, if {@code 0}, then wait until result is received.
     * @return Collection of events.
     */
    public <T extends GridEvent> GridFuture<List<T>> remoteEventsAsync(final IgnitePredicate<T> p,
        final Collection<? extends ClusterNode> nodes, final long timeout) {
        assert p != null;
        assert nodes != null;

        final GridFutureAdapter<List<T>> fut = new GridFutureAdapter<>(ctx);

        ctx.closure().runLocalSafe(new GPR() {
            @Override public void run() {
                try {
                    fut.onDone(query(p, nodes, timeout));
                }
                catch (GridException e) {
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
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "deprecation"})
    private <T extends GridEvent> List<T> query(IgnitePredicate<T> p, Collection<? extends ClusterNode> nodes,
        long timeout) throws GridException {
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
            @Override public void onEvent(GridEvent evt) {
                assert evt instanceof GridDiscoveryEvent;

                synchronized (qryMux) {
                    uids.remove(((GridDiscoveryEvent)evt).eventNode().id());

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
                        res.events(marsh.<Collection<GridEvent>>unmarshal(res.eventsBytes(), null));

                    if (res.exceptionBytes() != null)
                        res.exception(marsh.<Throwable>unmarshal(res.exceptionBytes(), null));
                }
                catch (GridException e) {
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

            byte[] serFilter = marsh.marshal(p);

            GridDeployment dep = ctx.deploy().deploy(p.getClass(), U.detectClassLoader(p.getClass()));

            if (dep == null)
                throw new GridDeploymentException("Failed to deploy event filter: " + p);

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

                    throw new GridException("Got interrupted while waiting for event query responses.", e);
                }

                if (err.get() != null)
                    throw new GridException("Failed to query events due to exception on remote node.", err.get());

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
                    throw new GridException("Failed to receive event query response from following nodes: " +
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
     * @throws GridException If sending failed.
     */
    private void sendMessage(Collection<? extends ClusterNode> nodes, GridTopic topic,
        GridEventStorageMessage msg, GridIoPolicy plc) throws GridException {
        ClusterNode locNode = F.find(nodes, null, F.localNode(ctx.localNodeId()));

        Collection<? extends ClusterNode> rmtNodes = F.view(nodes, F.remoteNodes(ctx.localNodeId()));

        if (locNode != null)
            ctx.io().send(locNode, topic, msg, plc);

        if (!rmtNodes.isEmpty()) {
            msg.responseTopicBytes(marsh.marshal(msg.responseTopic()));

            ctx.io().send(rmtNodes, topic, msg, plc);
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

                IgnitePredicate<GridEvent> filter = null;

                Collection<GridEvent> evts;

                try {
                    if (req.responseTopicBytes() != null)
                        req.responseTopic(marsh.unmarshal(req.responseTopicBytes(), null));

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
                        throw new GridDeploymentException("Failed to obtain deployment for event filter " +
                            "(is peer class loading turned on?): " + req);

                    filter = marsh.unmarshal(req.filter(), dep.classLoader());

                    // Resource injection.
                    ctx.resource().inject(dep, dep.deployedClass(req.filterClassName()), filter);

                    // Get local events.
                    evts = localEvents(filter);
                }
                catch (GridException e) {
                    U.error(log, "Failed to query events [nodeId=" + nodeId + ", filter=" + filter + ']', e);

                    evts = Collections.emptyList();

                    ex = e;
                }
                catch (Throwable e) {
                    U.error(log, "Failed to query events due to user exception [nodeId=" + nodeId +
                        ", filter=" + filter + ']', e);

                    evts = Collections.emptyList();

                    ex = e;
                }

                // Response message.
                GridEventStorageMessage res = new GridEventStorageMessage(evts, ex);

                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending event query response to node [nodeId=" + nodeId + "res=" + res + ']');

                    if (!ctx.localNodeId().equals(nodeId)) {
                        res.eventsBytes(marsh.marshal(res.events()));
                        res.exceptionBytes(marsh.marshal(res.exception()));
                    }

                    ctx.io().send(node, req.responseTopic(), res, PUBLIC_POOL);
                }
                catch (GridException e) {
                    U.error(log, "Failed to send event query response to node [node=" + nodeId + ", res=" +
                        res + ']', e);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * Wraps user listener predicate provided via {@link GridEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)}.
     */
    private class UserListenerWrapper implements GridLocalEventListener {
        /** */
        private final IgnitePredicate<GridEvent> lsnr;

        /**
         * @param lsnr User listener predicate.
         */
        private UserListenerWrapper(IgnitePredicate<? extends GridEvent> lsnr) {
            this.lsnr = (IgnitePredicate<GridEvent>)lsnr;
        }

        /**
         * @return User listener.
         */
        private IgnitePredicate<? extends GridEvent> listener() {
            return lsnr;
        }

        /** {@inheritDoc} */
        @Override public void onEvent(GridEvent evt) {
            if (!lsnr.apply(evt))
                removeLocalEventListener(this);
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
