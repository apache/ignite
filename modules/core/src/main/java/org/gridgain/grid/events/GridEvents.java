/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Provides functionality for local and remote event notifications on nodes defined by {@link #projection()}.
 * There are {@code 2} ways to subscribe to event listening, {@code local} and {@code remote}. Instance
 * of {@code GridMessaging} is obtained from grid projection as follows:
 * <pre name="code" class="java">
 * GridEvents evts = GridGain.grid().events();
 * </pre> * <p>
 * Local subscription, defined by {@link #localListen(org.apache.ignite.lang.IgnitePredicate, int...)} method, will add
 * a listener for specified events on local node only. This listener will be notified whenever any
 * of subscribed events happen on local node regardless of whether local node belongs to underlying
 * grid projection or not.
 * <p>
 * Remote subscription, defined by {@link #remoteListen(org.apache.ignite.lang.IgniteBiPredicate, org.apache.ignite.lang.IgnitePredicate, int...)}, will add an
 * event listener for specified events on all nodes in the projection (possibly including local node if
 * it belongs to the projection as well). All projection nodes will then be notified of the subscribed events.
 * If the events pass the remote event filter, the events will be sent to local node for local listener notification.
 * <p>
 * Note that by default, all events in GridGain are disabled for performance reasons. You must only enable
 * events that you need within your logic. You can enable/disable events either by calling {@link #enableLocal(int...)}
 * or {@link #disableLocal(int...)} methods, or from XML configuration.
 * For example, you can enable all cache events as follows:
 * <pre name="code" class="xml">
 * &lt;property name="includeEventTypes"&gt;
 *     &lt;util:constant static-field="org.gridgain.grid.events.GridEventType.EVTS_CACHE"/&gt;
 * &lt;/property&gt;
 * </pre>
 */
public interface GridEvents extends IgniteAsyncSupport {
    /**
     * Gets grid projection to which this {@code GridMessaging} instance belongs.
     *
     * @return Grid projection to which this {@code GridMessaging} instance belongs.
     */
    public ClusterGroup projection();

    /**
     * Queries nodes in this projection for events using passed in predicate filter for event
     * selection.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param p Predicate filter used to query events on remote nodes.
     * @param timeout Maximum time to wait for result, {@code 0} to wait forever.
     * @param types Event types to be queried.
     * @return Collection of grid events returned from specified nodes.
     * @throws GridException If query failed.
     */
    public <T extends GridEvent> List<T> remoteQuery(IgnitePredicate<T> p, long timeout, @Nullable int... types)
        throws GridException;

    /**
     * Adds event listener for specified events to all nodes in the projection (possibly including
     * local node if it belongs to the projection as well). This means that all events occurring on
     * any node within this grid projection that pass remove filter will be sent to local node for
     * local listener notification.
     * <p>
     * The listener can be unsubscribed automatically if local node stops, if {@code locLsnr} callback
     * returns {@code false} or if {@link #stopRemoteListen(UUID)} is called.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param locLsnr Listener callback that is called on local node. If {@code null}, this events will be handled
     *      on remote nodes by passed in {@code rmtFilter}.
     * @param rmtFilter Filter callback that is called on remote node. Only events that pass the remote filter
     *      will be sent to local node. If {@code null}, all events of specified types will
     *      be sent to local node. This remote filter can be used to pre-handle events remotely,
     *      before they are passed in to local callback. It will be auto-unsubsribed on the node
     *      where event occurred in case if it returns {@code false}.
     * @param types Types of events to listen for. If not provided, all events that pass the
     *      provided remote filter will be sent to local node.
     * @param <T> Type of the event.
     * @return {@code Operation ID} that can be passed to {@link #stopRemoteListen(UUID)} method to stop listening.
     * @throws GridException If failed to add listener.
     */
    public <T extends GridEvent> UUID remoteListen(@Nullable IgniteBiPredicate<UUID, T> locLsnr,
        @Nullable IgnitePredicate<T> rmtFilter, @Nullable int... types) throws GridException;

    /**
     * Adds event listener for specified events to all nodes in the projection (possibly including
     * local node if it belongs to the projection as well). This means that all events occurring on
     * any node within this grid projection that pass remove filter will be sent to local node for
     * local listener notification.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param bufSize Remote events buffer size. Events from remote nodes won't be sent until buffer
     *      is full or time interval is exceeded.
     * @param interval Maximum time interval after which events from remote node will be sent. Events
     *      from remote nodes won't be sent until buffer is full or time interval is exceeded.
     * @param autoUnsubscribe Flag indicating that event listeners on remote nodes should be
     *      automatically unregistered if master node (node that initiated event listening) leaves
     *      topology. If this flag is {@code false}, listeners will be unregistered only when
     *      {@link #stopRemoteListen(UUID)} method is called, or the {@code 'callback (locLsnr)'}
     *      passed in returns {@code false}.
     * @param locLsnr Callback that is called on local node. If this predicate returns {@code true},
     *      the implementation will continue listening to events. Otherwise, events
     *      listening will be stopped and listeners will be unregistered on all nodes
     *      in the projection. If {@code null}, this events will be handled on remote nodes by
     *      passed in {@code rmtFilter} until local node stops (if {@code 'autoUnsubscribe'} is {@code true})
     *      or until {@link #stopRemoteListen(UUID)} is called.
     * @param rmtFilter Filter callback that is called on remote node. Only events that pass the remote filter
     *      will be sent to local node. If {@code null}, all events of specified types will
     *      be sent to local node. This remote filter can be used to pre-handle events remotely,
     *      before they are passed in to local callback. It will be auto-unsubsribed on the node
     *      where event occurred in case if it returns {@code false}.
     * @param types Types of events to listen for. If not provided, all events that pass the
     *      provided remote filter will be sent to local node.
     * @param <T> Type of the event.
     * @return {@code Operation ID} that can be passed to {@link #stopRemoteListen(UUID)} method to stop listening.
     * @see #stopRemoteListen(UUID)
     * @throws GridException If failed to add listener.
     */
    public <T extends GridEvent> UUID remoteListen(int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable IgniteBiPredicate<UUID, T> locLsnr, @Nullable IgnitePredicate<T> rmtFilter,
        @Nullable int... types) throws GridException;

    /**
     * Stops listening to remote events. This will unregister all listeners identified with provided
     * operation ID on all nodes defined by {@link #projection()}.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param opId Operation ID that was returned from
     *      {@link #remoteListen(org.apache.ignite.lang.IgniteBiPredicate, org.apache.ignite.lang.IgnitePredicate, int...)} method.
     * @see #remoteListen(org.apache.ignite.lang.IgniteBiPredicate, org.apache.ignite.lang.IgnitePredicate, int...)
     * @throws GridException If failed to stop listeners.
     */
    public void stopRemoteListen(UUID opId) throws GridException;

    /**
     * Waits for the specified events.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param filter Optional filtering predicate. Only if predicates evaluates to {@code true} will the event
     *      end the wait.
     * @param types Types of the events to wait for. If not provided, all events will be passed to the filter.
     * @return Grid event.
     * @throws GridException If wait was interrupted.
     */
    public <T extends GridEvent> T waitForLocal(@Nullable IgnitePredicate<T> filter, @Nullable int... types)
        throws GridException;

    /**
     * Queries local node for events using passed-in predicate filter for event selection.
     *
     * @param p Predicate to filter events. All predicates must be satisfied for the
     *      event to be returned.
     * @param types Event types to be queried.
     * @return Collection of grid events found on local node.
     */
    public <T extends GridEvent> Collection<T> localQuery(IgnitePredicate<T> p, @Nullable int... types);

    /**
     * Records customer user generated event. All registered local listeners will be notified.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal GridGain events and should not be used by user-defined events.
     * Attempt to record internal event with this method will cause {@code IllegalArgumentException}
     * to be thrown.
     *
     * @param evt Locally generated event.
     * @throws IllegalArgumentException If event type is within GridGain reserved range between {@code 1} and
     *      {@code 1000}.
     */
    public void recordLocal(GridEvent evt);

    /**
     * Adds an event listener for local events. Note that listener will be added regardless of whether
     * local node is in this projection or not.
     *
     * @param lsnr Predicate that is called on each received event. If predicate returns {@code false},
     *      it will be unregistered and will stop receiving events.
     * @param types Event types for which this listener will be notified.
     * @throws IllegalArgumentException Thrown in case when passed in array of event types is empty.
     */
    public void localListen(IgnitePredicate<? extends GridEvent> lsnr, int... types);

    /**
     * Removes local event listener.
     *
     * @param lsnr Local event listener to remove.
     * @param types Types of events for which to remove listener. If not specified,
     *      then listener will be removed for all types it was registered for.
     * @return {@code true} if listener was removed, {@code false} otherwise.
     */
    public boolean stopLocalListen(IgnitePredicate<? extends GridEvent> lsnr, @Nullable int... types);

    /**
     * Enables provided events. Allows to start recording events that
     * were disabled before. Note that specified events will be enabled
     * regardless of whether local node is in this projection or not.
     *
     * @param types Events to enable.
     */
    public void enableLocal(int... types);

    /**
     * Disables provided events. Allows to stop recording events that
     * were enabled before. Note that specified events will be disabled
     * regardless of whether local node is in this projection or not.
     *
     * @param types Events to disable.
     */
    public void disableLocal(int... types);

    /**
     * Gets types of enabled events.
     *
     * @return Types of enabled events.
     */
    public int[] enabledEvents();

    /**
     * Check if event is enabled.
     *
     * @param type Event type.
     * @return {@code True} if event of passed in type is enabled.
     */
    public boolean isEnabled(int type);

    /** {@inheritDoc} */
    @Override GridEvents enableAsync();
}
