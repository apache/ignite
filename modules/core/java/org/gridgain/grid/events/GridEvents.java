// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Provides functionality for local and remote event notifications on nodes within the grid projection.
 * There are {@code 2} ways to subscribe to event listening, {@code local} and {@code remote}.
 * <p>
 * Local subscription, defined by {@link #localListen(GridPredicate, int...)} method, will add
 * a listener for specified events on local node only. This listener will be notified whenever any
 * of subscribed events happens on this node regardless of whether this node belongs to underlying
 * grid projection or not.
 * <p>
 * Remote subscription, defined by {@link #remoteListen(GridBiPredicate, GridPredicate, int...)}, will add an
 * event listener for specified events on all nodes in the projection (possibly including this node if
 * it belongs to the projection as well). All projection nodes will then be notified of the subscribed events, and
 * if they pass event filter, the events will be sent to this node for local listener notification.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridEvents {
    /**
     * Gets grid projection to which this {@code GridMessaging} instance belongs.
     *
     * @return Grid projection to which this {@code GridMessaging} instance belongs.
     */
    public GridProjection projection();

    /**
     * Asynchronously queries nodes in this projection for events using passed in predicate filter for event
     * selection. This operation is distributed and hence can fail on communication layer and generally can
     * take much longer than local event notifications. Note that this method will not block and will return
     * immediately with future.
     *
     * @param pe Predicate filter used to query events on remote nodes.
     * @param timeout Maximum time to wait for result, {@code 0} to wait forever.
     * @return Collection of grid events returned from specified nodes.
     */
    public <T extends GridEvent> GridFuture<List<T>> remoteQuery(GridPredicate<T> pe, long timeout);

    /**
     * Starts listening to remote events. This will register event listeners on <b>all nodes defined by
     * this projection</b> and caught events will be passed through an optional remote filter and sent
     * to the local node.
     *
     * @param locLsnr Callback that is called on local node. If this predicate returns {@code true},
     *      the implementation will continue listening to events. Otherwise, events
     *      listening will be stopped and listeners will be unregistered on all nodes
     *      in the projection. If {@code null}, this events will be handled on remote nodes by
     *      passed in {@code rmtFilter} until this node stops or until {@link #stopRemoteListen(UUID)}
     *      is called.
     * @param rmtFilter Filter callback that is called on remote node. Only events that pass the remote filter
     *      will be sent to local node. If {@code null}, all events of specified types will
     *      be sent to local node. This remote filter can be used to pre-handle events remotely,
     *      before they are passed in to local callback.
     * @param types Types of events to listen for. If not provided, all events that pass the
     *      provided remote filter will be sent to local node.
     * @param <T> Type of the event.
     * @return Future that finishes when all listeners are registered. It returns {@code operation ID}
     *      that can be passed to {@link #stopRemoteListen(UUID)} method to stop listening.
     * @see #stopRemoteListen(UUID)
     */
    public <T extends GridEvent> GridFuture<UUID> remoteListen(@Nullable GridBiPredicate<UUID, T> locLsnr,
        @Nullable GridPredicate<T> rmtFilter, @Nullable int... types);

    /**
     * Starts listening to remote events. This will register event listeners on <b>all nodes defined by
     * this projection</b> and caught events will be passed through an optional remote filter and sent
     * to the local node.
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
     *      passed in {@code rmtFilter} until this node stops (if {@code 'autoUnsubscribe'} is {@code true})
     *      or until {@link #stopRemoteListen(UUID)} is called.
     * @param rmtFilter Filter callback that is called on remote node. Only events that pass the remote filter
     *      will be sent to local node. If {@code null}, all events of specified types will
     *      be sent to local node. This remote filter can be used to pre-handle events remotely,
     *      before they are passed in to local callback.
     * @param types Types of events to listen for. If not provided, all events that pass the
     *      provided remote filter will be sent to local node.
     * @param <T> Type of the event.
     * @return Future that finishes when all listeners are registered. It returns {@code operation ID}
     *      that can be passed to {@link #stopRemoteListen(UUID)} method to stop listening.
     * @see #stopRemoteListen(UUID)
     */
    public <T extends GridEvent> GridFuture<UUID> remoteListen(int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable GridBiPredicate<UUID, T> locLsnr, @Nullable GridPredicate<T> rmtFilter,
        @Nullable int... types);

    /**
     * Stops listening to remote events. This will unregister all listeners identified with provided
     * operation ID on all nodes defined by {@link #projection()}.
     *
     * @param opId Operation ID that was returned from
     *      {@link #remoteListen(GridBiPredicate, GridPredicate, int...)} method.
     * @return Future that finishes when all listeners are unregistered.
     * @see #remoteListen(GridBiPredicate, GridPredicate, int...)
     */
    public GridFuture<?> stopRemoteListen(UUID opId);

    /**
     * Gets event future that allows for asynchronous waiting for the specified events.
     * <p>
     * This is one of the two similar methods providing different semantic for waiting for events.
     * One method uses passed in optional continuation so that caller can pass a logic that
     * emits the event, and another method (this one) returns future allowing caller a more discrete
     * control.
     * <p>
     * This method returns a future which by calling one of its {@code get} methods will block
     * and wait for the specified event (either indefinitely or with provided timeout). Note that
     * you need to call this method to acquire the future before emitting the event itself. This
     * way you can avoid the window when event is emitted but no listener is set for it.
     * <p>
     * This method encapsulates an important paradigm as many operations in GridGain cause local events
     * to be generated even for the operations that may happen on the remote nodes. This method provides
     * convenient one-stop blocking and waiting functionality for such cases.
     *
     * @param p Optional filtering predicate. Only if predicates evaluates to {@code true} will the event
     *      end the wait. Note that events of provided types only will be fed to the predicate.
     * @param types Types of the events to wait for.
     * @return Grid event future.
     */
    public <T extends GridEvent> GridFuture<T> waitForLocal(@Nullable GridPredicate<T> p, @Nullable int... types);

    /**
     * Queries local node for events using passed-in predicate filters for event selection.
     *
     * @param p Mandatory predicates to filter events. All predicates must be satisfied for the
     *      event to be returned.
     * @return Collection of grid events found on local node.
     */
    public <T extends GridEvent> Collection<T> localQuery(GridPredicate<T> p);

    /**
     * Records locally generated event. Registered local listeners will be notified, if any. This
     * event can be obtained from the remote nodes by performing a distributed query using
     * {@link #remoteQuery(GridPredicate, long)} method.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal GridGain events and should not be used by user-defined events.
     * Attempt to record internal event with this method will cause {@code IllegalArgumentException}
     * to be thrown.
     *
     * @param evt Locally generated event.
     * @throws IllegalArgumentException If event type is within GridGain reserved range between {@code 1} and
     *      {@code 1000}.
     * @see GridEventType
     */
    public void recordLocal(GridEvent evt);

    /**
     * Adds an event listener for local events.
     * <p>
     * Note that by default all events in GridGain are enabled and therefore generated and stored
     * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
     * under the load and therefore it creates a significant additional load on the system. If these events are
     * not needed by the application this load is unnecessary and leads to significant performance degradation.
     * <p>
     * It is <b>highly recommended</b> to enable only those events that your application logic requires
     * by using {@link GridConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
     * events are required for GridGain's internal operations and such events will still be generated but not stored by
     * event storage SPI if they are disabled in GridGain configuration.
     * <p>
     * Note also that since event types are defined as integer the unknown (invalid) event types cannot be detected
     * and therefore will be ignored (because there is no way to know which user-defined types are used).
     *
     * @param lsnr Predicate that is called on each received event. If predicate returns {@code true},
     *      the implementation will continue listening for the new events. Otherwise, the implementation
     *      will unregister the listener and stop receiving events.
     * @param types Event types for which this listener will be notified. If this array is empty an exception
     *      will be thrown.
     *      <p>
     *      <b>NOTE:</b> subscribing to large set of events will impose significant performance penalty.
     * @throws GridRuntimeException Thrown in case when passed in array of event types is empty.
     * @see GridEvent
     * @see GridEventType
     */
    public void localListen(GridPredicate<? extends GridEvent> lsnr, int... types);

    /**
     * Removes local event listener.
     *
     * @param lsnr Local event listener to remove.
     * @param types Types of events for which to remove listener. If not specified,
     *      then listener will be removed for all types it was registered for.
     * @return {@code true} if listener was removed, {@code false} otherwise.
     * @see GridEventType
     * @see GridEvent
     */
    public boolean stopLocalListen(GridPredicate<? extends GridEvent> lsnr, @Nullable int... types);

    /**
     * Enables provided events. Allows to start recording events that
     * were disabled before. Can be used to enable event recording
     * temporarily for monitoring purposes. Recording can be disabled
     * by {@link #disableLocal(int[])} method.
     *
     * @param types Events to enable.
     * @see #disableLocal(int[])
     */
    public void enableLocal(int... types);

    /**
     * Disables provided events. Allows to stop recording events that
     * were enabled before. Any disabled events can be enabled again
     * by {@link #enableLocal(int[])} method.
     *
     * @param types Events to disable.
     * @see #enableLocal(int[])
     */
    public void disableLocal(int... types);
}
