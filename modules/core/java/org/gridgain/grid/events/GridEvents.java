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
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Defines grid projection interface and monadic set of operations on a set of grid nodes.
 * <p>
 * All main grid entities such as grid and a node instances can be viewed as
 * collection of grid nodes (in case of the grid node this collection consist of only one
 * element). As such they all share the same set of operations that can be performed on a set
 * grid nodes. These operations are defined in {@link GridEvents} interface and called
 * <tt>monadic</tt> as they are equally defined on any arbitrary set of nodes.
 *
 * <h1 class="header">Nullable and Monads</h1>
 * Many methods in this interface accepts nullable parameters. Although it may seem counter intuitive
 * for some of them - it is done to promote monadic usage of this interface. Java doesn't natively support
 * concepts like <tt>Option</tt> in Scala and returning, accepting, and properly handling
 * {@code null} values is Java's way to support such monadic invocations.
 * <p>
 * All methods that accept {@code null} values (for monadic purposes) will gracefully handle it by
 * either returning a finished future, or empty collection, {@code null} value, or combination of the
 * above. Most method calls therefore can be chained without an explicit checks for {@code null}s.
 * <p>
 * The downside of this approach that inadvertent errors of passing {@code null} will not result
 * in {@link NullPointerException} and may be harder to catch.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridEvents {
    /**
     * @return TODO
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
    public GridFuture<List<GridEvent>> queryRemote(GridPredicate<GridEvent> pe, long timeout);

    /**
     * Starts consuming remote events. This will register event listeners on <b>all nodes defined by
     * this projection</b> and caught events will be passed through an optional filter and sent
     * to the local node.
     *
     * @param cb Callback that is called on local node. If this predicate returns {@code true},
     *      the implementation will continue consuming events. Otherwise, events
     *      consuming will be stopped and listeners will be unregistered on all nodes
     *      in the projection. If {@code null}, this events will be handled on remote nodes by
     *      passed in {@code filter} until this node stops or until {@link #stopConsume(UUID)}
     *      is called.
     * @param filter Filter callback that is called on remote node. Only events that pass the filter will
     *      be sent to local node. If {@code null}, all events of specified types will
     *      be sent to local node. This filter can be used to pre-handle events remotely,
     *      before they are passed in to local callback.
     * @param types Types of events to listen for. If not provided, all events that pass the
     *      provided filter will be sent to local node.
     * @param <T> Type of the event.
     * @return Future that finishes when all listeners are registered. It returns {@code consumeId}
     *      that can be passed to {@link #stopConsume(UUID)} method to stop consuming.
     * @see #stopConsume(UUID)
     */
    public <T extends GridEvent> GridFuture<UUID> consumeRemote(@Nullable GridBiPredicate<UUID, T> cb,
        @Nullable GridPredicate<T> filter, @Nullable int... types);

    /**
     * Starts consuming remote events. This will register event listeners on <b>all nodes defined by
     * this projection</b> and caught events will be passed through an optional filter and sent
     * to the local node.
     *
     * @param bufSize Remote events buffer size. Events from remote nodes won't be sent until buffer
     *      is full or time interval is exceeded.
     * @param interval Maximum time interval after which events from remote node will be sent. Events
     *      from remote nodes won't be sent until buffer is full or time interval is exceeded.
     * @param autoUnsubscribe Flag indicating that event listeners on remote nodes should be
     *      automatically unregistered if master node (node that initiated event consuming) leaves
     *      topology. If this flag is {@code false}, listeners will be unregistered only when
     *      {@link #stopConsume(UUID)} method is called, or the {@code 'callback (cb)'}
     *      passed in returns {@code false}.
     * @param cb Callback that is called on local node. If this predicate returns {@code true},
     *      the implementation will continue consuming events. Otherwise, events
     *      consuming will be stopped and listeners will be unregistered on all nodes
     *      in the projection. If {@code null}, this events will be handled on remote nodes by
     *      passed in {@code filter} until this node stops (if {@code 'autoUnsubscribe'} is {@code true})
     *      or until {@link #stopConsume(UUID)} is called.
     * @param filter Filter callback that is called on remote node. Only events that pass the filter will
     *      be sent to local node. If {@code null}, all events of specified types will
     *      be sent to local node. This filter can be used to pre-handle events remotely,
     *      before they are passed in to local callback.
     * @param types Types of events to listen for. If not provided, all events that pass the
     *      provided filter will be sent to local node.
     * @param <T> Type of the event.
     * @return Future that finishes when all listeners are registered. It returns {@code consumeId}
     *      that can be passed to {@link #stopConsume(UUID)} method to stop consuming.
     * @see #stopConsume(UUID)
     */
    public <T extends GridEvent> GridFuture<UUID> consumeRemote(int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable GridBiPredicate<UUID, T> cb, @Nullable GridPredicate<T> filter,
        @Nullable int... types);

    /**
     * Stops consuming remote events. This will unregister all listeners identified with provided
     * consume ID on <b>all nodes defined by this projection</b>.
     *
     * @param consumeId Consume ID that was returned from
     *      {@link #consumeRemote(GridBiPredicate, GridPredicate, int...)} method.
     *      If {@code null}, this method is no-op.
     * @return Future that finishes when all listeners are unregistered.
     * @see #consumeRemote(GridBiPredicate, GridPredicate, int...)
     */
    public GridFuture<?> stopConsume(@Nullable UUID consumeId);

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
    public GridFuture<GridEvent> waitForLocal(@Nullable GridPredicate<GridEvent> p, @Nullable int... types);

    /**
     * Queries local node for events using passed-in predicate filters for event selection.
     *
     * @param p Mandatory predicates to filter events. All predicates must be satisfied for the
     *      event to be returned.
     *      <p>
     *      <b>Note:</b> unlike other methods in GridGain APIs if no predicates is provided this
     *      method will return no results. This exception is made to avoid situation when all local
     *      events are erroneously returned. Returning all local events may result in creating
     *      collection with tens of thousands elements seriously compromising the system's performance.
     * @return Collection of grid events found on local node.
     * @see PE
     */
    public Collection<GridEvent> queryLocal(@Nullable GridPredicate<GridEvent>... p);

    /**
     * Records locally generated event. Registered local listeners will be notified, if any. This
     * event can be obtained from the remote nodes by performing a distributed query using
     * {@link #queryRemote(GridPredicate, long)} method.
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
     * @param lsnr Event listener for local events to add.
     * @param types Event types for which this listener will be notified. If this array is empty an exception
     *      will be thrown.
     *      <p>
     *      <b>NOTE:</b> subscribing to large set of events will impose significant performance penalty.
     * @throws GridRuntimeException Thrown in case when passed in array of event types is empty.
     * @see GridEvent
     * @see GridEventType
     * @see #addLocalListener(GridLocalEventListener, int, int...)
     */
    public void addLocalListener(GridLocalEventListener lsnr, int[] types);

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
     * Note that unlike its sibling method this method never throws an exception because its signature
     * guarantees that there is at least one event type to subscribe for.
     * <p>
     * Note also that since event types are defined as integer the unknown (invalid) event types cannot be detected
     * and therefore will be ignored (because there is no way to know which user-defined types are used).
     *
     * @param lsnr Event listener for local events to add.
     * @param type Event type for which this listener will be notified.
     * @param types Optional event types for which this listener will be notified.
     * @see GridEvent
     * @see GridEventType
     * @see #addLocalListener(GridLocalEventListener, int[])
     */
    public void addLocalListener(GridLocalEventListener lsnr, int type, @Nullable int... types);

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
    public boolean removeLocalListener(GridLocalEventListener lsnr, @Nullable int... types);

    /**
     * Enables provided events. Allows to start recording events that
     * were disabled before. Can be used to enable event recording
     * temporarily for monitoring purposes. Recording can be disabled
     * by {@link #disableLocal(int[])} or {@link #disableLocal(int, int...)}
     * method.
     *
     * @param types Events to enable.
     * @see #enableLocal(int, int...)
     * @see #disableLocal(int[])
     * @see #disableLocal(int, int...)
     */
    public void enableLocal(int[] types);

    /**
     * Enables provided events. Allows to start recording events that
     * were disabled before. Can be used to enable event recording
     * temporarily for monitoring purposes. Recording can be disabled
     * by {@link #disableLocal(int[])} or {@link #disableLocal(int, int...)}
     * method.
     *
     * @param type Event to enable.
     * @param types Events to enable.
     * @see #enableLocal(int[])
     * @see #disableLocal(int[])
     * @see #disableLocal(int, int...)
     */
    public void enableLocal(int type, @Nullable int... types);

    /**
     * Disables provided events. Allows to stop recording events that
     * were enabled before. Any disabled events can be enabled again
     * by {@link #enableLocal(int[])} or {@link #enableLocal(int, int...)}
     * method.
     *
     * @param types Events to disable.
     * @see #disableLocal(int, int...)
     * @see #enableLocal(int[])
     * @see #enableLocal(int, int...)
     */
    public void disableLocal(int[] types);

    /**
     * Disables provided events. Allows to stop recording events that
     * were enabled before. Any disabled events can be enabled again
     * by {@link #enableLocal(int[])} or {@link #enableLocal(int, int...)}
     * method.
     *
     * @param type Event to disable.
     * @param types Events to disable.
     * @see #disableLocal(int[])
     * @see #enableLocal(int[])
     * @see #enableLocal(int, int...)
     */
    public void disableLocal(int type, @Nullable int... types);
}
