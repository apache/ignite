// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.messaging;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridMessaging {
    /**
     * Gets grid projection to which this {@code GridMessaging} instance belongs.
     *
     * @return Grid projection to which this {@code GridMessaging} instance belongs.
     */
    public GridProjection projection();

    /**
     * Sends given message with specified topic to the nodes in this projection.
     *
     * @param topic Topic of this message. If {@code null} or empty - the message has default topic.
     * @param msg Message to send.
     * @throws GridException If failed to send a message to any of the nodes.
     * @throws GridEmptyProjectionException Thrown in case when this projection is empty.
     *      Note that in case of dynamic projection this method will take a snapshot of all the
     *      nodes at the time of this call, apply all filtering predicates, if any, and if the
     *      resulting collection of nodes is empty - the exception will be thrown.
     */
    public void send(@Nullable Object topic, Object msg) throws GridException;

    /**
     * Sends given messages with specified topic to the nodes in this projection.
     *
     * @param topic Topic of this message. If {@code null} or empty - the message has default topic.
     * @param msgs Messages to send. Order of the sending is undefined. If the method produces
     *      the exception none or some messages could have been sent already.
     * @throws GridException If failed to send a message to any of the nodes.
     * @throws GridEmptyProjectionException Thrown in case when this projection is empty.
     *      Note that in case of dynamic projection this method will take a snapshot of all the
     *      nodes at the time of this call, apply all filtering predicates, if any, and if the
     *      resulting collection of nodes is empty - the exception will be thrown.
     */
    public void send(@Nullable Object topic, Collection<?> msgs) throws GridException;

    /**
     * Sends given message with specified topic to the nodes in this projection.
     * <p>
     * Messages, sent with this method, will arrive in the same order they were sent.
     *
     * @param topic Topic of this message. If {@code null} or empty - the message has default topic.
     * @param msg Message to send.
     * @param timeout Timeout in milliseconds, after which the message will be discarded.
     * @throws GridException If failed to send a message to any of the nodes.
     * @throws GridEmptyProjectionException Thrown in case when this projection is empty.
     *      Note that in case of dynamic projection this method will take a snapshot of all the
     *      nodes at the time of this call, apply all filtering predicates, if any, and if the
     *      resulting collection of nodes is empty - the exception will be thrown.
     */
    public void sendOrdered(@Nullable Object topic, Object msg, long timeout) throws GridException;

    /**
     * Convenient utility listening method for messages. This method provides a convenient idiom
     * of protocol-based message exchange with automatic listener management.
     * <p>
     * When this method is called it will register message listener for the messages <tt>from all nodes
     * in the grid</tt> and return immediately without blocking. On the background, for each received
     * message it will call passed in predicate. If predicate returns {@code true}, it will continue
     * listening for the new messages. Otherwise, it will unregister the listener and stop receiving
     * messages.
     *
     * @param topic Topic to subscribe to. Will only receive messages for that topic. Empty string or
     *      {@code null} mean default topic (the one, which is used when calling {@link #send(Object, Object)}).
     * @param p Predicate that is called on each received message. If predicate returns {@code true},
     *      the implementation will continue listening for the new messages. Otherwise, the implementation
     *      will unregister the listener and stop receiving messages.
     * @see GridMessagingListenActor
     * @see #remoteListen(Object, GridBiPredicate)
     */
    public void localListen(@Nullable Object topic, GridBiPredicate<UUID, ?> p);

    /**
     * Registers given message listener on <b>all nodes defined by this projection</b> to listen for
     * messages sent <b>to given topic</b>. Messages can be sent using one of the following
     * methods:
     * <ul>
     *     <li>{@link #send(Object, Object)}</li>
     *     <li>{@link #send(Object, Collection)}</li>
     * </ul>
     * Essentially, this method allows to "wire up" sender and receiver(s) of the messages in a
     * completely distributed manner.
     * <p>
     * Note that returned future will complete when listener is registered on current snapshot of nodes
     * for this projection. But if projection is dynamic, listener will be registered on all nodes that join it,
     * even if they join after the future is completed.
     *
     * @param topic Topic to subscribe to. Will only receive messages for that topic. Empty string or
     *      {@code null} mean default topic (the one, which is used when calling {@link #send(Object, Object)}).
     * @param p Predicate that is called on each received message. If predicate returns {@code true},
     *      the implementation will continue listening for the new messages. Otherwise, the implementation
     *      will unregister the listener and stop receiving messages.
     * @return Future for this distributed operation.
     * @see GridMessagingListenActor
     * @see #localListen(Object, GridBiPredicate)
     * @see #send(Object, Object)
     * @see #send(Object, Collection)
     */
    public GridFuture<?> remoteListen(@Nullable Object topic, GridBiPredicate<UUID, ?> p);
}
