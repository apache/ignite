/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.messaging;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Convenience actor-base adapter for {@link GridMessaging#localListen(Object, org.gridgain.grid.lang.IgniteBiPredicate)}
 * method.
 */
public abstract class GridMessagingListenActor<T> implements IgniteBiPredicate<UUID, T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean keepGoing = true;

    /** */
    private UUID nodeId;

    /** */
    @GridInstanceResource
    private transient Ignite ignite;

    /** */
    @GridLoggerResource
    private transient GridLogger log;

    /**
     * Gets grid instance associated with this actor.
     *
     * @return Grid instance associated with this actor.
     */
    protected final Ignite grid() {
        assert ignite != null;

        return ignite;
    }

    /** {@inheritDoc} */
    @Override public final boolean apply(UUID nodeId, T rcvMsg) {
        assert nodeId != null;
        assert rcvMsg != null;

        if (!keepGoing)
            return false;

        this.nodeId = nodeId;

        try {
            receive(nodeId, rcvMsg);
        }
        catch (Throwable e) {
            onError(e);
        }

        return keepGoing;
    }

    /**
     * This method is called in case when method {@link #receive(UUID, Object)} threw an exception.
     * Insides of this method the implementation should call any of the {@code respond}, {@code stop}
     * or {@code skip} methods. If overriding method does nothing - than return value of method
     * {@link #receive(UUID, Object)} is undefined.
     * <p>
     * Default implementation simply calls method {@link #stop()}.
     *
     * @param e Exception thrown from method {@link #receive(UUID, Object)}.
     */
    protected void onError(Throwable e) {
        U.error(log, "Listener operation failed.", e);

        stop();
    }

    /**
     * This method receives the message. This is the only method that subclass needs to override.
     * Insides of this method the implementation should call any of the {@code respond}, {@code stop}
     * or {@code skip} methods. Note that if none of these methods are called - listener will continue
     * listen for the new messages.
     *
     * @param nodeId ID of the sender node.
     * @param rcvMsg Received message.
     * @throws Throwable Thrown in case of any errors. Method {@link #onError(Throwable)}} will
     *      be called right before returning from this method.
     */
    protected abstract void receive(UUID nodeId, T rcvMsg) throws Throwable;

    /**
     * This method instructs underlying implementation to stop receiving new messages and unregister
     * the message listener.
     * <p>
     * Note that subclasses can call any of {@code respond}, {@code stop} or {@code skip} methods any
     * number of times. Only the last executed method will determine whether or not the implementation will
     * continue listen for the new messages.
     */
    protected final void stop() {
        keepGoing = false;
    }

    /**
     * This method sends the response message to the original sender node and instructs underlying
     * implementation to stop receiving new messages and unregister the message listener.
     * <p>
     * Note that subclasses can call any of {@code respond}, {@code stop} or {@code skip} methods any
     * number of times. Only the last executed method will determine whether or not the implementation will
     * continue listen for the new messages.
     *
     * @param respMsg Optional response message. If not {@code null} - it will be sent to the original
     *      sender node.
     * @throws GridException Thrown in case of any errors.
     */
    protected final void stop(@Nullable Object respMsg) throws GridException {
        keepGoing = false;

        send(nodeId, respMsg);
    }

    /**
     * Skips current message and continues to listen for new message. This method simply calls
     * {@code respond(null)}.
     * <p>
     * Note that subclasses can call any of {@code respond}, {@code stop} or {@code skip} methods any
     * number of times. Only the last executed method will determine whether or not the implementation will
     * continue listen for the new messages.
     */
    protected final void skip() {
        checkReversing();

        keepGoing = true;
    }

    /**
     * Responds to the original sender node with given message and continues to listen for the new messages.
     * <p>
     * Note that subclasses can call any of {@code respond}, {@code stop} or {@code skip} methods any
     * number of times. Only the last executed method will determine whether or not the implementation will
     * continue listen for the new messages.
     *
     * @param respMsg Optional response message. If not {@code null} - it will be sent to the original
     *      sender node.
     * @throws GridException Thrown in case of any errors.
     */
    protected final void respond(@Nullable Object respMsg) throws GridException {
        checkReversing();

        keepGoing = true;

        send(nodeId, respMsg);
    }

    /**
     * Responds to the provided node with given message and continues to listen for the new messages.
     * <p>
     * Note that subclasses can call any of {@code respond}, {@code stop} or {@code skip} methods any
     * number of times. Only the last executed method will determine whether or not the implementation will
     * continue listen for the new messages.
     *
     * @param id ID of the node to send the message to, if any.
     * @param respMsg Optional response message. If not {@code null} - it will be sent to the original
     *      sender node.
     * @throws GridException Thrown in case of any errors.
     */
    protected final void respond(UUID id, @Nullable Object respMsg) throws GridException {
        checkReversing();

        keepGoing = true;

        send(id, respMsg);
    }

    /**
     * Checks reversing.
     */
    private void checkReversing() {
        if (!keepGoing)
            U.warn(log, "Suspect logic - reversing listener return status (was 'true', then 'false', " +
                "now 'true' again).");
    }

    /**
     * Sends optional message. If message is {@code null} - it's no-op.
     *
     * @param nodeId ID of the node to send message to.
     * @param respMsg Message to send.
     * @throws GridException Thrown in case of any errors.
     */
    private void send(UUID nodeId, @Nullable Object respMsg) throws GridException {
        assert nodeId != null;

        if (respMsg != null) {
            ClusterNode node = ignite.cluster().node(nodeId);

            if (node != null)
                ignite.message(ignite.cluster().forNode(node)).send(null, respMsg); // Can still fail.
            else
                throw new GridException("Failed to send message since destination node has " +
                    "left topology (ignoring) [nodeId=" +nodeId + ", respMsg=" + respMsg + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMessagingListenActor.class, this);
    }
}
