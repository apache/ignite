/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.task;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;

import java.io.*;
import java.nio.*;

/**
 * Task result request.
 */
public class GridTaskResultRequest extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Task ID. */
    private IgniteUuid taskId;

    /** Topic. */
    @GridDirectTransient
    private Object topic;

    /** Serialized topic. */
    private byte[] topicBytes;

    /**
     * Public no-arg constructor for {@link Externalizable} support.
     */
    public GridTaskResultRequest() {
        // No-op.
    }

    /**
     * @param taskId Task ID.
     * @param topic Topic.
     * @param topicBytes Serialized topic.
     */
    GridTaskResultRequest(IgniteUuid taskId, Object topic, byte[] topicBytes) {
        this.taskId = taskId;
        this.topic = topic;
        this.topicBytes = topicBytes;
    }

    /**
     * @return Task ID.
     */
    public IgniteUuid taskId() {
        return taskId;
    }

    /**
     * @param taskId Task ID.
     */
    public void taskId(IgniteUuid taskId) {
        assert taskId != null;

        this.taskId = taskId;
    }

    /**
     * @return Topic.
     */
    public Object topic() {
        return topic;
    }

    /**
     * @return Serialized topic.
     */
    public byte[] topicBytes() {
        return topicBytes;
    }

    /**
     * @param topic Topic.
     */
    public void topic(String topic) {
        assert topic != null;

        this.topic = topic;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridTaskResultRequest _clone = new GridTaskResultRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridTaskResultRequest _clone = (GridTaskResultRequest)_msg;

        _clone.taskId = taskId;
        _clone.topic = topic;
        _clone.topicBytes = topicBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putGridUuid("taskId", taskId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putByteArray("topicBytes", topicBytes))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                taskId = commState.getGridUuid("taskId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                topicBytes = commState.getByteArray("topicBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 73;
    }
}
