/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Portable reader implementation.
 */
public class GridTcpCommunicationMessageReader implements MessageReader {
    /** Stream. */
    private final GridTcpCommunicationByteBufferStream stream;

    /** Whether last field was fully read. */
    private boolean lastRead;

    /**
     * @param msgFactory Message factory.
     */
    public GridTcpCommunicationMessageReader(GridTcpMessageFactory msgFactory) {
        this.stream = new GridTcpCommunicationByteBufferStream(msgFactory);
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String name) {
        if (stream.remaining() < 1) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readByte();
        }
    }

    /** {@inheritDoc} */
    @Override public short readShort(String name) {
        if (stream.remaining() < 2) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readShort();
        }
    }

    /** {@inheritDoc} */
    @Override public int readInt(String name) {
        if (stream.remaining() < 4) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readInt();
        }
    }

    /** {@inheritDoc} */
    @Override public long readLong(String name) {
        if (stream.remaining() < 8) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readLong();
        }
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String name) {
        if (stream.remaining() < 4) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readFloat();
        }
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String name) {
        if (stream.remaining() < 8) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readDouble();
        }
    }

    /** {@inheritDoc} */
    @Override public char readChar(String name) {
        if (stream.remaining() < 2) {
            lastRead = false;

            return 0;
        }
        else {
            lastRead = true;

            return stream.readChar();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String name) {
        if (stream.remaining() < 1) {
            lastRead = false;

            return false;
        }
        else {
            lastRead = true;

            return stream.readBoolean();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String name) {
        byte[] arr = stream.readByteArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String name) {
        short[] arr = stream.readShortArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String name) {
        int[] arr = stream.readIntArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String name) {
        long[] arr = stream.readLongArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String name) {
        float[] arr = stream.readFloatArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String name) {
        double[] arr = stream.readDoubleArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String name) {
        char[] arr = stream.readCharArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String name) {
        boolean[] arr = stream.readBooleanArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridTcpCommunicationMessageAdapter readMessage(String name) {
        GridTcpCommunicationMessageAdapter msg = stream.readMessage();

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean isLastRead() {
        return lastRead;
    }
}
