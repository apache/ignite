/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Portable writer implementation.
 */
public class GridTcpCommunicationMessageWriter implements MessageWriter {
    /** Stream. */
    private final GridTcpCommunicationByteBufferStream stream = new GridTcpCommunicationByteBufferStream(null);

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public boolean writeByte(String name, byte val) {
        if (stream.remaining() < 1)
            return false;

        stream.writeByte(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeShort(String name, short val) {
        if (stream.remaining() < 2)
            return false;

        stream.writeShort(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeInt(String name, int val) {
        if (stream.remaining() < 4)
            return false;

        stream.writeInt(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeLong(String name, long val) {
        if (stream.remaining() < 8)
            return false;

        stream.writeLong(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloat(String name, float val) {
        if (stream.remaining() < 4)
            return false;

        stream.writeFloat(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeDouble(String name, double val) {
        if (stream.remaining() < 8)
            return false;

        stream.writeDouble(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeChar(String name, char val) {
        if (stream.remaining() < 2)
            return false;

        stream.writeChar(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeBoolean(String name, boolean val) {
        if (stream.remaining() < 1)
            return false;

        stream.writeBoolean(val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(String name, @Nullable byte[] val) {
        if (val != null) {
            stream.writeByteArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeShortArray(String name, @Nullable short[] val) {
        if (val != null) {
            stream.writeShortArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeIntArray(String name, @Nullable int[] val) {
        if (val != null) {
            stream.writeIntArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeLongArray(String name, @Nullable long[] val) {
        if (val != null) {
            stream.writeLongArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloatArray(String name, @Nullable float[] val) {
        if (val != null) {
            stream.writeFloatArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeDoubleArray(String name, @Nullable double[] val) {
        if (val != null) {
            stream.writeDoubleArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeCharArray(String name, @Nullable char[] val) {
        if (val != null) {
            stream.writeCharArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeBooleanArray(String name, @Nullable boolean[] val) {
        if (val != null) {
            stream.writeBooleanArray(val);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 4)
                return false;

            stream.writeInt(-1);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeMessage(String name, @Nullable GridTcpCommunicationMessageAdapter msg) {
        if (msg != null) {
            stream.writeMessage(msg);

            return stream.lastFinished();
        }
        else {
            if (stream.remaining() < 1)
                return false;

            stream.writeByte(Byte.MIN_VALUE);

            return true;
        }
    }
}
