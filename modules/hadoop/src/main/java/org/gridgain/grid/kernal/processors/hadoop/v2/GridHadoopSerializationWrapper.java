/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.io.serializer.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import java.io.*;

import org.jetbrains.annotations.*;

/**
 * The wrapper around external serializer.
 */
public class GridHadoopSerializationWrapper<T> implements GridHadoopSerialization {
    /** External serializer - writer */
    private final Serializer<T> serializer;

    /** External serializer - reader */
    private final Deserializer<T> deserializer;
    private OutputStream outStream;
    private InputStream inStream;

    /**
     * @param serialization External serializer to wrap.
     * @param cls The class to serialize.
     */
    public GridHadoopSerializationWrapper(Serialization<T> serialization, Class<T> cls) {
        serializer = serialization.getSerializer(cls);
        deserializer = serialization.getDeserializer(cls);
    }

    /** {@inheritDoc} */
    @Override public void write(DataOutput out, Object obj) throws GridException {
        assert out instanceof OutputStream;

        try {
            if (outStream != out) {
                if (outStream != null)
                    serializer.close();

                outStream = (OutputStream) out;

                serializer.open(outStream);
            }

            serializer.serialize((T)obj);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object read(DataInput in, @Nullable Object obj) throws GridException {
        assert in instanceof InputStream;

        try {
            if (inStream != in) {
                if (inStream != null)
                    deserializer.close();

                inStream = (InputStream) in;

                deserializer.open(inStream);
            }

            obj = deserializer.deserialize((T) obj);
        }
        catch (IOException e) {
            throw new GridException(e);
        }

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        try {
            if (outStream != null) {
                serializer.close();
                outStream = null;
            }

            if (inStream != null) {
                deserializer.close();
                inStream = null;
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
