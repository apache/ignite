/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.io.serializer.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import java.io.*;

import org.jetbrains.annotations.*;

/**
 *
 */
public class GridHadoopSerializationAdapter<T> implements GridHadoopSerialization {
    private final Buffer outBuffer;
    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    private static class Buffer extends ByteArrayOutputStream {
        void writeTo(DataOutput out) throws IOException {
            out.write(buf, 0, count);
        }
    }

    public GridHadoopSerializationAdapter(Serialization<T> serialization, Class<T> cls) {
        serializer = serialization.getSerializer(cls);
        deserializer = serialization.getDeserializer(cls);

        outBuffer = new Buffer();
    }

    @Override public void write(DataOutput out, Object obj) throws GridException {

        try {
            //It could be uncommented if open and close of serializer is moved out from the method.
/*
            if (out instanceof OutputStream) {
                serializer.open((OutputStream) out);
                serializer.serialize((T)obj);
                //Call of close method closes out.
                //serializer.close();
            }
            else {
*/
                //Safe method
                outBuffer.reset();

                serializer.open(outBuffer);
                serializer.serialize((T)obj);
                serializer.close();

                outBuffer.writeTo(out);
//            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public Object read(DataInput in, @Nullable Object obj) throws GridException {
        try {
            deserializer.open((InputStream) in);
            obj = deserializer.deserialize((T) obj);
            deserializer.close();
        }
        catch (IOException e) {
            throw new GridException(e);
        }

        return obj;
    }
}
