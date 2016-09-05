/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.cassandra.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Serializer based on Kryo serialization.
 */
public class KryoSerializer implements Serializer {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int DFLT_BUFFER_SIZE = 4096;

    /** Thread local instance of {@link com.esotericsoftware.kryo.Kryo} */
    private transient ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            return new Kryo();
        }
    };

    /** {@inheritDoc} */
    @Override public ByteBuffer serialize(Object obj) {
        if (obj == null)
            return null;

        ByteArrayOutputStream stream = null;

        Output out = null;

        try {
            stream = new ByteArrayOutputStream(DFLT_BUFFER_SIZE);

            out = new Output(stream);

            kryos.get().writeClassAndObject(out, obj);
            out.flush();

            return ByteBuffer.wrap(stream.toByteArray());
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to serialize object of the class '" + obj.getClass().getName() + "'", e);
        }
        finally {
            U.closeQuiet(out);
            U.closeQuiet(stream);
        }
    }

    /** {@inheritDoc} */
    @Override public Object deserialize(ByteBuffer buf) {
        ByteArrayInputStream stream = null;
        Input in = null;

        try {
            stream = new ByteArrayInputStream(buf.array());
            in = new Input(stream);

            return kryos.get().readClassAndObject(in);
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to deserialize object from byte stream", e);
        }
        finally {
            U.closeQuiet(in);
            U.closeQuiet(stream);
        }
    }
}
