/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.store.cassandra.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Serializer based on Kryo serialization.
 */
public class KryoSerializer implements Serializer {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int DFLT_BUFFER_SIZE = 4096;

    /** Thread local instance of {@link Kryo} */
    private transient ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        /** {@inheritDoc} */
        @Override protected Kryo initialValue() {
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
            throw new IllegalStateException("Failed to serialize object of the class '" + obj.getClass().getName() + "'", e);
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
            throw new IllegalStateException("Failed to deserialize object from byte stream", e);
        }
        finally {
            U.closeQuiet(in);
            U.closeQuiet(stream);
        }
    }
}
