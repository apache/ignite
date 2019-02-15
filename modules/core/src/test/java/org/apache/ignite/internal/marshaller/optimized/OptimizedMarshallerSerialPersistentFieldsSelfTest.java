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
package org.apache.ignite.internal.marshaller.optimized;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.io.Serializable;

import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.GridMarshallerAbstractTest;
import org.apache.ignite.marshaller.Marshaller;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test that Optimized Marshaller works with classes with serialPersistentFields.
 */
@RunWith(JUnit4.class)
public class OptimizedMarshallerSerialPersistentFieldsSelfTest  extends GridMarshallerAbstractTest {
    /** {@inheritDoc} */
    @Override protected Marshaller marshaller() {
        return new OptimizedMarshaller(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimizedMarshaller() throws Exception {
        unmarshal(marshal(new TestClass()));

        TestClass2 val = unmarshal(marshal(new TestClass2()));

        assertNull(val.field3);
    }

    /**
     * Test class with serialPersistentFields fields.
     */
    private static class TestClass implements Serializable {
        private static final long serialVersionUID = 0L;

        /** For serialization compatibility. */
        private static final ObjectStreamField[] serialPersistentFields = {
            new ObjectStreamField("field1", Integer.TYPE),
            new ObjectStreamField("field2", Integer.TYPE)
        };

        /**
         * @param s Object output stream.
         */
        private void writeObject(ObjectOutputStream s) throws IOException {
            s.putFields().put("field1", 1);
            s.putFields().put("field2", 2);
            s.writeFields();

            s.writeObject(null);
        }

        /**
         * @param s Object input stream.
         */
        private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
            s.defaultReadObject();

            s.readObject();
        }
    }

    /**
     * Test class with serialPersistentFields fields.
     */
    private static class TestClass2 implements Serializable {
        private static final long serialVersionUID = 0L;

        private Integer field3 = 1;

        /** For serialization compatibility. */
        private static final ObjectStreamField[] serialPersistentFields = {
            new ObjectStreamField("field1", Integer.TYPE),
            new ObjectStreamField("field2", Integer.TYPE)
        };

        /**
         * @param s Object output stream.
         */
        private void writeObject(ObjectOutputStream s) throws IOException {
            s.putFields().put("field1", 1);
            s.putFields().put("field2", 2);
            s.writeFields();

            s.writeObject(null);
        }

        /**
         * @param s Object input stream.
         */
        private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
            s.defaultReadObject();

            s.readObject();
        }
    }
}
