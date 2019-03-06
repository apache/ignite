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

package org.apache.ignite.tests;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.ignite.cache.store.cassandra.serializer.KryoSerializer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Simple test for KryoSerializer.
 */
public class KryoSerializerTest {
    /**
     * Serialize simple object test.
     */
    @Test
    public void simpleTest() {
        MyPojo pojo1 = new MyPojo("123", 1, 123423453467L, new Date(), null);

        KryoSerializer ser = new KryoSerializer();

        ByteBuffer buff = ser.serialize(pojo1);
        MyPojo pojo2 = (MyPojo)ser.deserialize(buff);

        assertEquals("Kryo simple serialization test failed", pojo1, pojo2);
    }

    /**
     * Serialize object with cyclic references test.
     */
    @Test
    public void cyclicStructureTest() {
        MyPojo pojo1 = new MyPojo("123", 1, 123423453467L, new Date(), null);
        MyPojo pojo2 = new MyPojo("321", 2, 123456L, new Date(), pojo1);
        pojo1.setRef(pojo2);

        KryoSerializer ser = new KryoSerializer();

        ByteBuffer buff1 = ser.serialize(pojo1);
        ByteBuffer buff2 = ser.serialize(pojo2);

        MyPojo pojo3 = (MyPojo)ser.deserialize(buff1);
        MyPojo pojo4 = (MyPojo)ser.deserialize(buff2);

        assertEquals("Kryo cyclic structure serialization test failed", pojo1, pojo3);
        assertEquals("Kryo cyclic structure serialization test failed", pojo1.getRef(), pojo3.getRef());
        assertEquals("Kryo cyclic structure serialization test failed", pojo2, pojo4);
        assertEquals("Kryo cyclic structure serialization test failed", pojo2.getRef(), pojo4.getRef());
    }
}
