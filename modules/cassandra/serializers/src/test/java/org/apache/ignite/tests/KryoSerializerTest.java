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
