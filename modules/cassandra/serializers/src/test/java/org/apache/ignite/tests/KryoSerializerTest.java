package org.apache.ignite.tests;

import org.apache.ignite.cache.store.cassandra.serializer.KryoSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;

public class KryoSerializerTest {
    @Test public void simpleTest() {
        MyPojo pojo1 = new MyPojo("123", 1, 123423453467L, new Date(), null);

        KryoSerializer ser = new KryoSerializer();

        ByteBuffer buff = ser.serialize(pojo1);
        MyPojo pojo2 = (MyPojo)ser.deserialize(buff);

        if (!pojo1.equals(pojo2))
            throw new RuntimeException("Kryo simple serialization test failed");
    }

    @Test public void cyclicStructureTest() {
        MyPojo pojo1 = new MyPojo("123", 1, 123423453467L, new Date(), null);
        MyPojo pojo2 = new MyPojo("321", 2, 123456L, new Date(), pojo1);
        pojo1.setRef(pojo2);

        KryoSerializer ser = new KryoSerializer();

        ByteBuffer buff1 = ser.serialize(pojo1);
        ByteBuffer buff2 = ser.serialize(pojo2);

        MyPojo pojo3 = (MyPojo)ser.deserialize(buff1);
        MyPojo pojo4 = (MyPojo)ser.deserialize(buff2);

        if (!pojo1.equals(pojo3) || !pojo1.getRef().equals(pojo3.getRef()))
            throw new RuntimeException("Kryo cyclic structure serialization test failed");

        if (!pojo2.equals(pojo4) || !pojo2.getRef().equals(pojo4.getRef()))
            throw new RuntimeException("Kryo cyclic structure serialization test failed");
    }
}
