package org.apache.ignite.tests;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.apache.ignite.cache.store.cassandra.persistence.PojoFieldAccessor;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for PojoField serialization.
 */
public class PojoFieldAccessorSerializationTest {

    private static final String NAME_VALUE = "pojo";
    private static final String NAME_FIELD = "name";

    private static class Pojo {

        private String name;

        public Pojo(final String name) {
            this.name = name;
        }
    }

    /**
     * Serialization test.
     */
    @Test
    public void serializationTestWithFieldConstructor() throws NoSuchFieldException {
        final Field field = Pojo.class.getDeclaredField(NAME_FIELD);
        final PojoFieldAccessor src = new PojoFieldAccessor(field);
        final JavaSerializer serializer = new JavaSerializer();

        final ByteBuffer buff = serializer.serialize(src);
        final PojoFieldAccessor _src = (PojoFieldAccessor)serializer.deserialize(buff);

        final String name = _src.getName();
        assertEquals("Incorrectly serialized PojoFieldAccessor name-property", NAME_FIELD, name);
        final Class<?> clazz = _src.getFieldType();
        assertEquals("Incorrectly serialized PojoFieldAccessor class-property", clazz, String.class);
    }

    /**
     * Serialization test.
     */
    @Test
    public void serializationTestWithPropertyDescriptorConstructor() throws NoSuchFieldException {
        final Field field = Pojo.class.getDeclaredField(NAME_FIELD);
        final PojoFieldAccessor src = new PojoFieldAccessor(field);
        final JavaSerializer serializer = new JavaSerializer();

        final ByteBuffer buff = serializer.serialize(src);
        final PojoFieldAccessor _src = (PojoFieldAccessor)serializer.deserialize(buff);

        final String name = _src.getName();
        assertEquals("Incorrectly serialized PojoFieldAccessor name-property", NAME_FIELD, name);
        final Class<?> clazz = _src.getFieldType();
        assertEquals("Incorrectly serialized PojoFieldAccessor class-property", clazz, String.class);
    }
}
