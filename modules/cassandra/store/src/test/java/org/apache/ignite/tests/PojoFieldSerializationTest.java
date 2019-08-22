package org.apache.ignite.tests;

import java.nio.ByteBuffer;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.persistence.PojoField;
import org.apache.ignite.cache.store.cassandra.persistence.PojoFieldAccessor;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.junit.Assert.assertEquals;

/**
 * Test for PojoField serialization.
 */
public class PojoFieldSerializationTest {

    private static final String NAME_VALUE = "pojo";
    private static final String NAME_FIELD = "name";

    private static class MockPojoField extends PojoField {

        public MockPojoField(final Element el, final Class<?> pojoCls) {
            super(el, pojoCls);
        }

        public MockPojoField(final PojoFieldAccessor accessor) {
            super(accessor);
        }
    }

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
    public void serializationTestWithAccessorConstructor() {
        final MockPojoField src = new MockPojoField(PropertyMappingHelper.getPojoFieldAccessor(Pojo.class, NAME_FIELD));
        final JavaSerializer serializer = new JavaSerializer();

        final ByteBuffer buff = serializer.serialize(src);
        final MockPojoField _src = (MockPojoField)serializer.deserialize(buff);

        final String value = (String)_src.getValueFromObject(new Pojo(NAME_VALUE), null);
        assertEquals("Incorrectly serialized PojoField failed to access value", NAME_VALUE, value);
    }

    /**
     * Serialization test.
     */
    @Test
    public void serializationTestWithElementConstructor() throws ParserConfigurationException {
        final Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        final Element el = document.createElement("field");
        el.setAttribute(NAME_FIELD, NAME_FIELD);
        el.setAttribute("col", NAME_FIELD);
        final MockPojoField src = new MockPojoField(el, Pojo.class);
        final JavaSerializer serializer = new JavaSerializer();

        final ByteBuffer buff = serializer.serialize(src);
        final MockPojoField _src = (MockPojoField)serializer.deserialize(buff);

        final String value = (String)_src.getValueFromObject(new Pojo(NAME_VALUE), null);
        assertEquals("Incorrectly serialized PojoField failed to access value", NAME_VALUE, value);
    }
}
