package org.apache.ignite.tests.cache.store.cassandra.handler;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.handler.TypeHandler;
import org.apache.ignite.cache.store.cassandra.handler.TypeHandlerHelper;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;


public class TypeHandlerHelperTest {
    @Test
    public void should_create_and_cached_type_handler_object() {
        //given
        String handlerName = TestHandler1.class.getName();
        //when
        TypeHandler<?, ?> actual = TypeHandlerHelper.getInstanceFromClassName(handlerName);
        TypeHandler<?, ?> secondActual = TypeHandlerHelper.getInstanceFromClassName(handlerName);
        //then
        assertThat(actual)
                .isInstanceOf(TestHandler1.class)
                .isEqualTo(secondActual);
    }

    public static class TestHandler1 implements TypeHandler<String,Integer> {

        @Override
        public String toJavaType(Row row, int index) {
            return null;
        }

        @Override
        public String toJavaType(Row row, String col) {
            return null;
        }

        @Override
        public Integer toCassandraPrimitiveType(String javaValue) {
            return null;
        }

        @Override
        public String getDDLType() {
            return DataType.Name.INT.toString();
        }
    }

}