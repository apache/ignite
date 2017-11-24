package org.apache.ignite.tests.cache.store.cassandra.common;

import com.datastax.driver.core.Row;
import org.apache.ignite.cache.store.cassandra.common.TypeHandler;
import org.apache.ignite.cache.store.cassandra.common.TypeHandlerHelper;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;


public class TypeHandlerHelperTest {


    @Test
    public void should_() {
        //given
        String handlerName = TestHandler1.class.getName();
        //when
        TypeHandler<?, ?> actual = TypeHandlerHelper.getInstanceFromClassName(handlerName);
        //then
        assertThat(actual).isInstanceOf(TestHandler1.class);
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
        public Class<Integer> getClazz() {
            return Integer.class;
        }
    }

}