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