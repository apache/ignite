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

package org.apache.ignite.cache.store.cassandra.handler;

import com.datastax.driver.core.Row;

import java.io.Serializable;

/**
 * Interface which should be implemented by all handlers responsible
 * for converting data to/from primitive Cassandra type.
 */
public interface TypeHandler<J, C> extends Serializable {
    /**
     * Get primitive Cassandra type object from database and convert to complex Java type object
     *
     * @param row row from Cassandra
     * @param index index column in row
     * @return data from Cassandra converted to complex java type object
     */
    J toJavaType(Row row, int index);

    /**
     * Get primitive Cassandra type object from database and convert to complex Java type object
     *
     * @param row row from Cassandra
     * @param col name column in row
     * @return data from Cassandra converted to complex java type object
     */
    J toJavaType(Row row, String col);

    /**
     * Convert complex Java type object to primitive Cassandra type object
     *
     * @param javaValue complex Java type object
     * @return data converted to a primitive Cassandra type object
     */
    C toCassandraPrimitiveType(J javaValue);

    /**
     * Get DDL for cassandra type.
     *
     * @return DDL type
     */
    String getDDLType();
}
