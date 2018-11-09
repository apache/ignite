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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Type handler for convert java {@link Short} &lt;-&gt; cassandra 'smallint'
 */
public class PrimitiveShortTypeHandler implements TypeHandler<Short, Short> {
    private static final long serialVersionUID = 7462142021805511717L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Short toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            throw new IllegalArgumentException("Can't cast null value from Cassandra table column index '" + index +
                    "' to " + "short value used in domain object model");
        }
        return row.getShort(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Short toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                    "' to " + "short value used in domain object model");
        }
        return row.getShort(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Short toCassandraPrimitiveType(Short javaValue) {
        return javaValue;
    }

    /**
     * Get 'smallint' cassandra type.
     * @return 'smallint' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.SMALLINT.toString();
    }
}
