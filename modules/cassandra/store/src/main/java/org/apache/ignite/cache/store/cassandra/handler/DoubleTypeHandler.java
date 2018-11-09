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
 * Type handler for convert java {@link Double} &lt;-&gt; cassandra 'double'
 */
public class DoubleTypeHandler implements TypeHandler<Double, Double> {
    private static final long serialVersionUID = 2165323112631971960L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Double toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            return null;
        }
        return row.getDouble(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            return null;
        }
        return row.getDouble(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double toCassandraPrimitiveType(Double javaValue) {
        return javaValue;
    }

    /**
     * Get 'double' cassandra type.
     * @return 'double' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.DOUBLE.toString();
    }
}
