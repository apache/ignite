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
 * Type handler for convert java {@link Float} &lt;-&gt; cassandra 'float'
 */
public class PrimitiveFloatTypeHandler implements TypeHandler<Float, Float> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Float toJavaType(Row row, int index) {
        if(row.isNull(index)) {
            throw new IllegalArgumentException("Can't cast null value from Cassandra table column index '" + index +
                    "' to " + "float value used in domain object model");
        }
        return row.getFloat(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float toJavaType(Row row, String col) {
        if(row.isNull(col)) {
            throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                    "' to " + "float value used in domain object model");
        }
        return row.getFloat(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float toCassandraPrimitiveType(Float javaValue) {
        return javaValue;
    }

    /**
     * Get 'float' cassandra type.
     * @return 'float' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.FLOAT.toString();
    }
}
