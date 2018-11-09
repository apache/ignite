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

import java.math.BigDecimal;

/**
 * Type handler for convert java {@link BigDecimal} &lt;-&gt; cassandra 'decimal'
 */
public class BigDecimalTypeHandler implements TypeHandler<BigDecimal, BigDecimal> {
    private static final long serialVersionUID = -3250017425792473843L;

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal toJavaType(Row row, int index) {
        return row.getDecimal(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal toJavaType(Row row, String col) {
        return row.getDecimal(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal toCassandraPrimitiveType(BigDecimal javaValue) {
        return javaValue;
    }

    /**
     * Get 'decimal' cassandra type.
     * @return 'decimal' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.DECIMAL.toString();
    }
}
