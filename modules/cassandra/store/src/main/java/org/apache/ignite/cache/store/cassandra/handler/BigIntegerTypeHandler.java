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

import java.math.BigInteger;

/**
 * Type handler for convert java {@link BigInteger} &lt;-&gt; cassandra 'varint'
 */
public class BigIntegerTypeHandler implements TypeHandler<BigInteger, BigInteger> {
    private static final long serialVersionUID = 887012126046068873L;

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger toJavaType(Row row, int index) {
        return row.getVarint(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger toJavaType(Row row, String col) {
        return row.getVarint(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger toCassandraPrimitiveType(BigInteger javaValue) {
        return javaValue;
    }

    /**
     * Get 'varint' cassandra type.
     * @return 'varint' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.VARINT.toString();
    }
}
