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

import java.util.Date;

/**
 * Type handler for convert java {@link Date} &lt;-&gt; cassandra 'timestamp'
 */
public class DateTypeHandler implements TypeHandler<Date, Date> {
    private static final long serialVersionUID = -408976844667561421L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toJavaType(Row row, int index) {
        return row.getTimestamp(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toJavaType(Row row, String col) {
        return row.getTimestamp(col);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date toCassandraPrimitiveType(Date javaValue) {
        return javaValue;
    }

    /**
     * Get 'timestamp' cassandra type.
     * @return 'timestamp' cassandra type
     */
    @Override
    public String getDDLType() {
        return DataType.Name.TIMESTAMP.toString();
    }
}
