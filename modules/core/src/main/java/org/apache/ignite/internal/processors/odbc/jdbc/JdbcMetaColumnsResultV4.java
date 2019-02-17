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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC columns metadata result.
 */
public class JdbcMetaColumnsResultV4 extends JdbcMetaColumnsResult {
    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaColumnsResultV4() {
        super(META_COLUMNS_V4);
    }

    /**
     * @param meta Columns metadata.
     */
    JdbcMetaColumnsResultV4(Collection<JdbcColumnMeta> meta) {
        super(META_COLUMNS_V4, meta);
    }

    /** {@inheritDoc} */
    @Override protected JdbcColumnMeta createMetaColumn() {
        return new JdbcColumnMetaV4();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaColumnsResultV4.class, this);
    }
}
