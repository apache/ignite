/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
public class JdbcMetaColumnsResultV3 extends JdbcMetaColumnsResult {
    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaColumnsResultV3() {
        super(META_COLUMNS_V3);
    }

    /**
     * @param meta Columns metadata.
     */
    JdbcMetaColumnsResultV3(Collection<JdbcColumnMeta> meta) {
        super(META_COLUMNS_V3, meta);
    }

    /** {@inheritDoc} */
    @Override protected JdbcColumnMeta createMetaColumn() {
        return new JdbcColumnMetaV3();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaColumnsResultV3.class, this);
    }
}
