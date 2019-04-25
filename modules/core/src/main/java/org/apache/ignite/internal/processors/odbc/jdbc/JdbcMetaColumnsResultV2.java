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
public class JdbcMetaColumnsResultV2 extends JdbcMetaColumnsResult {
    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaColumnsResultV2() {
        super(META_COLUMNS_V2);
    }

    /**
     * @param meta Columns metadata.
     */
    JdbcMetaColumnsResultV2(Collection<JdbcColumnMeta> meta) {
        super(META_COLUMNS_V2, meta);
    }

    /** {@inheritDoc} */
    @Override protected JdbcColumnMeta createMetaColumn() {
        return new JdbcColumnMetaV2();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaColumnsResultV2.class, this);
    }
}
