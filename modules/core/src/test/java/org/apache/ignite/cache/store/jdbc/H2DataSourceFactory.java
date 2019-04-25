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

package org.apache.ignite.cache.store.jdbc;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * Datasource to use for store tests.
 */
public class H2DataSourceFactory implements Factory<DataSource> {
    /** DB connection URL. */
    private static final String DFLT_CONN_URL = "jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1";

    /** {@inheritDoc} */
    @Override public DataSource create() {
        return JdbcConnectionPool.create(DFLT_CONN_URL, "sa", "");
    }
}
