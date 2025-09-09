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

package org.apache.ignite.internal.processors.cache.index;

import java.io.File;
import java.nio.file.Paths;
import javax.sql.DataSource;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class H2ConnectionSettingsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testInitForbidden() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName())
            .setCacheConfiguration(cacheConfiguration());

        GridTestUtils.assertThrowsWithCause(() -> startGrid(cfg), org.h2.jdbc.JdbcSQLException.class);

        assertFalse(checkFile().exists());
    }

    /** */
    @Test
    public void testInitForbiddenInDynamic() throws Exception {
        IgniteEx ign = startGrid();

        IgniteEx cln = startClientGrid(1);

        GridTestUtils.assertThrowsWithCause(() -> cln.createCache(cacheConfiguration()), org.h2.jdbc.JdbcSQLException.class);

        assertFalse(checkFile().exists());

        assertNotNull(ign.createCache(DEFAULT_CACHE_NAME));
    }

    /** */
    private CacheConfiguration<Integer, Integer> cacheConfiguration() throws Exception {
        CacheJdbcPojoStoreFactory<Integer, Integer> factory = new CacheJdbcPojoStoreFactory<>();

        checkFile().delete();

        String init = "//javascript\njava.lang.Runtime.getRuntime().exec(\"touch " + checkFile().getAbsolutePath() + " \")";
        String url = "jdbc:h2:mem:test;init=CREATE TRIGGER h BEFORE SELECT ON INFORMATION_SCHEMA.CATALOGS AS '" + init + "'";

        IgniteReflectionFactory<DataSource> reflectionFactory = new IgniteReflectionFactory<>(org.h2.jdbcx.JdbcDataSource.class);
        reflectionFactory.setProperties(F.asMap("url", url));

        return new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheStoreFactory(factory.setDataSourceFactory(reflectionFactory));
    }

    /** */
    private File checkFile() throws IgniteCheckedException {
        return Paths.get(U.defaultWorkDirectory(), "init_check").toFile();
    }
}
