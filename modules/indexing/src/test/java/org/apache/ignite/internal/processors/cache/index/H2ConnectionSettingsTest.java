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
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class H2ConnectionSettingsTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testForbidInitSetting() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName());

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        CacheJdbcPojoStoreFactory<Integer, Integer> factory = new CacheJdbcPojoStoreFactory<>();

        File checkFile = Paths.get(U.defaultWorkDirectory(), "init_check").toFile();

        checkFile.delete();

        String init = "//javascript\njava.lang.Runtime.getRuntime().exec(\"touch " + checkFile.getAbsolutePath() + " \")";
        String url = "jdbc:h2:mem:test;init=CREATE TRIGGER h BEFORE SELECT ON INFORMATION_SCHEMA.CATALOGS AS '" + init + "'";

        IgniteReflectionFactory<DataSource> reflectionFactory = new IgniteReflectionFactory<>(org.h2.jdbcx.JdbcDataSource.class);
        reflectionFactory.setProperties(F.asMap("url", url));

        ccfg.setCacheStoreFactory(factory.setDataSourceFactory(reflectionFactory));

        cfg.setCacheConfiguration(ccfg);

        boolean started = false;

        try {
            startGrid(cfg);

            started = true;
        }
        catch (Exception err) {
            // Ignore.
        }

        assert !started : "Ignite start should fail";

        assertFalse(checkFile.exists());
    }
}
