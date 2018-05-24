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

package org.apache.ignite.cache.store.jdbc;

import java.net.URL;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Tests for {@link CacheJdbcPojoStore} created via XML.
 */
public class CachePojoStoreXmlSelfTest extends CacheJdbcPojoStoreAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        String path = builtinKeys
            ? "modules/spring/src/test/config/jdbc-pojo-store-builtin.xml"
            : "modules/spring/src/test/config/jdbc-pojo-store-obj.xml";

        URL url = U.resolveIgniteUrl(path);

        IgniteSpringHelper spring = IgniteComponentType.SPRING.create(false);

        IgniteConfiguration cfg = spring.loadConfigurations(url).get1().iterator().next();

        if (sqlEscapeAll()) {
            for (CacheConfiguration ccfg : cfg.getCacheConfiguration())
                ((CacheJdbcPojoStoreFactory)ccfg.getCacheStoreFactory()).setSqlEscapeAll(true);
        }

        cfg.setIgniteInstanceName(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Marshaller marshaller() {
        return null;
    }
}
