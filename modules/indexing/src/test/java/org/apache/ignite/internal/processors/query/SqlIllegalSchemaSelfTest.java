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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.CacheException;
import java.util.concurrent.Callable;

/**
 * Tests for illegal SQL schemas in node and cache configurations.
 */
@SuppressWarnings({"ThrowableNotThrown", "unchecked"})
public class SqlIllegalSchemaSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadCacheName() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setCacheConfiguration(new CacheConfiguration().setName(QueryUtils.SCHEMA_SYS));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignition.start(cfg);

                return null;
            }
        }, IgniteException.class, "SQL schema name derived from cache name is reserved (please set explicit SQL " +
            "schema name through CacheConfiguration.setSqlSchema() or choose another cache name) [cacheName=IGNITE, " +
            "schemaName=null]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadCacheNameDynamic() throws Exception {
        Ignite node = startGrid();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.getOrCreateCache(new CacheConfiguration().setName(QueryUtils.SCHEMA_SYS));

                return null;
            }
        }, CacheException.class, "SQL schema name derived from cache name is reserved (please set explicit SQL " +
            "schema name through CacheConfiguration.setSqlSchema() or choose another cache name) [" +
            "cacheName=IGNITE, schemaName=null]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadSchemaLower() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setCacheConfiguration(new CacheConfiguration().setName("CACHE")
            .setSqlSchema(QueryUtils.SCHEMA_SYS.toLowerCase()));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignition.start(cfg);

                return null;
            }
        }, IgniteException.class, "SQL schema name is reserved (please choose another one) [cacheName=CACHE, " +
            "schemaName=ignite]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadSchemaLowerDynamic() throws Exception {
        Ignite node = startGrid();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.getOrCreateCache(
                    new CacheConfiguration().setName("CACHE").setSqlSchema(QueryUtils.SCHEMA_SYS.toLowerCase())
                );

                return null;
            }
        }, CacheException.class, "SQL schema name is reserved (please choose another one) [cacheName=CACHE, schemaName=ignite]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadSchemaUpper() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setCacheConfiguration(new CacheConfiguration().setName("CACHE")
            .setSqlSchema(QueryUtils.SCHEMA_SYS.toUpperCase()));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignition.start(cfg);

                return null;
            }
        }, IgniteException.class, "SQL schema name is reserved (please choose another one) [cacheName=CACHE, " +
            "schemaName=IGNITE]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadSchemaUpperDynamic() throws Exception {
        Ignite node = startGrid();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.getOrCreateCache(
                    new CacheConfiguration().setName("CACHE").setSqlSchema(QueryUtils.SCHEMA_SYS.toUpperCase())
                );

                return null;
            }
        }, CacheException.class, "SQL schema name is reserved (please choose another one) [cacheName=CACHE, " +
            "schemaName=IGNITE]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadSchemaQuoted() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setCacheConfiguration(new CacheConfiguration().setName("CACHE")
            .setSqlSchema("\"" + QueryUtils.SCHEMA_SYS.toUpperCase() + "\""));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignition.start(cfg);

                return null;
            }
        }, IgniteException.class, "SQL schema name is reserved (please choose another one) [cacheName=CACHE, " +
            "schemaName=\"IGNITE\"]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testBadSchemaQuotedDynamic() throws Exception {
        Ignite node = startGrid();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.getOrCreateCache(
                    new CacheConfiguration().setName("CACHE")
                        .setSqlSchema("\"" + QueryUtils.SCHEMA_SYS.toUpperCase() + "\"")
                );

                return null;
            }
        }, CacheException.class, "SQL schema name is reserved (please choose another one) [cacheName=CACHE, " +
            "schemaName=\"IGNITE\"]");
    }
}
