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

package org.apache.ignite.configuration;

import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Smoke test to validate CacheConfiguration#toString changes.
 */
public class CacheConfigurationToStringTest {

    /**
     * Checks that collection fields are printed normally.
     */
    @Test
    public void testCollectionFields() {
        CacheConfiguration<Object, Object> ccfg =  new CacheConfiguration<>();

        ccfg.setName("foo");

        ccfg.setPluginConfigurations(new MyCachePluginConfiguration<>("bar"));

        ccfg.setQueryEntities(Arrays.asList(
                new QueryEntity(String.class, String.class),
                new QueryEntity(Integer.class, Integer.class)
        ));

        ccfg.setKeyConfiguration(new CacheKeyConfiguration(String.class));

        ccfg.setSqlFunctionClasses(MyFakeSqlFunctions.class);

        String str = ccfg.toString();

        assertTrue(str, str.contains("keyCfg=CacheKeyConfiguration[] [CacheKeyConfiguration [typeName=java.lang.String, affKeyFieldName=null]]"));
        assertTrue(str, str.contains("QueryEntity [keyType=java.lang.Integer, valType=java.lang.Integer"));
        assertTrue(str, str.contains("QueryEntity [keyType=java.lang.String, valType=java.lang.String"));
        assertTrue(str, str.contains("pluginCfgs=CachePluginConfiguration[] [MyCachePluginConfiguration: bar]"));
        assertTrue(str, str.contains("sqlFuncCls=Class[] [class org.apache.ignite.configuration.CacheConfigurationToStringTest$MyFakeSqlFunctions]"));
    }

    private static class MyFakeSqlFunctions { }

    private static class MyCachePluginConfiguration<K, V> implements CachePluginConfiguration<K, V> {
        private String a;

        public MyCachePluginConfiguration(String a) {
            this.a = a;
        }

        @Override
        public String toString() {
            return "MyCachePluginConfiguration: " + a;
        }
    }
}
