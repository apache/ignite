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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.text.DateFormat;
import java.util.UUID;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonBeanProcessor;
import net.sf.json.processors.JsonBeanProcessorMatcher;
import net.sf.json.processors.JsonValueProcessor;

import java.util.*;
import net.sf.json.processors.JsonValueProcessorMatcher;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;

/**
 * Jetty protocol json configuration.
 */
public class GridJettyJsonConfig extends JsonConfig {
    /**
     * Constructs default jetty json config.
     */
    public GridJettyJsonConfig() {
        registerJsonValueProcessor(UUID.class, new UUIDToStringJsonProcessor());
        registerJsonValueProcessor(Date.class, new DateToStringJsonProcessor());
        registerJsonValueProcessor(java.sql.Date.class, new DateToStringJsonProcessor());

        registerJsonBeanProcessor(GridCacheSqlMetadata.class, new GridCacheSqlMetadataBeanProcessor());
        registerJsonValueProcessor(GridCacheSqlIndexMetadata.class, new GridCacheSqlIndexMetadataToJson());

        setJsonBeanProcessorMatcher(new GridJettyJsonBeanProcessorMatcher());
        setJsonValueProcessorMatcher(new GridJettyJsonValueProcessorMatcher());
    }

    /**
     * Helper class for simple to-string conversion for {@link UUID}.
     */
    private static class UUIDToStringJsonProcessor implements JsonValueProcessor {
        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val == null)
                return new JSONObject(true);

            if (val instanceof UUID)
                return val.toString();

            throw new UnsupportedOperationException("Serialize value to json is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return processArrayValue(val, jsonCfg);
        }
    }

    /**
     * Helper class for simple to-string conversion for {@link Date}.
     */
    private static class DateToStringJsonProcessor implements JsonValueProcessor {
        private final DateFormat enUsFormat
            =  DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.US);

        /** {@inheritDoc} */
        @Override public synchronized Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val == null)
                return new JSONObject(true);

            if (val instanceof Date)
                return enUsFormat.format(val);

            throw new UnsupportedOperationException("Serialize value to json is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public synchronized Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return processArrayValue(val, jsonCfg);
        }
    }

    /**
     * Helper class for simple to-json conversion for {@link GridCacheSqlMetadata}.
     */
    private static class GridCacheSqlMetadataBeanProcessor implements JsonBeanProcessor {
        /** {@inheritDoc} */
        @Override public JSONObject processBean(Object bean, JsonConfig jsonCfg) {
            if (bean == null)
                return new JSONObject(true);

            if (bean instanceof GridCacheSqlMetadata) {
                GridCacheSqlMetadata r = (GridCacheSqlMetadata) bean;

                return new JSONObject()
                    .element("cacheName", r.cacheName(), jsonCfg)
                    .element("types", r.types(), jsonCfg)
                    .element("keyClasses", r.keyClasses(), jsonCfg)
                    .element("valClasses", r.valClasses(), jsonCfg)
                    .element("fields", r.fields(), jsonCfg)
                    .element("indexes", r.indexes(), jsonCfg);
            }

            throw new UnsupportedOperationException("Serialize bean to json is not supported: " + bean);
        }
    }

    /**
     * Helper class for simple to-json conversion for {@link GridCacheSqlIndexMetadata}.
     */
    private static class GridCacheSqlIndexMetadataToJson implements JsonValueProcessor {
        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val == null)
                return new JSONObject(true);

            if (val instanceof GridCacheSqlIndexMetadata) {
                GridCacheSqlIndexMetadata r = (GridCacheSqlIndexMetadata) val;

                return new JSONObject()
                    .element("name", r.name())
                    .element("fields", r.fields())
                    .element("descendings", r.descendings())
                    .element("unique", r.unique());
            }

            throw new UnsupportedOperationException("Serialize array to string is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object value, JsonConfig jsonCfg) {
            return processArrayValue(value, jsonCfg);
        }
    }

    /**
     * Class for finding a matching JsonBeanProcessor. Matches the target class with instanceOf.
     */
    private static final class GridJettyJsonBeanProcessorMatcher extends JsonBeanProcessorMatcher {
        /** {@inheritDoc} */
        @Override public Object getMatch(Class target, Set keys) {
            if (target == null || keys == null)
                return null;

            if (keys.contains(target))
                return target;

            for (Object key : keys) {
                Class<?> clazz = (Class<?>) key;

                if (clazz.isAssignableFrom(target))
                    return key;
            }

            return null;
        }
    }

    /**
     * Class for finding a matching JsonValueProcessor. Matches the target class with instanceOf.
     */
    private static final class GridJettyJsonValueProcessorMatcher extends JsonValueProcessorMatcher {
        /** {@inheritDoc} */
        @Override public Object getMatch(Class target, Set keys) {
            if (target == null || keys == null)
                return null;

            if (keys.contains(target))
                return target;

            for (Object key : keys) {
                Class<?> clazz = (Class<?>) key;

                if (clazz.isAssignableFrom(target))
                    return key;
            }

            return null;
        }
    }
}
