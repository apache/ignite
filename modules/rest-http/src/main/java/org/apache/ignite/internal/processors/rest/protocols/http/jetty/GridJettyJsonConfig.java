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

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonBeanProcessor;
import net.sf.json.processors.JsonBeanProcessorMatcher;
import net.sf.json.processors.JsonValueProcessor;
import net.sf.json.processors.JsonValueProcessorMatcher;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Jetty protocol json configuration.
 */
class GridJettyJsonConfig extends JsonConfig {
    /** Logger. */
    private final IgniteLogger log;

    /**
     * Class for finding a matching JsonBeanProcessor.
     */
    private static final JsonBeanProcessorMatcher LESS_NAMING_BEAN_MATCHER = new JsonBeanProcessorMatcher() {
        /** {@inheritDoc} */
        @Override public Object getMatch(Class target, Set keys) {
            return GridJettyJsonConfig.getMatch(target, keys);
        }
    };

    /**
     * Class for finding a matching JsonValueProcessor.
     */
    private static final JsonValueProcessorMatcher LESS_NAMING_VALUE_MATCHER = new JsonValueProcessorMatcher() {
        /** {@inheritDoc} */
        @Override public Object getMatch(Class target, Set keys) {
            return GridJettyJsonConfig.getMatch(target, keys);
        }
    };

    /**
     * Helper class for conversion to JSONObject.
     */
    private static abstract class AbstractJsonValueProcessor implements JsonValueProcessor {
        /**
         * Processes the bean an returns a suitable JSONObject representation.
         *
         * @param bean the input bean
         * @param jsonCfg the current configuration environment
         */
        protected abstract Object processBean(Object bean, JsonConfig jsonCfg);

        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val instanceof Object[]) {
                final JSONArray ret = new JSONArray();

                for (Object bean : (Object[])val)
                    ret.add(processBean(bean, jsonCfg));

                return ret;
            }

            return processBean(val, jsonCfg);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return processArrayValue(val, jsonCfg);
        }
    }

    /**
     * Helper class for simple to-string conversion for {@link UUID}.
     */
    private static JsonValueProcessor UUID_PROCESSOR = new AbstractJsonValueProcessor() {
        /** {@inheritDoc} */
        protected Object processBean(Object bean, JsonConfig jsonCfg) {
            if (bean == null)
                return new JSONObject(true);

            if (bean instanceof UUID)
                return bean.toString();

            throw new UnsupportedOperationException("Serialize value to json is not supported: " + bean);
        }
    };

    /**
     * Helper class for simple to-string conversion for {@link UUID}.
     */
    private static JsonValueProcessor IGNITE_BI_TUPLE_PROCESSOR = new AbstractJsonValueProcessor() {
        /** {@inheritDoc} */
        protected Object processBean(Object bean, JsonConfig jsonCfg) {
            if (bean == null)
                return new JSONObject(true);

            if (bean instanceof IgniteBiTuple) {
                IgniteBiTuple t2 = (IgniteBiTuple)bean;

                final JSONObject ret = new JSONObject();

                ret.element("key", t2.getKey(), jsonCfg);
                ret.element("value", t2.getValue(), jsonCfg);

                return ret;
            }

            throw new UnsupportedOperationException("Serialize value to json is not supported: " + bean);
        }
    };

    /**
     * Helper class for simple to-string conversion for {@link IgniteUuid}.
     */
    private static JsonValueProcessor IGNITE_UUID_PROCESSOR = new AbstractJsonValueProcessor() {
        /** {@inheritDoc} */
        protected Object processBean(Object bean, JsonConfig jsonCfg) {
            if (bean == null)
                return new JSONObject(true);

            if (bean instanceof IgniteUuid)
                return bean.toString();

            throw new UnsupportedOperationException("Serialize value to json is not supported: " + bean);
        }
    };

    /**
     * Helper class for simple to-string conversion for {@link Date}.
     */
    private static JsonValueProcessor DATE_PROCESSOR = new AbstractJsonValueProcessor() {
        /** Thread local US date format. */
        private final ThreadLocal<DateFormat> dateFmt = new ThreadLocal<DateFormat>() {
            @Override protected DateFormat initialValue() {
                return DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.US);
            }
        };

        /** {@inheritDoc} */
        protected Object processBean(Object bean, JsonConfig jsonCfg) {
            if (bean == null)
                return new JSONObject(true);

            if (bean instanceof Date)
                return dateFmt.get().format(bean);

            throw new UnsupportedOperationException("Serialize value to json is not supported: " + bean);
        }
    };

    /**
     * Constructs default jetty json config.
     */
    GridJettyJsonConfig(IgniteLogger log) {
        this.log = log;

        setAllowNonStringKeys(true);

        registerJsonValueProcessor(IgniteBiTuple.class, IGNITE_BI_TUPLE_PROCESSOR);
        registerJsonValueProcessor(UUID.class, UUID_PROCESSOR);
        registerJsonValueProcessor(IgniteUuid.class, IGNITE_UUID_PROCESSOR);
        registerJsonValueProcessor(Date.class, DATE_PROCESSOR);
        registerJsonValueProcessor(java.sql.Date.class, DATE_PROCESSOR);

        final LessNamingProcessor lessNamingProcessor = new LessNamingProcessor();

        registerJsonBeanProcessor(LessNamingProcessor.class, lessNamingProcessor);
        registerJsonValueProcessor(LessNamingProcessor.class, lessNamingProcessor);

        setJsonBeanProcessorMatcher(LESS_NAMING_BEAN_MATCHER);
        setJsonValueProcessorMatcher(LESS_NAMING_VALUE_MATCHER);
    }

    /**
     * Returns the matching class calculated with the target class and the provided set. Matches the target class with
     * instanceOf, for Visor classes return custom processor class.
     *
     * @param target the target class to match
     * @param keys a set of possible matches
     */
    private static Object getMatch(Class target, Set keys) {
        if (target == null || keys == null)
            return null;

        if (target.getSimpleName().startsWith("Visor") ||
            GridCacheSqlMetadata.class.isAssignableFrom(target) ||
            GridCacheSqlIndexMetadata.class.isAssignableFrom(target))
            return LessNamingProcessor.class;

        if (keys.contains(target))
            return target;

        for (Object key : keys) {
            Class<?> clazz = (Class<?>)key;

            if (clazz.isAssignableFrom(target))
                return key;
        }

        return null;
    }

    /**
     * Helper class for simple to-json conversion for Visor classes.
     */
    private class LessNamingProcessor implements JsonBeanProcessor, JsonValueProcessor {
        /** Methods to exclude. */
        private final Collection<String> exclMtds = Arrays.asList("toString", "hashCode", "clone", "getClass");

        /** */
        private final Map<Class<?>, Collection<Method>> clsCache = new HashMap<>();

        /** */
        private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

        /** {@inheritDoc} */
        @Override public JSONObject processBean(Object bean, JsonConfig jsonCfg) {
            if (bean == null)
                return new JSONObject(true);

            final JSONObject ret = new JSONObject();

            Collection<Method> methods;

            Class<?> cls = bean.getClass();

            // Get descriptor from cache.
            rwLock.readLock().lock();

            try {
                methods = clsCache.get(cls);
            }
            finally {
                rwLock.readLock().unlock();
            }

            // If missing in cache - build descriptor
            if (methods == null) {
                Method[] publicMtds = cls.getMethods();

                methods = new ArrayList<>(publicMtds.length);

                for (Method mtd : publicMtds) {
                    Class retType = mtd.getReturnType();

                    if (mtd.getParameterTypes().length != 0 ||
                        retType == void.class ||
                        retType == cls ||
                        exclMtds.contains(mtd.getName()) ||
                        (retType == VisorCache.class && mtd.getName().equals("history")))
                        continue;

                    mtd.setAccessible(true);

                    methods.add(mtd);
                }

                /*
                 * Allow multiple puts for the same class - they will simply override.
                 */
                rwLock.writeLock().lock();

                try {
                    clsCache.put(cls, methods);
                }
                finally {
                    rwLock.writeLock().unlock();
                }
            }

            // Extract fields values using descriptor and build JSONObject.
            for (Method mtd : methods) {
                try {
                    ret.element(mtd.getName(), mtd.invoke(bean), jsonCfg);
                }
                catch (Exception e) {
                    U.error(log, "Failed to read object property [type= " + cls.getName()
                        + ", property=" + mtd.getName() + "]", e);
                }
            }

            return ret;
        }

        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val instanceof Object[]) {
                final JSONArray ret = new JSONArray();

                for (Object bean : (Object[])val)
                    ret.add(processBean(bean, jsonCfg));

                return ret;
            }

            return processBean(val, jsonCfg);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return processArrayValue(val, jsonCfg);
        }
    }
}
