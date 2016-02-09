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

package org.apache.ignite.testframework.config.params;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;

/**
 * Enum variants.
 */
public class Parameters {
    /**
     * Private constructor.
     */
    private Parameters() {
        // No-op.
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] enumParameters(String mtdName, Class<?> enumCls) {
        return enumParameters(false, mtdName, enumCls);
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] enumParameters(boolean withNull, String mtdName, Class<?> enumCls) {
        return parameters0(mtdName, withNull, enumCls.getEnumConstants());
    }

    /**
     * @param mtdName Method name.
     * @param values Values.
     * @return Array of configuration paramethers.
     */
    @SuppressWarnings("unchecked")
    private static <T> ConfigurationParameter<T>[] parameters0(String mtdName, boolean withNull, Object[] values) {
        if (withNull) {
            Object[] valuesWithNull = new Object[values.length + 1];

            valuesWithNull[0] = null;

            System.arraycopy(values, 0, valuesWithNull, 1, valuesWithNull.length - 1);

            values = valuesWithNull;
        }

        assert values != null && values.length > 0 : "MtdName:" + mtdName;

        ConfigurationParameter<T>[] resArr = new ConfigurationParameter[values.length];

        for (int i = 0; i < resArr.length; i++)
            resArr[i] = new ReflectionConfigurationApplier<>(mtdName, values[i]);

        return resArr;
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] booleanParameters(String mtdName) {
        return parameters0(mtdName, false, new Boolean[] {true, false});
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] booleanParameters(boolean withNull, String mtdName) {
        return parameters0(mtdName, withNull, new Boolean[] {true, false});
    }

    /**
     * @param mtdName Method name.
     * @param values Values.
     * @return Array of configuration processors for given classes.
     */
    public static ConfigurationParameter[] objectParameters(String mtdName, Object... values) {
        return objectParameters(false, mtdName, values);
    }

    /**
     * @param mtdName Method name.
     * @param values Values.
     * @return Array of configuration processors for given classes.
     */
    public static ConfigurationParameter[] objectParameters(boolean withNull, String mtdName, Object... values) {
        return parameters0(mtdName, withNull, values);
    }

    /**
     * @param mtdName Method name.
     * @param val Value.
     * @return Configuration parameter.
     */
    public static <T> ConfigurationParameter<T> parameter(String mtdName, Object val) {
        return new ReflectionConfigurationApplier<>(mtdName, val);
    }

    /**
     * @return COmplex parameter.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T> complexParameter(ConfigurationParameter<T>... params) {
        return new ComplexParameter<T>(params);
    }

    /**
     * Reflection configuration applier.
     */
    @SuppressWarnings("serial")
    private static class ReflectionConfigurationApplier<T> implements ConfigurationParameter<T> {
        /** Classes of marameters cache. */
        private static final ConcurrentMap<T2<Class, String>, Class> paramClasses = new ConcurrentHashMap();

        /** */
        private final String mtdName;

        /** */
        private final Object val;

        /**
         * @param mtdName Method name.
         */
        ReflectionConfigurationApplier(String mtdName, Object val) {
            this.mtdName = mtdName;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            String mtdName0 = mtdName;

            if (mtdName0.startsWith("set") && mtdName0.length() > 3)
                mtdName0=mtdName0.substring(3, mtdName0.length());

            return mtdName0 + "=" + val;
        }

        /** {@inheritDoc} */
        @Override public T apply(T cfg) {
            if (val == null)
                return null;

            try {
                Class<?> paramCls = paramClasses.get(new T2<Class, String>(cfg.getClass(), mtdName));

                if (paramCls == null)
                    paramCls = val.getClass();
                else if (!paramCls.isInstance(val))
                    throw new IgniteException("Class parameter from cache does not match value argument class " +
                        "[paramCls=" + paramCls + ", val=" + val + "]");

                if (val.getClass().equals(Boolean.class))
                    paramCls = Boolean.TYPE;
                else if (val.getClass().equals(Byte.class))
                    paramCls = Byte.TYPE;
                else if (val.getClass().equals(Short.class))
                    paramCls = Short.TYPE;
                else if (val.getClass().equals(Character.class))
                    paramCls = Character.TYPE;
                else if (val.getClass().equals(Integer.class))
                    paramCls = Integer.TYPE;
                else if (val.getClass().equals(Long.class))
                    paramCls = Long.TYPE;
                else if (val.getClass().equals(Float.class))
                    paramCls = Float.TYPE;
                else if (val.getClass().equals(Double.class))
                    paramCls = Double.TYPE;

                Method mtd;

                Queue<Class> queue = new ArrayDeque<>();

                boolean failed = false;

                while (true) {
                    try {
                        mtd = cfg.getClass().getMethod(mtdName, paramCls);

                        if (failed)
                            paramClasses.put(new T2<Class, String>(cfg.getClass(), mtdName), paramCls);

                        break;
                    }
                    catch (NoSuchMethodException e) {
                        failed = true;

                        U.warn(null, "Method not found [cfgCls=" + cfg.getClass() + ", mtdName=" + mtdName
                            + ", paramCls=" + paramCls + "]");

                        Class<?>[] interfaces = paramCls.getInterfaces();

                        Class<?> superclass = paramCls.getSuperclass();

                        if (superclass != null)
                            queue.add(superclass);

                        if (!F.isEmpty(interfaces))
                            queue.addAll(Arrays.asList(interfaces));

                        if (queue.isEmpty())
                            throw new IgniteException("Method not found [cfgCls=" + cfg.getClass() + ", mtdName="
                                + mtdName + ", paramCls=" + val.getClass() + "]", e);

                        paramCls = queue.remove();
                    }
                }

                mtd.invoke(cfg, val);
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }

    /**
     *
     */
    private static class ComplexParameter<T> implements ConfigurationParameter<T> {
        /** */
        private final String name;

        /** */
        private ConfigurationParameter<T>[] params;

        /**
         * @param params Params
         */
        @SafeVarargs
        ComplexParameter(ConfigurationParameter<T>... params) {
            A.notEmpty(params, "params");

            this.params = params;

            if (params.length == 1)
                name = params[0].name();
            else {
                SB sb = new SB(params[0].name());

                for (int i = 1; i < params.length; i++)
                    sb.a('-').a(params[i]);

                name = sb.toString();
            }
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public T apply(T cfg) {
            for (ConfigurationParameter param : params)
                param.apply(cfg);

            return cfg;
        }
    }
}
