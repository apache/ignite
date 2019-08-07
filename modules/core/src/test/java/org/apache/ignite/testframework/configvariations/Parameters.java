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

package org.apache.ignite.testframework.configvariations;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Parameters utils.
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
    public static <T> ConfigParameter<T>[] enumParameters(String mtdName, Class<?> enumCls) {
        return enumParameters(false, mtdName, enumCls);
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    public static <T> ConfigParameter<T>[] enumParameters(boolean withNull, String mtdName, Class<?> enumCls) {
        return parameters0(mtdName, withNull, enumCls.getEnumConstants());
    }

    /**
     * @param mtdName Method name.
     * @param values Values.
     * @return Array of configuration paramethers.
     */
    @SuppressWarnings("unchecked")
    private static <T> ConfigParameter<T>[] parameters0(String mtdName, boolean withNull, Object[] values) {
        for (Object val : values) {
            if (!isPrimitiveOrEnum(val) && !(val instanceof Factory))
                throw new IllegalArgumentException("Value have to be primite, enum or factory: " + val);
        }

        if (withNull) {
            Object[] valuesWithNull = new Object[values.length + 1];

            valuesWithNull[0] = null;

            System.arraycopy(values, 0, valuesWithNull, 1, valuesWithNull.length - 1);

            values = valuesWithNull;
        }

        assert values != null && values.length > 0 : "MtdName:" + mtdName;

        ConfigParameter<T>[] resArr = new ConfigParameter[values.length];

        for (int i = 0; i < resArr.length; i++)
            resArr[i] = new ReflectionParameter<>(mtdName, values[i]);

        return resArr;
    }

    /**
     * @param val Value.
     * @return Primitive or enum or not.
     */
    private static boolean isPrimitiveOrEnum(Object val) {
        return val.getClass().isPrimitive()
            || val.getClass().equals(Boolean.class)
            || val.getClass().equals(Byte.class)
            || val.getClass().equals(Short.class)
            || val.getClass().equals(Character.class)
            || val.getClass().equals(Integer.class)
            || val.getClass().equals(Long.class)
            || val.getClass().equals(Float.class)
            || val.getClass().equals(Double.class)
            || val.getClass().isEnum();
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    public static <T> ConfigParameter<T>[] booleanParameters(String mtdName) {
        return parameters0(mtdName, false, new Boolean[] {true, false});
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    public static <T> ConfigParameter<T>[] booleanParameters(boolean withNull, String mtdName) {
        return parameters0(mtdName, withNull, new Boolean[] {true, false});
    }

    /**
     * @param mtdName Method name.
     * @param values Values.
     * @return Array of configuration processors for given classes.
     */
    public static ConfigParameter[] objectParameters(String mtdName, Object... values) {
        return objectParameters(false, mtdName, values);
    }

    /**
     * @param mtdName Method name.
     * @param values Values.
     * @return Array of configuration processors for given classes.
     */
    public static ConfigParameter[] objectParameters(boolean withNull, String mtdName, Object... values) {
        return parameters0(mtdName, withNull, values);
    }

    /**
     * @param mtdName Method name.
     * @param val Value.
     * @return Configuration parameter.
     */
    public static <T> ConfigParameter<T> parameter(String mtdName, Object val) {
        return new ReflectionParameter<>(mtdName, val);
    }

    /**
     * @return Complex parameter.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigParameter<T> complexParameter(ConfigParameter<T>... params) {
        return new ComplexParameter<T>(params);
    }

    /**
     * @param cls Class.
     * @return Factory that uses default constructor to initiate object by given class.
     */
    public static <T> Factory<T> factory(Class<?> cls) {
        return new ReflectionFactory<>(cls);
    }

    /**
     * Reflection configuration applier.
     */
    private static class ReflectionParameter<T> implements ConfigParameter<T> {
        /** Classes of marameters cache. */
        private static final ConcurrentMap<T2<Class, String>, Class> paramClassesCache = new ConcurrentHashMap();

        /** */
        private final String mtdName;

        /** Primitive, enum or factory. */
        private final Object val;

        /**
         * @param mtdName Method name.
         */
        ReflectionParameter(String mtdName, @Nullable Object val) {
            if (val != null && !isPrimitiveOrEnum(val) && !(val instanceof Factory))
                throw new IllegalArgumentException("Value have to be primite, enum or factory: " + val);

            this.mtdName = mtdName;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            String mtdName0 = mtdName;

            if (mtdName0.startsWith("set") && mtdName0.length() > 3)
                mtdName0 = mtdName0.substring(3, mtdName0.length());

            String val0;

            if (val == null)
                val0 = "null";
            else if (val instanceof Factory)
                val0 = ((Factory)val).create().toString();
            else
                val0 = val.toString();

            return mtdName0 + "=" + val0;
        }

        /** {@inheritDoc} */
        @Override public T apply(T cfg) {
            if (val == null)
                return null;

            try {
                Object val0 = val;

                if (!isPrimitiveOrEnum(val))
                    val0 = ((Factory)val0).create();

                Class<?> paramCls = paramClassesCache.get(new T2<Class, String>(cfg.getClass(), mtdName));

                if (paramCls == null)
                    paramCls = val0.getClass();
                else if (!paramCls.isInstance(val0))
                    throw new IgniteException("Class parameter from cache does not match value argument class " +
                        "[paramCls=" + paramCls + ", val=" + val0 + "]");

                if (val0.getClass().equals(Boolean.class))
                    paramCls = Boolean.TYPE;
                else if (val0.getClass().equals(Byte.class))
                    paramCls = Byte.TYPE;
                else if (val0.getClass().equals(Short.class))
                    paramCls = Short.TYPE;
                else if (val0.getClass().equals(Character.class))
                    paramCls = Character.TYPE;
                else if (val0.getClass().equals(Integer.class))
                    paramCls = Integer.TYPE;
                else if (val0.getClass().equals(Long.class))
                    paramCls = Long.TYPE;
                else if (val0.getClass().equals(Float.class))
                    paramCls = Float.TYPE;
                else if (val0.getClass().equals(Double.class))
                    paramCls = Double.TYPE;

                Method mtd;

                Queue<Class> queue = new ArrayDeque<>();

                boolean failed = false;

                while (true) {
                    try {
                        mtd = cfg.getClass().getMethod(mtdName, paramCls);

                        if (failed)
                            paramClassesCache.put(new T2<Class, String>(cfg.getClass(), mtdName), paramCls);

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
                                + mtdName + ", paramCls=" + val0.getClass() + "]", e);

                        paramCls = queue.remove();
                    }
                }

                mtd.invoke(cfg, val0);
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
    private static class ReflectionFactory<T> implements Factory<T> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private Class<?> cls;

        /**
         * @param cls Class.
         */
        ReflectionFactory(Class<?> cls) {
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public T create() {
            try {
                Constructor<?> constructor = cls.getConstructor();

                return (T)constructor.newInstance();
            }
            catch (NoSuchMethodException | InstantiationException | InvocationTargetException |
                IllegalAccessException e) {
                throw new IgniteException("Failed to create object using default constructor: " + cls, e);
            }
        }
    }

    /**
     *
     */
    private static class ComplexParameter<T> implements ConfigParameter<T> {
        /** */
        private final String name;

        /** */
        private ConfigParameter<T>[] params;

        /**
         * @param params Params
         */
        @SafeVarargs 
        ComplexParameter(ConfigParameter<T>... params) {
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
            for (ConfigParameter param : params)
                param.apply(cfg);

            return cfg;
        }
    }
}
