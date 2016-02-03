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
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;

/**
 * Enum variants.
 */
public class Variants {
    /**
     * Private constructor.
     */
    private Variants() {
        // No-op.
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] enumVariants(Class<?> enumCls, String mtdName) {
        Object[] enumConstants = enumCls.getEnumConstants();

        return enumVariables0(mtdName, enumConstants);
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] enumVariantsWithNull(Class<?> enumCls, String mtdName) {
        Object[] src = enumCls.getEnumConstants();

        Object[] enumConstants = new Object[src.length + 1];

        enumConstants[0] = null;

        System.arraycopy(src, 0, enumConstants, 1, enumConstants.length - 1);

        return enumVariables0(mtdName, enumConstants);
    }

    private static <T> ConfigurationParameter<T>[] enumVariables0(String mtdName, Object[] enumConstants) {
        ConfigurationParameter<T>[] arr = new ConfigurationParameter[enumConstants.length];

        for (int i = 0; i < arr.length; i++)
            arr[i] = new ReflectionConfigurationApplier<>(mtdName, enumConstants[i]);

        return arr;
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> ConfigurationParameter<T>[] booleanVariants(String mtdName) {
        ConfigurationParameter<T>[] arr = new ConfigurationParameter[2];

        arr[0] = new ReflectionConfigurationApplier<>(mtdName, true);
        arr[1] = new ReflectionConfigurationApplier<>(mtdName, false);

        return arr;
    }

    /**
     * Reflection configuration applier.
     */
    @SuppressWarnings("serial")
    private static class ReflectionConfigurationApplier<T> implements ConfigurationParameter<T> {
        /** */
        private final String mtdName;

        /** */
        private final Object param;

        /**
         * @param mtdName Method name.
         */
        ReflectionConfigurationApplier(String mtdName, Object param) {
            this.mtdName = mtdName;
            this.param = param;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            String mtdName0 = mtdName;

            if (mtdName0.startsWith("set") && mtdName0.length() > 3)
                mtdName0=mtdName0.substring(3, mtdName0.length());

            return mtdName0 + "=" + param;
        }

        /** {@inheritDoc} */
        @Override public T apply(T cfg) {
            if (param == null)
                return null;

            try {
                Class<?> paramCls = param.getClass();

                if (param.getClass().equals(Boolean.class))
                    paramCls = Boolean.TYPE;
                else if (param.getClass().equals(Integer.class))
                    paramCls = Integer.TYPE;

                Method mtd = cfg.getClass().getMethod(mtdName, paramCls);

                mtd.invoke(cfg, param);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}
