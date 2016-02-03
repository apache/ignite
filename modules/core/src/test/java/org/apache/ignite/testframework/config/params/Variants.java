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
import org.apache.ignite.lang.IgniteClosure;

/**
 * Enum variants.
 */
public class Variants {
    /**
     * Private constructor.
     */
    private Variants() {

    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteClosure<T, Void>[] enumVariants(Class<?> enumClass, String mtdName) {
        Object[] enumConstants = enumClass.getEnumConstants();

        IgniteClosure<T, Void>[] arr = new IgniteClosure[enumConstants.length];

        for (int i = 0; i < arr.length; i++)
            arr[i] = new ReflectionConfigurationApplier<>(mtdName, enumConstants[i]);

        return arr;
    }

    /**
     * @return Array of configuration processors for given enum.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteClosure<T, Void>[] booleanVariants(String mtdName) {
        IgniteClosure<T, Void>[] arr = new IgniteClosure[2];

        arr[0] = new ReflectionConfigurationApplier<>(mtdName, true);
        arr[1] = new ReflectionConfigurationApplier<>(mtdName, false);

        return arr;
    }

    /**
     * Reflection configuration applier.
     */
    @SuppressWarnings("serial")
    private static class ReflectionConfigurationApplier<T> implements IgniteClosure<T, Void> {
        /** */
        private final String mtdName;

        /** */
        private final Object param;

        /**
         * @param mtdName Method name.
         */
        public ReflectionConfigurationApplier(String mtdName, Object param) {
            this.mtdName = mtdName;
            this.param = param;
        }

        /** {@inheritDoc} */
        @Override public Void apply(T cfg) {
            try {
                Method mtd = cfg.getClass().getMethod(mtdName, param.getClass());

                mtd.invoke(cfg, param);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}
