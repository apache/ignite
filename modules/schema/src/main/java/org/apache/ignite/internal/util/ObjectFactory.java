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

package org.apache.ignite.internal.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Object factory.
 */
public class ObjectFactory<T> implements Factory<T> {
    /** Class default constructor. */
    private final Constructor<T> cnstr;

    /**
     * Constructor.
     *
     * @param tClass Class.
     */
    public ObjectFactory(Class<T> tClass) {
        try {
            cnstr = tClass.getDeclaredConstructor();
            cnstr.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteInternalException("Class has no default constructor: class=" + tClass.getName(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public T create() throws IgniteInternalException {
        try {
            return cnstr.newInstance();
        }
        catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new IgniteInternalException("Failed to instantiate class: " + cnstr.getDeclaringClass().getName(), e);
        }
    }
}
