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

package org.apache.ignite.internal.network.serialization.marshal;

import java.lang.reflect.Constructor;

/**
 * Instantiates using no-arg constructor. It only supports classes that have such constructors (but the constructors
 * may be private, they are still supported).
 */
class NoArgConstructorInstantiation implements Instantiation {
    /** {@inheritDoc} */
    @Override
    public boolean supports(Class<?> objectClass) {
        for (Constructor<?> constructor : objectClass.getDeclaredConstructors()) {
            if (constructor.getParameterCount() == 0) {
                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Object newInstance(Class<?> objectClass) throws InstantiationException {
        try {
            Constructor<?> constructor = objectClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new InstantiationException("Cannot instantiate " + objectClass, e);
        }
    }
}
