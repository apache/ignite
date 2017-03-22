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

package org.apache.ignite.internal.processors.query.property;

import org.apache.ignite.IgniteCheckedException;

import java.lang.reflect.Method;

/**
 * Getter and setter methods based accessor.
 */
public class QueryMethodsAccessor implements QueryPropertyAccessor {
    /** */
    private final Method getter;

    /** */
    private final Method setter;

    /** */
    private final String propName;

    /**
     * @param getter Getter method.
     * @param setter Setter method.
     * @param propName Property name.
     */
    public QueryMethodsAccessor(Method getter, Method setter, String propName) {
        getter.setAccessible(true);
        setter.setAccessible(true);

        this.getter = getter;
        this.setter = setter;
        this.propName = propName;
    }

    /** {@inheritDoc} */
    @Override public Object getValue(Object obj) throws IgniteCheckedException {
        try {
            return getter.invoke(obj);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to invoke getter method " +
                "[type=" + getType() + ", property=" + propName + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setValue(Object obj, Object newVal) throws IgniteCheckedException {
        try {
            setter.invoke(obj, newVal);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to invoke setter method " +
                "[type=" + getType() + ", property=" + propName + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public String getPropertyName() {
        return propName;
    }

    /** {@inheritDoc} */
    @Override public Class<?> getType() {
        return getter.getReturnType();
    }
}
