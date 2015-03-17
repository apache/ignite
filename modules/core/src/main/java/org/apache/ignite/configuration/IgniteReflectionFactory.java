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

import org.apache.ignite.internal.util.typedef.*;

import javax.cache.*;
import javax.cache.configuration.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Convenience class for reflection-based object creation.
 */
public class IgniteReflectionFactory<T> implements Factory<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private volatile boolean singleton;

    /** */
    private volatile Class<? extends T> cls;

    /** */
    private volatile Map<String, Serializable> props;

    /** */
    private transient T instance;

    /**
     *
     */
    public IgniteReflectionFactory() {
        // No-op.
    }

    /**
     * @param cls Component class.
     * @param singleton Singleton flag.
     */
    public IgniteReflectionFactory(Class<? extends T> cls, boolean singleton) {
        this.cls = cls;
        this.singleton = singleton;
    }

    /**
     * Creates non-singleton component factory.
     *
     * @param cls Component class.
     */
    public IgniteReflectionFactory(Class<? extends T> cls) {
        this(cls, false);
    }

    /**
     * @return {@code True} if factory is singleton.
     */
    public boolean isSingleton() {
        return singleton;
    }

    /**
     * @param singleton Singleton flag.
     */
    public void setSingleton(boolean singleton) {
        this.singleton = singleton;
    }

    /**
     * @return Component class to create.
     */
    public Class<? extends T> getComponentClass() {
        return cls;
    }

    /**
     * @param cls Component class to create.
     */
    public void setComponentClass(Class<T> cls) {
        this.cls = cls;
    }

    /**
     * @return Properties.
     */
    public Map<String, Serializable> getProperties() {
        return props;
    }

    /**
     * @param props Properties.
     */
    public void setProperties(Map<String, Serializable> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public T create() {
        synchronized (this) {
            if (singleton) {
                if (instance == null)
                    instance = createInstance();

                return instance;
            }

            return createInstance();
        }
    }

    /**
     * @return Initialized instance.
     */
    private T createInstance() {
        if (cls == null)
            throw new IllegalStateException("Failed to create object (object type is not set).");

        try {
            T obj = cls.newInstance();

            injectProperties(obj);

            return obj;
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new CacheException("Failed to instantiate factory object: " + cls.getName(), e);
        }
    }

    /**
     * @param obj Object to initialize with properties.
     */
    private void injectProperties(T obj) {
        if (!F.isEmpty(props)) {
            for (Map.Entry<String, Serializable> entry : props.entrySet()) {
                String fieldName = entry.getKey().trim();

                if (fieldName.isEmpty())
                    continue;

                Serializable val = entry.getValue();

                setWithMethod(obj, fieldName, val);
            }
        }
    }

    /**
     * @param obj Object to initialize with properties.
     * @param fieldName Field name.
     * @param val Value to set.
     * @return {@code True} if property was set.
     */
    private boolean setWithMethod(T obj, String fieldName, Serializable val) {
        StringBuilder sb = new StringBuilder("set");

        sb.append(fieldName);

        sb.setCharAt(3, Character.toUpperCase(sb.charAt(3)));

        Class paramCls = val.getClass();

        while (paramCls != null) {
            try {
                Method mtd = obj.getClass().getMethod(sb.toString(), paramCls);

                mtd.invoke(obj, val);

                return true;
            }
            catch (InvocationTargetException e) {
                throw new CacheException(e.getCause());
            }
            catch (NoSuchMethodException | IllegalAccessException ignore) {
                // try next class.
                paramCls = paramCls.getSuperclass();
            }
        }

        // Try interfaces.
        for (Class<?> itf : val.getClass().getInterfaces()) {
            try {
                Method mtd = obj.getClass().getMethod(sb.toString(), itf);

                mtd.invoke(obj, val);

                return true;
            }
            catch (InvocationTargetException e) {
                throw new CacheException(e.getCause());
            }
            catch (NoSuchMethodException | IllegalAccessException ignore) {
                // try next interface.
            }
        }

        return false;
    }
}
