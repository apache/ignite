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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Factory implementation that use reflection to create instance of given class.
 * <p>
 * There are 2 modes of factory: singleton and non-sigletton.
 * <p>
 * Class that should be created by {@link IgniteReflectionFactory} (component class) have to be
 * public java POJO with public setters for field
 * for which property injection will be used (see {@link #setProperties(Map)}).
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * The following configuration parameters are mandatory:
 * <ul>
 * <li>Component class - class to be created (see {@link #setComponentClass(Class)}.
 * It have to be public java POJO class with default constructor
 * and public setters to be used by properties injection (see {@link #setProperties(Map)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * </li>
 * <li>Singleton mode (see {@link #setSingleton(boolean)})</li>
 * <li>Properties map (see {@link #setProperties(Map)}</li>
 * <li>With method (see {@link #setWithMethod(Object, String, Serializable)}</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * Factory<CacheStoreSessionListener> factory =
 *     new IgniteReflectionFactory<CacheStoreSessionListener>(MyCacheStoreSessionListener.class);
 *
 * CacheConfiguration cc = new CacheConfiguration()
 *     .setCacheStoreSessionListenerFactories(factory);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration()
 *     .setCacheConfiguration(cc);
 *
 * // Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * TcpDiscoverySpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *     ...
 *     &lt;property name="cacheConfiguration"&gt;
 *         &lt;list&gt;
 *             &lt;bean class="org.apache.ignite.configuration.CacheConfiguration"&gt;
 *                 ...
 *                 &lt;property name="cacheStoreSessionListenerFactories"&gt;
 *                     &lt;list&gt;
 *                         &lt;bean class="org.apache.ignite.configuration.IgniteReflectionFactory"&gt;
 *                             &lt;property name="componentClass" value="custom.project.MyCacheStoreSessionListener"/&gt;
 *                         &lt;/bean&gt;
 *                     &lt;/list&gt;
 *                 &lt;/property&gt;
 *                 ...
 *             &lt;/bean&gt;
 *         &lt;/list&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see Factory
 */
public class IgniteReflectionFactory<T> implements Factory<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Singletom mode */
    private volatile boolean singleton;

    /** Component class */
    private volatile Class<? extends T> cls;

    /** Properties */
    private volatile Map<String, Serializable> props;

    /** */
    private transient T instance;

    /**
     * Default constructor.
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
     * Gets a map of properties. Map contains entries of component class field name
     * to value of the filed which will be used as initial value.
     *
     * @return Properties.
     */
    public Map<String, Serializable> getProperties() {
        return props;
    }

    /**
     * Sets a map of properties. Map contains entries of component class field name
     * to a value of the filed which will be used as initial value.
     *
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