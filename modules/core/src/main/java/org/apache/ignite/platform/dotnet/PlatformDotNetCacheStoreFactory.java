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

package org.apache.ignite.platform.dotnet;

import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetCacheStore;

import javax.cache.configuration.Factory;
import java.util.Map;

/**
 * Wrapper for .NET cache store implementations.
 * <p>
 * This wrapper should be used if you have an implementation of
 * {@code GridGain.Cache.IGridCacheStore} interface in .NET and
 * would like to configure it a persistence storage for your cache.
 * To do tis you will need to configure the wrapper via
 * {@link org.apache.ignite.configuration.CacheConfiguration#setCacheStoreFactory(javax.cache.configuration.Factory)} property
 * and provide assembly name and class name of your .NET store
 * implementation (both properties are mandatory):
 * <pre name="code" class="xml">
 * &lt;bean class="org.apache.ignite.cache.CacheConfiguration"&gt;
 *     ...
 *     &lt;property name="cacheStoreFactory"&gt;
 *         &lt;bean class="org.gridgain.grid.interop.dotnet.InteropDotNetCacheStoreFactory"&gt;
 *             &lt;property name="assemblyName" value="MyAssembly"/&gt;
 *             &lt;property name="className" value="MyApp.MyCacheStore"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * If properly configured, this wrapper will instantiate an instance
 * of your cache store in .NET and delegate all calls to that instance.
 * To create an instance, assembly name and class name are passed to
 * <a target="_blank" href="http://msdn.microsoft.com/en-us/library/d133hta4.aspx">System.Activator.CreateInstance(String, String)</a>
 * method in .NET during node startup. Refer to its documentation for
 * details.
 */
public class PlatformDotNetCacheStoreFactory implements Factory<PlatformDotNetCacheStore> {
    /** */
    private static final long serialVersionUID = 0L;

    /** .Net assembly name. */
    private String assemblyName;

    /** .Net class name. */
    private String clsName;

    /** Properties. */
    private Map<String, ?> props;

    /** Instance. */
    private transient PlatformDotNetCacheStore instance;

    /**
     * Gets .NET assembly name.
     *
     * @return .NET assembly name.
     */
    public String getAssemblyName() {
        return assemblyName;
    }

    /**
     * Set .NET assembly name.
     *
     * @param assemblyName .NET assembly name.
     */
    public void setAssemblyName(String assemblyName) {
        this.assemblyName = assemblyName;
    }

    /**
     * Gets .NET class name.
     *
     * @return .NET class name.
     */
    public String getClassName() {
        return clsName;
    }

    /**
     * Sets .NET class name.
     *
     * @param clsName .NET class name.
     */
    public void setClassName(String clsName) {
        this.clsName = clsName;
    }

    /**
     * Get properties.
     *
     * @return Properties.
     */
    public Map<String, ?> getProperties() {
        return props;
    }

    /**
     * Set properties.
     *
     * @param props Properties.
     */
    public void setProperties(Map<String, ?> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public PlatformDotNetCacheStore create() {
        synchronized (this) {
            if (instance == null) {
                instance = new PlatformDotNetCacheStore();

                instance.setAssemblyName(assemblyName);
                instance.setClassName(clsName);
                instance.setProperties(props);
            }

            return instance;
        }
    }
}