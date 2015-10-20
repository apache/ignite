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

import org.apache.ignite.internal.processors.platform.lifecycle.PlatformLifecycleBean;
import org.apache.ignite.lifecycle.LifecycleBean;

import java.util.Map;

/**
 * Lifecycle bean implementation which can be used to configure .Net lifecycle beans in Java Spring configuration.
 */
public class PlatformDotNetLifecycleBean extends PlatformLifecycleBean implements LifecycleBean {
    /** Assembly name. */
    private String assemblyName;

    /** Class name. */
    private String clsName;

    /** Properties. */
    private Map<String, ?> props;

    /**
     * Constructor.
     */
    public PlatformDotNetLifecycleBean() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param assemblyName Assembly name.
     * @param clsName Class name.
     */
    public PlatformDotNetLifecycleBean(String assemblyName, String clsName) {
        this.assemblyName = assemblyName;
        this.clsName = clsName;
    }

    /**
     * Get assembly name.
     *
     * @return Assembly name.
     */
    public String getAssemblyName() {
        return assemblyName;
    }

    /**
     * Set assembly name.
     *
     * @param assemblyName Assembly name.
     */
    public void setAssemblyName(String assemblyName) {
        this.assemblyName = assemblyName;
    }

    /**
     * Get class name.
     *
     * @return Class name.
     */
    public String getClassName() {
        return clsName;
    }

    /**
     * Set class name.
     *
     * @param clsName Class name.
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
}