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

import java.util.Map;
import org.apache.ignite.internal.processors.platform.lifecycle.PlatformLifecycleBean;
import org.apache.ignite.lifecycle.LifecycleBean;

/**
 * Lifecycle bean implementation which can be used to configure .Net lifecycle beans in Java Spring configuration.
 */
public class PlatformDotNetLifecycleBean extends PlatformLifecycleBean implements LifecycleBean {
    /** Type name. */
    private String typName;

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
     * @param typName Type name.
     */
    public PlatformDotNetLifecycleBean(String typName) {
        this.typName = typName;
    }

    /**
     * Get type name.
     *
     * @return Type name.
     */
    public String getTypeName() {
        return typName;
    }

    /**
     * Set type name.
     *
     * @param typName Type name.
     */
    public void setTypeName(String typName) {
        this.typName = typName;
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
