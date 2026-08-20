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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This class allows defining system data region configuration with various parameters for Apache Ignite
 * page memory (see {@link DataStorageConfiguration}.
 * This class is similiar to {@link DataRegionConfiguration}, but with restricted set of properties.
 */
public class SystemDataRegionConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Default initial size in bytes of a memory chunk for the system cache (40 MB). */
    public static final long DFLT_SYS_REG_INIT_SIZE = 40L * 1024 * 1024;

    /** Default max size in bytes of a memory chunk for the system cache (100 MB). */
    public static final long DFLT_SYS_REG_MAX_SIZE = 100L * 1024 * 1024;

    /** Initial size in bytes of a memory chunk reserved for system cache. */
    private long initSize = DFLT_SYS_REG_INIT_SIZE;

    /** Maximum size in bytes of a memory chunk reserved for system cache. */
    private long maxSize = DFLT_SYS_REG_MAX_SIZE;

    /**
     * Initial size of a data region reserved for system cache.
     *
     * @return Size in bytes.
     */
    public long getInitialSize() {
        return initSize;
    }

    /**
     * Sets initial size of a data region reserved for system cache.
     *
     * Default value is {@link #DFLT_SYS_REG_INIT_SIZE}
     *
     * @param initSize Size in bytes.
     * @return {@code this} for chaining.
     */
    public SystemDataRegionConfiguration setInitialSize(long initSize) {
        A.ensure(initSize > 0, "System region initial size should be greater than zero.");

        this.initSize = initSize;

        return this;
    }

    /**
     * Maximum data region size in bytes reserved for system cache.
     *
     * @return Size in bytes.
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Sets maximum data region size in bytes reserved for system cache. The total size should not be less than 10 MB
     * due to internal data structures overhead.
     *
     * Default value is {@link #DFLT_SYS_REG_MAX_SIZE}.
     *
     * @param maxSize Maximum size in bytes for system cache data region.
     * @return {@code this} for chaining.
     */
    public SystemDataRegionConfiguration setMaxSize(long maxSize) {
        A.ensure(maxSize > 0, "System region max size should be greater than zero.");

        this.maxSize = maxSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SystemDataRegionConfiguration.class, this);
    }
}
