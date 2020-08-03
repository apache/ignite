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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Mirror of .Net class IgniteConfiguration.cs
 */
public class PlatformDotNetConfiguration implements PlatformConfiguration {
    /** */
    private PlatformDotNetBinaryConfiguration binaryCfg;

    /** */
    private List<String> assemblies;

    /**
     * Default constructor.
     */
    public PlatformDotNetConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public PlatformDotNetConfiguration(PlatformDotNetConfiguration cfg) {
        if (cfg.getBinaryConfiguration() != null)
            binaryCfg = new PlatformDotNetBinaryConfiguration(cfg.getBinaryConfiguration());

        if (cfg.getAssemblies() != null)
            assemblies = new ArrayList<>(cfg.getAssemblies());
    }

    /**
     * @return Configuration.
     */
    public PlatformDotNetBinaryConfiguration getBinaryConfiguration() {
        return binaryCfg;
    }

    /**
     * @param binaryCfg Configuration.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetConfiguration setBinaryConfiguration(PlatformDotNetBinaryConfiguration binaryCfg) {
        this.binaryCfg = binaryCfg;

        return this;
    }

    /**
     * @return Assemblies.
     */
    public List<String> getAssemblies() {
        return assemblies;
    }

    /**
     *
     * @param assemblies Assemblies.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetConfiguration setAssemblies(List<String> assemblies) {
        this.assemblies = assemblies;

        return this;
    }

    /**
     * @return Configuration copy.
     */
    private PlatformDotNetConfiguration copy() {
        return new PlatformDotNetConfiguration(this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetConfiguration.class, this);
    }
}
