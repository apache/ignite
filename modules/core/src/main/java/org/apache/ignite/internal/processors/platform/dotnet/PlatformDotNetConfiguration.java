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

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.internal.processors.platform.PlatformConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.portable.api.PortableException;
import org.apache.ignite.internal.portable.api.PortableMarshalAware;
import org.apache.ignite.internal.portable.api.PortableRawReader;
import org.apache.ignite.internal.portable.api.PortableRawWriter;
import org.apache.ignite.internal.portable.api.PortableReader;
import org.apache.ignite.internal.portable.api.PortableWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Mirror of .Net class Configuration.cs
 */
public class PlatformDotNetConfiguration implements PlatformConfiguration, PortableMarshalAware {
    /** */
    private PlatformDotNetPortableConfiguration portableCfg;

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
        if (cfg.getPortableConfiguration() != null)
            portableCfg = new PlatformDotNetPortableConfiguration(cfg.getPortableConfiguration());

        if (cfg.getAssemblies() != null)
            assemblies = new ArrayList<>(cfg.getAssemblies());
    }

    /**
     * @return Configuration.
     */
    public PlatformDotNetPortableConfiguration getPortableConfiguration() {
        return portableCfg;
    }

    /**
     * @param portableCfg Configuration.
     */
    public void setPortableConfiguration(PlatformDotNetPortableConfiguration portableCfg) {
        this.portableCfg = portableCfg;
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
     */
    public void setAssemblies(List<String> assemblies) {
        this.assemblies = assemblies;
    }

    /**
     * @return Configuration copy.
     */
    @SuppressWarnings("UnusedDeclaration")
    private PlatformDotNetConfiguration copy() {
        return new PlatformDotNetConfiguration(this);
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeObject(portableCfg);
        rawWriter.writeCollection(assemblies);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader rawReader = reader.rawReader();

        portableCfg = rawReader.readObject();
        assemblies = (List<String>)rawReader.<String>readCollection();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetConfiguration.class, this);
    }
}