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

package org.apache.ignite.dotnet;

import org.apache.ignite.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Mirror of .Net class GridDotNetConfiguration.cs
 */
public class GridDotNetConfiguration implements PortableMarshalAware {
    /** */
    private GridDotNetPortableConfiguration portableCfg;

    /** */
    private List<String> assemblies;

    /**
     * Default constructor.
     */
    public GridDotNetConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridDotNetConfiguration(GridDotNetConfiguration cfg) {
        if (cfg.getPortableConfiguration() != null)
            portableCfg = new GridDotNetPortableConfiguration(cfg.getPortableConfiguration());

        if (cfg.getAssemblies() != null)
            assemblies = new ArrayList<>(cfg.getAssemblies());
    }

    /**
     * @return Configuration.
     */
    public GridDotNetPortableConfiguration getPortableConfiguration() {
        return portableCfg;
    }

    /**
     * @param portableCfg Configuration.
     */
    public void setPortableConfiguration(GridDotNetPortableConfiguration portableCfg) {
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
     * Copy configuration.
     *
     * @return Copied configuration.
     */
    public GridDotNetConfiguration copy() {
        return new GridDotNetConfiguration(this);
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
        return S.toString(GridDotNetConfiguration.class, this);
    }
}
