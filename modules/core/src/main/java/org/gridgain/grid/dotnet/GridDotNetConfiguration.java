/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dotnet;

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
     * @param cfg configuration to copy.
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

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws PortableException {
        GridPortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeObject(portableCfg);
        rawWriter.writeCollection(assemblies);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws PortableException {
        GridPortableRawReader rawReader = reader.rawReader();

        portableCfg = (GridDotNetPortableConfiguration)rawReader.readObject();
        assemblies = (List<String>)rawReader.<String>readCollection();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDotNetConfiguration.class, this);
    }
}
