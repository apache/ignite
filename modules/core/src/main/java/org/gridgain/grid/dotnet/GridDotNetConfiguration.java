/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dotnet;

import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Mirror of .Net class GridDotNetConfiguration.cs
 */
public class GridDotNetConfiguration implements GridPortableMarshalAware, Cloneable {
    /** */
    private GridDotNetPortableConfiguration portableCfg;

    /** */
    private List<String> assemblies;

    /**
     * @return Configuration.
     */
    public GridDotNetPortableConfiguration getPortableCfg() {
        return portableCfg;
    }

    /**
     * @param portableCfg Configuration.
     */
    public void setPortableCfg(GridDotNetPortableConfiguration portableCfg) {
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
    @Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
        GridPortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeObject(portableCfg);
        rawWriter.writeCollection(assemblies);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws GridPortableException {
        GridPortableRawReader rawReader = reader.rawReader();

        portableCfg = (GridDotNetPortableConfiguration)rawReader.readObject();
        assemblies = (List<String>)rawReader.<String>readCollection();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "CloneCallsConstructors"})
    @Override public GridDotNetConfiguration clone() {
        try {
            GridDotNetConfiguration res = (GridDotNetConfiguration)super.clone();

            if (portableCfg != null)
                res.portableCfg = portableCfg.clone();

            if (assemblies != null)
                res.assemblies = new ArrayList<>(assemblies);

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDotNetConfiguration.class, this);
    }
}
