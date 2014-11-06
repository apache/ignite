/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Simple adapter for {@link GridDeploymentResource} interface.
 */
public class GridDeploymentResourceAdapter implements GridDeploymentResource {
    /** */
    private final String name;

    /** */
    private final Class<?> rsrcCls;

    /** */
    private final ClassLoader clsLdr;

    /**
     * Creates resource.
     *
     * @param name Resource name.
     * @param rsrcCls Class.
     * @param clsLdr Class loader.
     */
    public GridDeploymentResourceAdapter(String name, Class<?> rsrcCls, ClassLoader clsLdr) {
        assert name != null;
        assert rsrcCls != null;
        assert clsLdr != null;

        this.name = name;
        this.rsrcCls = rsrcCls;
        this.clsLdr = clsLdr;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Class<?> getResourceClass() {
        return rsrcCls;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        GridDeploymentResourceAdapter rsrc = (GridDeploymentResourceAdapter)obj;

        return clsLdr.equals(rsrc.clsLdr) == true && name.equals(rsrc.name) == true &&
            rsrcCls.equals(rsrc.rsrcCls) == true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = name.hashCode();

        res = 31 * res + rsrcCls.hashCode();
        res = 31 * res + clsLdr.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentResourceAdapter.class, this);
    }
}
