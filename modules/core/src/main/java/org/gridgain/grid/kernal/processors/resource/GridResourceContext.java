/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.lang.annotation.*;
import java.util.*;

/**
 * TODO
 */
public interface GridResourceContext {
    /**
     *
     * @return
     */
    public Collection<GridBiTuple<Class<? extends Annotation>, GridResourceInjector>> injectors();

    /**
     * Return original object if AOP used with proxy objects.
     *
     * @param target Target object.
     * @return Original object wrapped by proxy.
     * @throws org.gridgain.grid.GridException If unwrap failed.
     */
    public Object unwrapTarget(Object target) throws GridException;
}

