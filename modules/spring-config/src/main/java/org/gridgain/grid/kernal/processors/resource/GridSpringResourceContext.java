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
import org.gridgain.grid.resources.*;
import org.springframework.aop.framework.*;
import org.springframework.context.*;

import java.lang.annotation.*;
import java.util.*;

/**
 * TODO
 */
public class GridSpringResourceContext implements GridResourceContext {
    /** */
    private final ApplicationContext springCtx;

    /** */
    private final Collection<GridBiTuple<Class<? extends Annotation>, GridResourceInjector>> injectors;

    /**
     * @param springCtx Spring application context.
     */
    public GridSpringResourceContext(ApplicationContext springCtx) {
        this.springCtx = springCtx;

        injectors = new ArrayList<>(2);

        injectors.add(new GridBiTuple<Class<? extends Annotation>, GridResourceInjector>(
            GridSpringApplicationContextResource.class, new GridResourceBasicInjector<>(springCtx)));

        injectors.add(new GridBiTuple<Class<? extends Annotation>, GridResourceInjector>(
            GridSpringResource.class, new GridResourceSpringBeanInjector(springCtx)));
    }

    @Override public Collection<GridBiTuple<Class<? extends Annotation>, GridResourceInjector>> injectors() {
        return injectors;
    }

    @Override public Object unwrapTarget(Object target) throws GridException {
        if (target instanceof Advised) {
            try {
                return ((Advised)target).getTargetSource().getTarget();
            }
            catch (Exception e) {
                throw new GridException("Failed to unwrap Spring proxy target [cls=" + target.getClass().getName() +
                    ", target=" + target + ']', e);
            }
        }

        return target;
    }
}
