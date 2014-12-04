/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute.gridify;

import org.gridgain.grid.*;
import java.lang.annotation.*;

/**
 * This interface defines an interceptor apply for {@link Gridify} annotation. Interceptor
 * gets called in advise code to decide whether or not to grid-enable this method.
 * <p>
 * Interceptors can be used to provide fine-grain control on {@link Gridify} annotation
 * behavior. For example, an interceptor can be implemented to grid enable the method
 * only if CPU on the local node has been above 80% of utilization for the last 5 minutes.
 */
public interface GridifyInterceptor {
    /**
     * This method is called before actual grid-enabling happens.
     *
     * @param gridify Gridify annotation instance that caused the grid-enabling.
     * @param arg Gridify argument.
     * @return {@code True} if method should be grid-enabled, {@code false} otherwise.
     * @throws GridException Thrown in case of any errors.
     */
    public boolean isGridify(Annotation gridify, GridifyArgument arg) throws GridException;
}
