// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 * Defines functional mapping interface. It assumes two types, set of values
 * of one type and a function that converts values of one type to another using
 * given set of values.
 * <p>
 * In GridGain it is primarily used as a mapping routine between closures and grid nodes
 * in {@link GridCompute#mapreduce(GridMapper, Collection, GridReducer)} method.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridMapper<T1, T2>extends GridLambdaAdapter {
    /**
     * Closure body.
     *
     * @param e Closure parameter.
     * @return Optional return value.
     */
    public abstract T2 apply(T1 e);

    /**
     * Collects values to be used by this mapper. This interface doesn't define how
     * many times this method can be called. This method, however, should be called
     * before {@link #apply(Object)} method.
     *
     * @param vals Values to use during mapping.
     */
    public abstract void collect(Collection<T2> vals);
}
