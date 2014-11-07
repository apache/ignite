/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify.hierarchy;

import org.gridgain.grid.compute.gridify.*;

/**
 * Target base class.
 */
public abstract class GridSuperTarget {
    /**
     * @return Always returns "GridSuperTarget.methodA()".
     */
    @Gridify(gridName = "GridifyHierarchyTest")
    protected String methodA() {
        System.out.println(">>> Called GridSuperTarget.methodA()");

        return "GridSuperTarget.methodA()";
    }

    /**
     * @return "GridSuperTarget.methodC()" string.
     */
    protected String methodB() {
        return methodC();
    }

    /**
     * @return "GridSuperTarget.methodC()" string.
     */
    @Gridify(gridName = "GridifyHierarchyTest")
    private String methodC() {
        System.out.println(">>> Called GridSuperTarget.methodC()");

        return "GridSuperTarget.methodC()";
    }

}
