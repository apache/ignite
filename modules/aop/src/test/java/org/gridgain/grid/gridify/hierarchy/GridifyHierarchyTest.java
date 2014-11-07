/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify.hierarchy;

import org.gridgain.testframework.junits.common.*;

/**
 * Gridify hierarchy test.
 */
public class GridifyHierarchyTest extends GridCommonAbstractTest {
    /** */
    public GridifyHierarchyTest() {
        super(true);
    }

    /** */
    public void noneTestGridifyHierarchyProtected() {
        GridTarget target = new GridTarget();

        target.methodA();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridifyHierarchyPrivate() throws Exception {
        GridTarget target = new GridTarget();

        target.methodB();
    }

   /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "GridifyHierarchyTest";
    }
}
