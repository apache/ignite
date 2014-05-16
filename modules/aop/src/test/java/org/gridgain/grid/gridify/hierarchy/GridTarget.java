/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify.hierarchy;

/**
 * Target.
 */
public class GridTarget extends GridSuperTarget {
    /** {@inheritDoc} */
    @Override protected String methodA() {
        System.out.println(">>> Called GridTarget.methodA()");

        String res = super.methodA();

        assert "GridSuperTarget.methodA()".equals(res) == true :
            "Unexpected GridSuperTarget.methodA() apply result [res=" + res + ']';

        return "GridTarget.MethodA()";
    }

    /** {@inheritDoc} */
    @Override protected String methodB() {
        String res = super.methodB();

        assert "GridSuperTarget.methodC()".equals(res) == true:
            "Unexpected GridSuperTarget.methodB() apply result [res=" + res + ']';

        return res;
    }
}
