/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import junit.framework.*;

import java.util.*;

import static org.gridgain.grid.util.GridLibraryConsistencyCheck.*;

/**
 * Library consistency check tests.
 */
public class GridLibraryConsistencyCheckSelfTest extends TestCase {
    /**
     *
     */
    public void testAllLibrariesLoaded() {
        ArrayList<String> libs = libraries();

        int idx = libs.indexOf(NOT_FOUND_MESSAGE);

        boolean testPassed = idx == -1;

        String msg = testPassed ? "" : "Library with class " + CLASS_LIST[idx] +
            " is removed, if it is made on purpose then remove this lib from " +
            GridLibraryConsistencyCheck.class.getSimpleName() + " class.";

        assertTrue(msg, testPassed);
    }
}
