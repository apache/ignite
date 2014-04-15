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

        boolean testPassed = !libs.contains(NOT_FOUND_MESSAGE);

        String missedLibs = "";

        for (int i = 0; i < libs.size(); i++)
            if (NOT_FOUND_MESSAGE.equals(libs.get(i)))
                missedLibs += '\t' + CLASS_LIST[i] + '\n';

        String msg = testPassed ? "" : "Libraries with following classes is removed (if it is made on purpose " +
            "then remove this lib from " + GridLibraryConsistencyCheck.class.getSimpleName() + " class):\n" +
            missedLibs;

        assertTrue(msg, testPassed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCheckLibraryHasDifferentVersions() throws Exception {
        int libCnt = CLASS_LIST.length;

        List<String> libs1 = initLibs(libCnt);
        List<String> libs2 = initLibs(libCnt);

        libs1.add("lib-1.0.jar");
        libs2.add("lib-1.1.jar");

        assertEquals(1, check(libs1, libs2).size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCheckLibraryNotFound() throws Exception {
        int libCnt = CLASS_LIST.length;

        List<String> libs1 = initLibs(libCnt);
        List<String> libs2 = initLibs(libCnt);

        libs1.add("lib-1.0.jar");
        libs2.add(NOT_FOUND_MESSAGE);

        assertEquals(0, check(libs1, libs2).size());
    }

    /** Initializes libraries list with not_found messages. */
    private static List<String> initLibs(int libCnt) {
        List<String> libs = new ArrayList<>(libCnt);

        for (int i = 0; i < libCnt - 1; i++)
            libs.add(NOT_FOUND_MESSAGE);

        return libs;
    }
}
