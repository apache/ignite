/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.util.*;

/**
 * Contains utility methods for Visor tasks and jobs.
 */
public class VisorTaskUtils {
    /**
     * Default substitute for {@code null} names.
     * */
    private static final String DFLT_EMPTY_NAME = "<default>";

    /**
     * @param name Grid-style nullable name.
     * @return Name with {@code null} replaced to &lt;default&gt;.
     */
    public static String escapeName(String name) {
        return name == null ? DFLT_EMPTY_NAME : name;
    }

    /**
     * Concat two arrays in one.
     * @param a first array.
     * @param b second array.
     * @return summary array.
     */
    public static int[] concat(int[] a, int[] b) {
        int[] c = Arrays.copyOf(a, a.length + b.length);

        System.arraycopy(b, 0, c, a.length, b.length);

        return c;
    }
}
