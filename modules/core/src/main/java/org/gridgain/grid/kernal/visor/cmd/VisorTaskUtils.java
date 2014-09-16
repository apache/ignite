/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static java.lang.System.*;

/**
 * Contains utility methods for Visor tasks and jobs.
 */
public class VisorTaskUtils {
    /** Default substitute for {@code null} names. */
    private static final String DFLT_EMPTY_NAME = "<default>";

    /**
     * @param name Grid-style nullable name.
     * @return Name with {@code null} replaced to &lt;default&gt;.
     */
    public static String escapeName(@Nullable String name) {
        return name == null ? DFLT_EMPTY_NAME : name;
    }

    /**
     * @param a First name.
     * @param b Second name.
     * @return {@code true} if both names equals.
     */
    public static boolean safeEquals(@Nullable String a, @Nullable String b) {
        return (a != null && b != null) ? a.equals(b) : (a == null && b == null);
    }

    /**
     * Concat arrays in one.
     *
     * @param arrays Arrays.
     * @return Summary array.
     */
    public static int[] concat(int[] ... arrays) {
        assert arrays != null;
        assert arrays.length > 1;

        int length = 0;

        for (int[] a : arrays)
            length += a.length;

        int[] r = Arrays.copyOf(arrays[0], length);

        for (int i = 1, shift = 0; i < arrays.length; i++) {
            shift += arrays[i - 1].length;
            System.arraycopy(arrays[i], 0, r, shift, arrays[i].length);
        }

        return r;
    }

    /**
     * Returns compact class host.
     *
     * @param obj Object to compact.
     * @return String.
     */
    @Nullable public static Object compactObject(Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof Enum)
            return obj.toString();

        if (obj instanceof String || obj instanceof Boolean || obj instanceof Number)
            return obj;

        if (obj instanceof Collection) {
            Collection col = (Collection)obj;

            Object[] res = new Object[col.size()];

            int i = 0;

            for (Object elm : col) {
                res[i++] = compactObject(elm);
            }

            return res;
        }

        if (obj.getClass().isArray()) {
            Class<?> arrType = obj.getClass().getComponentType();

            if (arrType.isPrimitive()) {
                if (obj instanceof boolean[])
                    return Arrays.toString((boolean[])obj);
                if (obj instanceof byte[])
                    return Arrays.toString((byte[])obj);
                if (obj instanceof short[])
                    return Arrays.toString((short[])obj);
                if (obj instanceof int[])
                    return Arrays.toString((int[])obj);
                if (obj instanceof long[])
                    return Arrays.toString((long[])obj);
                if (obj instanceof float[])
                    return Arrays.toString((float[])obj);
                if (obj instanceof double[])
                    return Arrays.toString((double[])obj);
            }

            Object[] arr = (Object[])obj;

            int iMax = arr.length - 1;

            StringBuilder sb = new StringBuilder("[");

            for (int i = 0; i <= iMax; i++) {
                sb.append(compactObject(arr[i]));

                if (i != iMax)
                    sb.append(", ");
            }

            sb.append("]");

            return sb.toString();
        }

        return U.compact(obj.getClass().getName());
    }

    @Nullable public static String compactClass(Object obj) {
        if (obj == null)
            return null;

        return U.compact(obj.getClass().getName());
    }

    /**
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    @Nullable public static String compactArray(Object[] arr) {
        if (arr == null || arr.length == 0)
            return null;

        String sep = ", ";

        StringBuilder sb = new StringBuilder();

        for (Object s: arr)
            sb.append(s).append(sep);

        if (sb.length() > 0)
            sb.setLength(sb.length() - sep.length());

        return U.compact(sb.toString());
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property name.
     * @param dflt Function that returns {@code Integer}.
     * @return {@code Integer} value
     */
    public static Integer intValue(String propName, Integer dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Integer.getInteger(sysProp) : dflt;
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property host.
     * @param dflt Function that returns {@code Boolean}.
     * @return {@code Boolean} value
     */
    public static boolean boolValue(String propName, boolean dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Boolean.getBoolean(sysProp) : dflt;
    }
}
