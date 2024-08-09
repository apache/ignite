package org.apache.ignite.console.agent.utils;

import java.util.List;

/**
 * @author liuzh
 * @since 2015-03-19
 */
public class StringUtils {

    public static String getNotNull(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }

    public static boolean isEmpty(String value) {
        if (value == null || value.length() == 0) {
            return true;
        }
        return false;
    }

    public static boolean isNotEmpty(String value) {
        return !isEmpty(value);
    }

    public static boolean isEmpty(List<?> value) {
        if (value == null || value.size() == 0) {
            return true;
        }
        return false;
    }

    public static boolean isNotEmpty(List<?> value) {
        return !isEmpty(value);
    }
}
