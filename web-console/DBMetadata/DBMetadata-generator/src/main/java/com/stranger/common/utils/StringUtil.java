package com.stranger.common.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.AntPathMatcher;

public class StringUtil extends StringUtils {
    private static final String NULLSTR = "";

    private static final char SEPARATOR = '_';

    public static <T> T nvl(T value, T defaultValue) {
        return (value != null) ? value : defaultValue;
    }

    public static boolean isEmpty(Collection<?> coll) {
        return (isNull(coll) || coll.isEmpty());
    }

    public static boolean isNotEmpty(Collection<?> coll) {
        return !isEmpty(coll);
    }

    public static boolean isEmpty(Object[] objects) {
        return (isNull(objects) || objects.length == 0);
    }

    public static boolean isNotEmpty(Object[] objects) {
        return !isEmpty(objects);
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return (isNull(map) || map.isEmpty());
    }

    public static boolean isNotEmpty(Map<?, ?> map) {
        return !isEmpty(map);
    }

    public static boolean isEmpty(String str) {
        return (isNull(str) || "".equals(str.trim()));
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isNull(Object object) {
        return (object == null);
    }

    public static boolean isNotNull(Object object) {
        return !isNull(object);
    }

    public static boolean isArray(Object object) {
        return (isNotNull(object) && object.getClass().isArray());
    }

    public static String trim(String str) {
        return (str == null) ? "" : str.trim();
    }

    public static String substring(String str, int start) {
        if (str == null)
            return "";
        if (start < 0)
            start = str.length() + start;
        if (start < 0)
            start = 0;
        if (start > str.length())
            return "";
        return str.substring(start);
    }

    public static String substring(String str, int start, int end) {
        if (str == null)
            return "";
        if (end < 0)
            end = str.length() + end;
        if (start < 0)
            start = str.length() + start;
        if (end > str.length())
            end = str.length();
        if (start > end)
            return "";
        if (start < 0)
            start = 0;
        if (end < 0)
            end = 0;
        return str.substring(start, end);
    }

    public static String format(String template, Object... params) {
        if (isEmpty(params) || isEmpty(template))
            return template;
        return StrFormatter.format(template, params);
    }

    public static final Set<String> str2Set(String str, String sep) {
        return new HashSet<>(str2List(str, sep, true, false));
    }

    public static final List<String> str2List(String str, String sep, boolean filterBlank, boolean trim) {
        List<String> list = new ArrayList<>();
        if (isEmpty(str))
            return list;
        if (filterBlank && isBlank(str))
            return list;
        String[] split = str.split(sep);
        for (String string : split) {
            if (!filterBlank || !isBlank(string)) {
                if (trim)
                    string = string.trim();
                list.add(string);
            }
        }
        return list;
    }

    public static boolean containsAny(Collection<String> collection, String... array) {
        if (isEmpty(collection) || isEmpty((Object[])array))
            return false;
        for (String str : array) {
            if (collection.contains(str))
                return true;
        }
        return false;
    }

    public static boolean containsAnyIgnoreCase(CharSequence cs, CharSequence... searchCharSequences) {
        if (isEmpty(cs) || isEmpty((Object[])searchCharSequences))
            return false;
        for (CharSequence testStr : searchCharSequences) {
            if (containsIgnoreCase(cs, testStr))
                return true;
        }
        return false;
    }

    public static String toUnderScoreCase(String str) {
        if (str == null)
            return null;
        StringBuilder sb = new StringBuilder();
        boolean preCharIsUpperCase = true;
        boolean curreCharIsUpperCase = true;
        boolean nexteCharIsUpperCase = true;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (i > 0) {
                preCharIsUpperCase = Character.isUpperCase(str.charAt(i - 1));
            } else {
                preCharIsUpperCase = false;
            }
            curreCharIsUpperCase = Character.isUpperCase(c);
            if (i < str.length() - 1)
                nexteCharIsUpperCase = Character.isUpperCase(str.charAt(i + 1));
            if (preCharIsUpperCase && curreCharIsUpperCase && !nexteCharIsUpperCase) {
                sb.append('_');
            } else if (i != 0 && !preCharIsUpperCase && curreCharIsUpperCase) {
                sb.append('_');
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    public static boolean inStringIgnoreCase(String str, String... strs) {
        if (str != null && strs != null)
            for (String s : strs) {
                if (str.equalsIgnoreCase(trim(s)))
                    return true;
            }
        return false;
    }

    public static String convertToCamelCase(String name) {
        StringBuilder result = new StringBuilder();
        // 快速检查
        if (name == null || name.isEmpty())
        {
            // 没必要转换
            return "";
        }
        else if (!name.contains("_") && !name.contains("-"))
        {
            // 不含下划线，仅将首字母大写
            return name.substring(0, 1).toUpperCase() + name.substring(1);
        }
        // 用下划线将原始字符串分割
        String[] camels = name.split("_");

        for (String camel : camels)
        {
            final String upperCase = camel.substring(0, 1).toUpperCase();
            final String lowerCase = camel.substring(1).toLowerCase();

            String[] camel1 = (upperCase+lowerCase).split("-");
            for (String came : camel1){
                // 跳过原始字符串中开头、结尾的下换线或双重下划线
                if (came.isEmpty())
                {
                    continue;
                }
                // 首字母大写
                result.append(came.substring(0, 1).toUpperCase());
                result.append(came.substring(1).toLowerCase());
            }

        }
        return result.toString();
    }

    public static String toCamelCase(String s) {
        if (s == null)
            return null;
        if (s.indexOf('_') == -1)
            return s;
        s = s.toLowerCase();
        StringBuilder sb = new StringBuilder(s.length());
        boolean upperCase = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '_') {
                upperCase = true;
            } else if (upperCase) {
                sb.append(Character.toUpperCase(c));
                upperCase = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static boolean matches(String str, List<String> strs) {
        if (isEmpty(str) || isEmpty(strs))
            return false;
        for (String pattern : strs) {
            if (isMatch(pattern, str))
                return true;
        }
        return false;
    }

    public static boolean isMatch(String pattern, String url) {
        AntPathMatcher matcher = new AntPathMatcher();
        return matcher.match(pattern, url);
    }

    public static <T> T cast(Object obj) {
        return (T)obj;
    }

    public static final String padl(Number num, int size) {
        return padl(num.toString(), size, '0');
    }

    public static final String padl(String s, int size, char c) {
        StringBuilder sb = new StringBuilder(size);
        if (s != null) {
            int len = s.length();
            if (s.length() <= size) {
                for (int i = size - len; i > 0; i--)
                    sb.append(c);
                sb.append(s);
            } else {
                return s.substring(len - size, len);
            }
        } else {
            for (int i = size; i > 0; i--)
                sb.append(c);
        }
        return sb.toString();
    }
}
