package com.stranger.common.utils;

import com.stranger.common.utils.Convert;
import com.stranger.common.utils.StringUtil;

public class StrFormatter {
    public static final String EMPTY_JSON = "{}";

    public static final char C_BACKSLASH = '\\';

    public static final char C_DELIM_START = '{';

    public static final char C_DELIM_END = '}';

    public static String format(String strPattern, Object... argArray) {
        if (StringUtil.isEmpty(strPattern) || StringUtil.isEmpty(argArray))
            return strPattern;
        int strPatternLength = strPattern.length();
        StringBuilder sbuf = new StringBuilder(strPatternLength + 50);
        int handledPosition = 0;
        for (int argIndex = 0; argIndex < argArray.length; argIndex++) {
            int delimIndex = strPattern.indexOf("{}", handledPosition);
            if (delimIndex == -1) {
                if (handledPosition == 0)
                    return strPattern;
                sbuf.append(strPattern, handledPosition, strPatternLength);
                return sbuf.toString();
            }
            if (delimIndex > 0 && strPattern.charAt(delimIndex - 1) == '\\') {
                if (delimIndex > 1 && strPattern.charAt(delimIndex - 2) == '\\') {
                    sbuf.append(strPattern, handledPosition, delimIndex - 1);
                    sbuf.append(Convert.utf8Str(argArray[argIndex]));
                    handledPosition = delimIndex + 2;
                } else {
                    argIndex--;
                    sbuf.append(strPattern, handledPosition, delimIndex - 1);
                    sbuf.append('{');
                    handledPosition = delimIndex + 1;
                }
            } else {
                sbuf.append(strPattern, handledPosition, delimIndex);
                sbuf.append(Convert.utf8Str(argArray[argIndex]));
                handledPosition = delimIndex + 2;
            }
        }
        sbuf.append(strPattern, handledPosition, strPattern.length());
        return sbuf.toString();
    }
}
