package org.apache.ignite.console.agent.utils;

import java.util.Collection;
import java.util.List;

/**
 * @author liuzh
 * @since 2015-03-19
 */
public class SqlStringUtils {

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

    public static boolean isEmpty(Collection<?> value) {
        if (value == null || value.size() == 0) {
            return true;
        }
        return false;
    }

    public static boolean isNotEmpty(Collection<?> value) {
        return !isEmpty(value);
    }
    
    /**
     * 去掉SQL注释
     * @param sql
     * @return
     */
    public static String removeSQLComments(String sql) {  
        StringBuilder result = new StringBuilder();  
        boolean inString = false;  
        boolean inMultiLineComment = false;  
        boolean wasEscape = false;  
  
        for (int i = 0; i < sql.length(); i++) {  
            char ch = sql.charAt(i);  
  
            // 处理字符串字面量  
            if (ch == '\'' && !wasEscape) {  
                inString = !inString;  
            }  
  
            // 处理转义字符  
            if (ch == '\\' && !inString) {  
                wasEscape = true;  
                result.append(ch);  
                continue;  
            }  
  
            // 如果不在字符串字面量中  
            if (!inString) {  
                // 处理多行注释  
                if (!inMultiLineComment) {  
                    if (i + 1 < sql.length() && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {  
                        inMultiLineComment = true;  
                        i++; // 跳过 '*'  
                        continue;  
                    }  
  
                    // 处理单行注释  
                    if (i + 1 < sql.length() && sql.charAt(i) == '-' && sql.charAt(i + 1) == '-') {  
                        // 查找行尾  
                        int endOfLine = sql.indexOf('\n', i);  
                        if (endOfLine == -1) {  
                            // 如果没有找到换行符，则到字符串末尾  
                            endOfLine = sql.length();  
                        }  
                        i = endOfLine - 1; // 跳过剩余的单行注释  
                        continue;  
                    }  
                } else if (inMultiLineComment) {  
                    if (i + 1 < sql.length() && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {  
                        inMultiLineComment = false;  
                        i++; // 跳过 '/'  
                        continue;  
                    }  
                }  
            }  
  
            // 重置转义标志  
            if (wasEscape) {  
                wasEscape = false;  
            }  
  
            // 如果不是注释或字符串字面量的一部分，则添加到结果中  
            if (!inString && !inMultiLineComment) {  
                result.append(ch);  
            }  
        }  
  
        return result.toString();  
    }
}
