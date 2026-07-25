package com.stranger.common.utils;

import com.stranger.common.config.GenConfig;
import com.stranger.common.constants.GenConstants;
import com.stranger.domain.GenTable;
import com.stranger.domain.GenTableColumn;
import java.util.Arrays;

import org.apache.commons.lang3.RegExUtils;

public class GenUtils {
    public static void initTable(GenTable genTable, String operName,GenConfig genConfig) {
        genTable.setClassName(convertClassName(genTable.getTableName(),genConfig));
        genTable.setPackageName(genConfig.getPackageName());
        genTable.setModuleName(getModuleName(genConfig.getPackageName()));
        genTable.setBusinessName(StringUtil.toCamelCase(getBusinessName(genTable.getTableName())));
        genTable.setFunctionName(replaceText(genTable.getTableComment()));
        genTable.setFunctionAuthor(genConfig.getAuthor());
        genTable.setCreateBy(operName);
    }

    private static String convertComment(String comment) {
        if (StringUtil.isEmpty(comment))
            return "";
        return comment.replaceAll("\"", "").replaceAll("\n", "");
    }

    public static void initColumnField(GenTableColumn column, GenTable table) {
        column.setColumnComment(convertComment(column.getColumnComment()));
        String dataType = getDbType(column.getColumnType());
        String columnName = column.getColumnName();
        column.setTableId(table.getTableId());
        column.setCreateBy(table.getCreateBy());
        column.setJavaField(StringUtil.toCamelCase(columnName));
        column.setJavaType("String");
        column.setQueryType("EQ");
        if (arraysContains(GenConstants.COLUMNTYPE_STR, dataType) || arraysContains(GenConstants.COLUMNTYPE_TEXT, dataType)) {
            Integer columnLength = getColumnLength(column.getColumnType());
            String htmlType = (columnLength.intValue() >= 500 || arraysContains(GenConstants.COLUMNTYPE_TEXT, dataType)) ? "textarea" : "input";
            column.setHtmlType(htmlType);
        } else if (arraysContains(GenConstants.COLUMNTYPE_TIME, dataType)) {
            column.setJavaType("Date");
            column.setHtmlType("datetime");
        } else if (arraysContains(GenConstants.COLUMNTYPE_NUMBER, dataType)) {
            column.setHtmlType("input");
            String[] str = StringUtil.split(StringUtil.substringBetween(column.getColumnType(), "(", ")"), ",");
            if (str != null && str.length == 2 && Integer.parseInt(str[1]) > 0) {
                column.setJavaType("BigDecimal");
            } else if (str != null && str.length == 1 && Integer.parseInt(str[0]) <= 10) {
                column.setJavaType("Integer");
            } else {
                column.setJavaType("Long");
            }
        }
        column.setIsInsert("1");
        if (!arraysContains(GenConstants.COLUMNNAME_NOT_EDIT, columnName) && !column.isPk())
            column.setIsEdit("1");
        if (!arraysContains(GenConstants.COLUMNNAME_NOT_LIST, columnName) && !column.isPk())
            column.setIsList("1");
        if (!arraysContains(GenConstants.COLUMNNAME_NOT_QUERY, columnName) && !column.isPk())
            column.setIsQuery("1");
        if (StringUtil.endsWithIgnoreCase(columnName, "name"))
            column.setQueryType("LIKE");
        if (StringUtil.endsWithIgnoreCase(columnName, "status")) {
            column.setHtmlType("radio");
        } else if (StringUtil.endsWithIgnoreCase(columnName, "type") ||
                StringUtil.endsWithIgnoreCase(columnName, "sex")) {
            column.setHtmlType("select");
        } else if (StringUtil.endsWithIgnoreCase(columnName, "image")) {
            column.setHtmlType("imageUpload");
        } else if (StringUtil.endsWithIgnoreCase(columnName, "file")) {
            column.setHtmlType("fileUpload");
        } else if (StringUtil.endsWithIgnoreCase(columnName, "content")) {
            column.setHtmlType("editor");
        }
    }

    public static boolean arraysContains(String[] arr, String targetValue) {
        return Arrays.<String>asList(arr).contains(targetValue);
    }

    public static String getModuleName(String packageName) {
        int lastIndex = packageName.lastIndexOf(".");
        int nameLength = packageName.length();
        return StringUtil.substring(packageName, lastIndex + 1, nameLength);
    }

    public static String getBusinessName(String tableName) {
        int firstIndex = tableName.indexOf("_");
        int nameLength = tableName.length();
        return StringUtil.substring(tableName, firstIndex + 1, nameLength);
    }

    public static String convertClassName(String tableName,GenConfig genConfig) {
        boolean autoRemovePre = genConfig.getAutoRemovePre();
        String tablePrefix = genConfig.getTablePrefix();
        if (autoRemovePre && StringUtil.isNotEmpty(tablePrefix)) {
            String[] searchList = StringUtil.split(tablePrefix, ",");
            tableName = replaceFirst(tableName, searchList);
        }
        return StringUtil.convertToCamelCase(tableName);
    }

    public static String replaceFirst(String replacementm, String[] searchList) {
        String text = replacementm;
        for (String searchString : searchList) {
            if (replacementm.startsWith(searchString)) {
                text = replacementm.replaceFirst(searchString, "");
                break;
            }
        }
        return text;
    }

    public static String replaceText(String text) {
        return RegExUtils.replaceAll(text, "(?:表)", "");
    }

    public static String getDbType(String columnType) {
        if (StringUtil.indexOf(columnType, "(") > 0)
            return StringUtil.substringBefore(columnType, "(");
        return columnType;
    }

    public static Integer getColumnLength(String columnType) {
        if (StringUtil.indexOf(columnType, "(") > 0) {
            String length = StringUtil.substringBetween(columnType, "(", ")");
            return Integer.valueOf(length);
        }
        return Integer.valueOf(0);
    }
}
