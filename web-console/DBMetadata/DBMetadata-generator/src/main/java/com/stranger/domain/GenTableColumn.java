package com.stranger.domain;

import com.stranger.common.utils.StringUtil;

public class GenTableColumn extends BaseEntity {
    private static final long serialVersionUID = 1L;

    private Long columnId;

    private Long tableId;

    private String columnName;

    private String columnComment;

    private String columnType;

    private String javaType;

    private String javaField;

    private String isPk;

    private String isIncrement;

    private String isRequired;

    private String isInsert;

    private String isEdit;

    private String isList;

    private String isQuery;

    private String queryType;

    private String htmlType;

    private String dictType;

    private Integer sort;

    public void setColumnId(Long columnId) {
        this.columnId = columnId;
    }

    public Long getColumnId() {
        return this.columnId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public Long getTableId() {
        return this.tableId;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    public String getColumnComment() {
        return this.columnComment;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnType() {
        return this.columnType;
    }

    public void setJavaType(String javaType) {
        this.javaType = javaType;
    }

    public String getJavaType() {
        return this.javaType;
    }

    public void setJavaField(String javaField) {
        this.javaField = javaField;
    }

    public String getJavaField() {
        return this.javaField;
    }

    public String getCapJavaField() {
        return StringUtil.capitalize(this.javaField);
    }

    public void setIsPk(String isPk) {
        this.isPk = isPk;
    }

    public String getIsPk() {
        return this.isPk;
    }

    public boolean isPk() {
        return isPk(this.isPk);
    }

    public boolean isPk(String isPk) {
        return (isPk != null && StringUtil.equals("1", isPk));
    }

    public String getIsIncrement() {
        return this.isIncrement;
    }

    public void setIsIncrement(String isIncrement) {
        this.isIncrement = isIncrement;
    }

    public boolean isIncrement() {
        return isIncrement(this.isIncrement);
    }

    public boolean isIncrement(String isIncrement) {
        return (isIncrement != null && StringUtil.equals("1", isIncrement));
    }

    public void setIsRequired(String isRequired) {
        this.isRequired = isRequired;
    }

    public String getIsRequired() {
        return this.isRequired;
    }

    public boolean isRequired() {
        return isRequired(this.isRequired);
    }

    public boolean isRequired(String isRequired) {
        return (isRequired != null && StringUtil.equals("1", isRequired));
    }

    public void setIsInsert(String isInsert) {
        this.isInsert = isInsert;
    }

    public String getIsInsert() {
        return this.isInsert;
    }

    public boolean isInsert() {
        return isInsert(this.isInsert);
    }

    public boolean isInsert(String isInsert) {
        return (isInsert != null && StringUtil.equals("1", isInsert));
    }

    public void setIsEdit(String isEdit) {
        this.isEdit = isEdit;
    }

    public String getIsEdit() {
        return this.isEdit;
    }

    public boolean isEdit() {
        return isInsert(this.isEdit);
    }

    public boolean isEdit(String isEdit) {
        return (isEdit != null && StringUtil.equals("1", isEdit));
    }

    public void setIsList(String isList) {
        this.isList = isList;
    }

    public String getIsList() {
        return this.isList;
    }

    public boolean isList() {
        return isList(this.isList);
    }

    public boolean isList(String isList) {
        return (isList != null && StringUtil.equals("1", isList));
    }

    public void setIsQuery(String isQuery) {
        this.isQuery = isQuery;
    }

    public String getIsQuery() {
        return this.isQuery;
    }

    public boolean isQuery() {
        return isQuery(this.isQuery);
    }

    public boolean isQuery(String isQuery) {
        return (isQuery != null && StringUtil.equals("1", isQuery));
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getQueryType() {
        return this.queryType;
    }

    public String getHtmlType() {
        return this.htmlType;
    }

    public void setHtmlType(String htmlType) {
        this.htmlType = htmlType;
    }

    public void setDictType(String dictType) {
        this.dictType = dictType;
    }

    public String getDictType() {
        return this.dictType;
    }

    public void setSort(Integer sort) {
        this.sort = sort;
    }

    public Integer getSort() {
        return this.sort;
    }

    public boolean isSuperColumn() {
        return isSuperColumn(this.javaField);
    }

    public static boolean isSuperColumn(String javaField) {
        return StringUtil.equalsAnyIgnoreCase(javaField, new CharSequence[] { "createBy", "createTime", "updateBy", "updateTime", "remark", "parentName", "parentId", "orderNum", "ancestors" });
    }

    public boolean isUsableColumn() {
        return isUsableColumn(this.javaField);
    }

    public static boolean isUsableColumn(String javaField) {
        return StringUtil.equalsAnyIgnoreCase(javaField, new CharSequence[] { "parentId", "orderNum", "remark" });
    }

    public String readConverterExp() {
        String remarks = StringUtil.substringBetween(this.columnComment, ", ");
        StringBuffer sb = new StringBuffer();
        if (StringUtil.isNotEmpty(remarks)) {
            for (String value : remarks.split(" ")) {
                if (StringUtil.isNotEmpty(value)) {
                    Object startStr = value.subSequence(0, 1);
                    String endStr = value.substring(1);
                    sb.append("").append(startStr).append("=").append(endStr).append(",");
                }
            }
            return sb.deleteCharAt(sb.length() - 1).toString();
        }
        return this.columnComment;
    }
}
