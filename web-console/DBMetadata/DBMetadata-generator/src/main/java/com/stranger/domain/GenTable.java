package com.stranger.domain;

import com.stranger.common.constants.GenConstants;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class GenTable extends BaseEntity {
    private static final long serialVersionUID = 1L;

    private Long tableId;

    private String tableName;

    private String tableComment;

    private String subTableName;

    private String subTableFkName;

    private String className;

    private String tplCategory = "crud";

    private String packageName;

    private String moduleName;

    private String urlPrefix;

    private String businessName;

    private String functionName;

    private String functionAuthor;

    private String genType;

    private String genPath;

    private GenTableColumn pkColumn;

    private com.stranger.domain.GenTable subTable;

    private List<GenTableColumn> columns;

    private String options;

    private String treeCode;

    private String treeParentCode;

    private String treeName;

    private String parentMenuId;

    private String parentMenuName;

    public String getUrlPrefix() {
        return this.urlPrefix;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }

    public Long getTableId() {
        return this.tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableComment() {
        return this.tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public String getSubTableName() {
        return this.subTableName;
    }

    public void setSubTableName(String subTableName) {
        this.subTableName = subTableName;
    }

    public String getSubTableFkName() {
        return this.subTableFkName;
    }

    public void setSubTableFkName(String subTableFkName) {
        this.subTableFkName = subTableFkName;
    }

    public String getClassName() {
        return this.className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getTplCategory() {
        return this.tplCategory;
    }

    public void setTplCategory(String tplCategory) {
        this.tplCategory = tplCategory;
    }

    public String getPackageName() {
        return this.packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getModuleName() {
        return this.moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getBusinessName() {
        return this.businessName;
    }

    public void setBusinessName(String businessName) {
        this.businessName = businessName;
    }

    public String getFunctionName() {
        return this.functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getFunctionAuthor() {
        return this.functionAuthor;
    }

    public void setFunctionAuthor(String functionAuthor) {
        this.functionAuthor = functionAuthor;
    }

    public String getGenType() {
        return this.genType;
    }

    public void setGenType(String genType) {
        this.genType = genType;
    }

    public String getGenPath() {
        return this.genPath;
    }

    public void setGenPath(String genPath) {
        this.genPath = genPath;
    }

    public GenTableColumn getPkColumn() {
        return this.pkColumn;
    }

    public void setPkColumn(GenTableColumn pkColumn) {
        this.pkColumn = pkColumn;
    }

    public com.stranger.domain.GenTable getSubTable() {
        return this.subTable;
    }

    public void setSubTable(com.stranger.domain.GenTable subTable) {
        this.subTable = subTable;
    }

    public List<GenTableColumn> getColumns() {
        return this.columns;
    }

    public void setColumns(List<GenTableColumn> columns) {
        this.columns = columns;
    }

    public String getOptions() {
        return this.options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getTreeCode() {
        return this.treeCode;
    }

    public void setTreeCode(String treeCode) {
        this.treeCode = treeCode;
    }

    public String getTreeParentCode() {
        return this.treeParentCode;
    }

    public void setTreeParentCode(String treeParentCode) {
        this.treeParentCode = treeParentCode;
    }

    public String getTreeName() {
        return this.treeName;
    }

    public void setTreeName(String treeName) {
        this.treeName = treeName;
    }

    public String getParentMenuId() {
        return this.parentMenuId;
    }

    public void setParentMenuId(String parentMenuId) {
        this.parentMenuId = parentMenuId;
    }

    public String getParentMenuName() {
        return this.parentMenuName;
    }

    public void setParentMenuName(String parentMenuName) {
        this.parentMenuName = parentMenuName;
    }

    public boolean isSub() {
        return isSub(this.tplCategory);
    }

    public static boolean isSub(String tplCategory) {
        return (tplCategory != null && StringUtils.equals("sub", tplCategory));
    }

    public boolean isTree() {
        return isTree(this.tplCategory);
    }

    public static boolean isTree(String tplCategory) {
        return (tplCategory != null && StringUtils.equals("tree", tplCategory));
    }

    public boolean isCrud() {
        return isCrud(this.tplCategory);
    }

    public static boolean isCrud(String tplCategory) {
        return (tplCategory != null && StringUtils.equals("crud", tplCategory));
    }

    public boolean isSuperColumn(String javaField) {
        return isSuperColumn(this.tplCategory, javaField);
    }

    public static boolean isSuperColumn(String tplCategory, String javaField) {
        if (isTree(tplCategory))
            return StringUtils.equalsAnyIgnoreCase(javaField,
                    (CharSequence[])ArrayUtils.addAll((Object[]) GenConstants.TREE_ENTITY, (Object[])GenConstants.BASE_ENTITY));
        return StringUtils.equalsAnyIgnoreCase(javaField, (CharSequence[])GenConstants.BASE_ENTITY);
    }
}
