package com.stranger.common.utils;

import com.stranger.domain.GenTable;
import com.stranger.domain.GenTableColumn;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.vertx.core.json.JsonObject;
import org.apache.velocity.VelocityContext;
import org.springframework.beans.factory.annotation.Value;

public class VelocityUtils {
    private static final String PROJECT_PATH = "main/java";

    private static final String MYBATIS_PATH = "main/resources/mapper";

    private static final String DEFAULT_PARENT_MENU_ID = "3";

    @Value("${url.api-prefix}")
    private String urlPrefix;

    public static VelocityContext prepareContext(GenTable genTable) {
        String moduleName = genTable.getModuleName();
        String businessName = genTable.getBusinessName();
        String packageName = genTable.getPackageName();
        String tplCategory = genTable.getTplCategory();
        String functionName = genTable.getFunctionName();
        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("tplCategory", genTable.getTplCategory());
        velocityContext.put("tableName", genTable.getTableName());
        velocityContext.put("functionName", StringUtil.isNotEmpty(functionName) ? functionName : "【请填写功能名称】");
                velocityContext.put("ClassName", genTable.getClassName());
        velocityContext.put("className", StringUtil.uncapitalize(genTable.getClassName()));
        velocityContext.put("moduleName", genTable.getModuleName());
        velocityContext.put("BusinessName", StringUtil.capitalize(genTable.getBusinessName()));
        velocityContext.put("businessName", genTable.getBusinessName());
        velocityContext.put("urlPrefix", genTable.getUrlPrefix());
        velocityContext.put("basePackage", getPackagePrefix(packageName));
        velocityContext.put("packageName", packageName);
        velocityContext.put("author", genTable.getFunctionAuthor());
        velocityContext.put("datetime", DateUtil.getDate());
        velocityContext.put("pkColumn", genTable.getPkColumn());
        velocityContext.put("importList", getImportList(genTable));
        velocityContext.put("permissionPrefix", getPermissionPrefix(moduleName, businessName));
        velocityContext.put("columns", genTable.getColumns());
        velocityContext.put("table", genTable);
        velocityContext.put("dicts", getDicts(genTable));
        setMenuVelocityContext(velocityContext, genTable);
        if ("tree".equals(tplCategory))
            setTreeVelocityContext(velocityContext, genTable);
        if ("sub".equals(tplCategory))
            setSubVelocityContext(velocityContext, genTable);
        return velocityContext;
    }

    public static void setMenuVelocityContext(VelocityContext context, GenTable genTable) {
        String options = genTable.getOptions();
        JsonObject paramsObj = new JsonObject(options);
        String parentMenuId = getParentMenuId(paramsObj);
        context.put("parentMenuId", parentMenuId);
    }

    public static void setTreeVelocityContext(VelocityContext context, GenTable genTable) {
        String options = genTable.getOptions();
        JsonObject paramsObj = new JsonObject(options);
        String treeCode = getTreecode(paramsObj);
        String treeParentCode = getTreeParentCode(paramsObj);
        String treeName = getTreeName(paramsObj);
        context.put("treeCode", treeCode);
        context.put("treeParentCode", treeParentCode);
        context.put("treeName", treeName);
        context.put("expandColumn", Integer.valueOf(getExpandColumn(genTable)));
        if (paramsObj.containsKey("treeParentCode"))
            context.put("tree_parent_code", paramsObj.getString("treeParentCode"));
        if (paramsObj.containsKey("treeName"))
            context.put("tree_name", paramsObj.getString("treeName"));
    }

    public static void setSubVelocityContext(VelocityContext context, GenTable genTable) {
        GenTable subTable = genTable.getSubTable();
        String subTableName = genTable.getSubTableName();
        String subTableFkName = genTable.getSubTableFkName();
        String subClassName = genTable.getSubTable().getClassName();
        String subTableFkClassName = StringUtil.convertToCamelCase(subTableFkName);
        context.put("subTable", subTable);
        context.put("subTableName", subTableName);
        context.put("subTableFkName", subTableFkName);
        context.put("subTableFkClassName", subTableFkClassName);
        context.put("subTableFkclassName", StringUtil.uncapitalize(subTableFkClassName));
        context.put("subClassName", subClassName);
        context.put("subclassName", StringUtil.uncapitalize(subClassName));
        context.put("subImportList", getImportList(genTable.getSubTable()));
    }

    public static List<String> getTemplateList(String tplCategory) {
        List<String> templates = new ArrayList<>();
        templates.add("vm/java/domain.java.vm");
        templates.add("vm/java/dto.java.vm");
        templates.add("vm/java/vo.java.vm");
        templates.add("vm/java/mapper.java.vm");
        templates.add("vm/java/service.java.vm");
        templates.add("vm/java/serviceImpl.java.vm");
        templates.add("vm/java/controller.java.vm");
        templates.add("vm/xml/pom.xml.vm");
        if (!"crud".equals(tplCategory))
            if (!"tree".equals(tplCategory))
                if ("sub".equals(tplCategory))
                    templates.add("vm/java/sub-domain.java.vm");
        return templates;
    }

    public static String getFileName(String template, GenTable genTable) {
        String fileName = "";
        String packageName = genTable.getPackageName();
        String moduleName = genTable.getModuleName();
        String className = genTable.getClassName();
        String businessName = genTable.getBusinessName();
        String javaPath = "main/java/" + StringUtil.replace(packageName, ".", "/");
        String mybatisPath = "main/resources/mapper/" + moduleName;
        String vuePath = "vue";
        if (template.contains("domain.java.vm"))
            fileName = StringUtil.format("{}/domain/{}.java", new Object[] { javaPath, className });
        if (template.contains("dto.java.vm"))
            fileName = StringUtil.format("{}/dto/{}Dto.java", new Object[] { javaPath, className });
        if (template.contains("vo.java.vm"))
            fileName = StringUtil.format("{}/vo/{}Vo.java", new Object[] { javaPath, className });
        if (template.contains("sub-domain.java.vm") && StringUtil.equals("sub", genTable.getTplCategory())) {
            fileName = StringUtil.format("{}/domain/{}.java", new Object[] { javaPath, genTable.getSubTable().getClassName() });
        } else if (template.contains("mapper.java.vm")) {
            fileName = StringUtil.format("{}/mapper/{}Mapper.java", new Object[] { javaPath, className });
        } else if (template.contains("service.java.vm")) {
            fileName = StringUtil.format("{}/service/{}Service.java", new Object[] { javaPath, className });
        } else if (template.contains("serviceImpl.java.vm")) {
            fileName = StringUtil.format("{}/service/impl/{}ServiceImpl.java", new Object[] { javaPath, className });
        } else if (template.contains("controller.java.vm")) {
            fileName = StringUtil.format("{}/controller/{}Controller.java", new Object[] { javaPath, className });
        } else if (template.contains("mapper.xml.vm")) {
            fileName = StringUtil.format("{}/{}Mapper.xml", new Object[] { mybatisPath, className });
        } else if (template.contains("sql.vm")) {
            fileName = businessName + "Menu.sql";
        } else if (template.contains("api.js.vm")) {
            fileName = StringUtil.format("{}/api/{}/{}.js", new Object[] { vuePath, moduleName, businessName });
        } else if (template.contains("index.vue.vm")) {
            fileName = StringUtil.format("{}/views/{}/{}/index.vue", new Object[] { vuePath, moduleName, businessName });
        } else if (template.contains("index-tree.vue.vm")) {
            fileName = StringUtil.format("{}/views/{}/{}/index.vue", new Object[] { vuePath, moduleName, businessName });
        }
        return fileName;
    }

    public static String getPackagePrefix(String packageName) {
        int lastIndex = packageName.lastIndexOf(".");
        return StringUtil.substring(packageName, 0, lastIndex);
    }

    public static HashSet<String> getImportList(GenTable genTable) {
        List<GenTableColumn> columns = genTable.getColumns();
        GenTable subGenTable = genTable.getSubTable();
        HashSet<String> importList = new HashSet<>();
        if (StringUtil.isNotNull(subGenTable))
            importList.add("java.util.List");
        for (GenTableColumn column : columns) {
            if ("Date".equals(column.getJavaType())) {
                importList.add("java.util.Date");
                importList.add("com.fasterxml.jackson.annotation.JsonFormat");
                continue;
            }
            if (!column.isSuperColumn() && "BigDecimal".equals(column.getJavaType()))
                importList.add("java.math.BigDecimal");
        }
        return importList;
    }

    public static String getDicts(GenTable genTable) {
        List<GenTableColumn> columns = genTable.getColumns();
        Set<String> dicts = new HashSet<>();
        addDicts(dicts, columns);
        if (StringUtil.isNotNull(genTable.getSubTable())) {
            List<GenTableColumn> subColumns = genTable.getSubTable().getColumns();
            addDicts(dicts, subColumns);
        }
        return StringUtil.join(dicts, ", ");
    }

    public static void addDicts(Set<String> dicts, List<GenTableColumn> columns) {
        for (GenTableColumn column : columns) {
            if (!column.isSuperColumn() && StringUtil.isNotEmpty(column.getDictType()) && StringUtil.equalsAny(column
                    .getHtmlType(), (CharSequence[])new String[] { "select", "radio", "checkbox" }))
                dicts.add("'" + column.getDictType() + "'");
        }
    }

    public static String getPermissionPrefix(String moduleName, String businessName) {
        return StringUtil.format("{}:{}", new Object[] { moduleName, businessName });
    }

    public static String getParentMenuId(JsonObject paramsObj) {
        if (StringUtil.isNotEmpty((Map)paramsObj) && paramsObj.containsKey("parentMenuId") &&
                StringUtil.isNotEmpty(paramsObj.getString("parentMenuId")))
            return paramsObj.getString("parentMenuId");
        return "3";
    }

    public static String getTreecode(JsonObject paramsObj) {
        if (paramsObj.containsKey("treeCode"))
            return StringUtil.toCamelCase(paramsObj.getString("treeCode"));
        return "";
    }

    public static String getTreeParentCode(JsonObject paramsObj) {
        if (paramsObj.containsKey("treeParentCode"))
            return StringUtil.toCamelCase(paramsObj.getString("treeParentCode"));
        return "";
    }

    public static String getTreeName(JsonObject paramsObj) {
        if (paramsObj.containsKey("treeName"))
            return StringUtil.toCamelCase(paramsObj.getString("treeName"));
        return "";
    }

    public static int getExpandColumn(GenTable genTable) {
        String options = genTable.getOptions();
        JsonObject paramsObj = new JsonObject(options);
        String treeName = paramsObj.getString("treeName");
        int num = 0;
        for (GenTableColumn column : genTable.getColumns()) {
            if (column.isList()) {
                num++;
                String columnName = column.getColumnName();
                if (columnName.equals(treeName))
                    break;
            }
        }
        return num;
    }
}
