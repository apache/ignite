package org.apache.ignite.console.agent.db;

import java.util.List;

/**
 * 内省数据库时的处理接口
 */
public interface DatabaseProcess {
    /**
     * 开始处理
     */
    void processStart();

    /**
     * 处理字段
     *
     * @param table
     * @param column
     */
    void processColumn(IntrospectedTable table, IntrospectedColumn column);

    /**
     * 处理表
     *
     * @param table
     */
    void processTable(IntrospectedTable table);

    /**
     * 处理完成
     *
     * @param introspectedTables
     */
    void processComplete(List<IntrospectedTable> introspectedTables);
}
