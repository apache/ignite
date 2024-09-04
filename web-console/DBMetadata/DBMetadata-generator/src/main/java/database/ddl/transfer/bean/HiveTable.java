package database.ddl.transfer.bean;

import java.util.List;

/**
 *@ClassName HiveTable
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-08 14:30
 *@Version
 **/
public class HiveTable {
    /**
     *  所在库名
     */
    private String databaseName;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 建表语句
     */
    private String createTableDDL;
    /**
     * 分区
     */
    private List<String> partitions;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCreateTableDDL() {
        return createTableDDL;
    }

    public void setCreateTableDDL(String createTableDDL) {
        this.createTableDDL = createTableDDL;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
