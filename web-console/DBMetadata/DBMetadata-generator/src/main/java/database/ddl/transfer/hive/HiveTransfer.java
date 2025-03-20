package database.ddl.transfer.hive;

import database.ddl.transfer.bean.HiveDataBase;
import database.ddl.transfer.bean.HiveTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.consts.HiveKeyWord;
import database.ddl.transfer.hive.HiveExecutSql;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *@ClassName HiveTransfer
 *@Description 只支持从HIVE到HIVE的转换
 *@Author luoyuntian
 *@Date 2020-01-08 15:26
 *@Version
 **/
public final class HiveTransfer {
    private static Logger logger = LoggerFactory.getLogger(HiveTransfer.class);
    private static final String DEFAULT_DATABASE ="default";
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:03
       * @description 根据配置转换所有表结构
        * @param
       * @return
       */
    public static  boolean transferAll(DBSettings sourceProperty,DBSettings targetProperty) throws SQLException {       
        List<HiveDataBase> dataBases = getHiveDatabases(sourceProperty);
        //创建库
        System.out.println("*************************************************");
        System.out.println("创建库开始..");
        createDatabases(targetProperty,dataBases);
        System.out.println("*************************************************");
        //创建表
        System.out.println("创建表开始..");
        for(HiveDataBase dataBase:dataBases){
            List<HiveTable> tables = getHiveTables(sourceProperty,dataBase);
            createTables(targetProperty,tables);
            //加入分区
            for(HiveTable table:tables){
                System.out.println("创建"+table.getTableName()+"表的分区开始:");
                addPartition(targetProperty,table);
            }
        }
        System.out.println("*************************************************");
        return true;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-15 15:02
       * @description 增量数据，同步一个库下所有表的结构
        * @param
       * @return
       */
    public static boolean incrementByDatabase(DBSettings sourceProperty,DBSettings targetProperty, String databaseName) throws SQLException {
       
        //复用全部导入的代码
        List<HiveDataBase> dataBases =  new ArrayList<>();
        HiveDataBase hiveDataBase = new HiveDataBase();
        List<String> tableNames = HiveExecutSql.getTables(databaseName,sourceProperty);
        hiveDataBase.setDataBaseName(databaseName);
        hiveDataBase.setTables(tableNames);
        dataBases.add(hiveDataBase);
        //创建库
        createDatabases(targetProperty,dataBases);
        //创建表
        List<HiveTable> tables = getHiveTables(sourceProperty,hiveDataBase);
        createTables(targetProperty,tables);
        //加入分区
        for(HiveTable table:tables){
            System.out.println("创建"+table.getTableName()+"表的分区开始:");
            addPartition(targetProperty,table);
        }
        return true;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-15 15:20
       * @description 增量数据，同步一个表的结构
        * @param
       * @return
       */
    public static boolean incrementByTable(DBSettings sourceProperty,DBSettings targetProperty,String databaseName,String tableName) throws SQLException {

        //获取表信息
        HiveTable table = new HiveTable();
        String createDDL = HiveExecutSql.getHiveCreateDDL(databaseName,tableName,sourceProperty);
        List<String> partitions = new ArrayList<>();
        if(createDDL.contains(HiveKeyWord.PARTITIONED_BY)){
            partitions = HiveExecutSql.getPartions(databaseName,tableName,sourceProperty);
        }
        table.setCreateTableDDL(createDDL);
        table.setPartitions(partitions);
        table.setTableName(tableName);
        table.setDatabaseName(databaseName);
        //创建表
        String creatDDL =  HiveExecutSql.formatTableCreateDDL(table.getCreateTableDDL());
        HiveExecutSql.createTable(table.getDatabaseName(),creatDDL,targetProperty);
        //加入分区
        addPartition(targetProperty,table);
        return true;

    }


    /**
       * @author luoyuntian
       * @date 2020-01-15 15:28
       * @description 增量数据，在目标hive表里新增一个分区
        * @param
       * @return
       */
    public static boolean incrementByPartition(DBSettings targetProperty,String databaseName,String tableName,String partition) throws SQLException {
       
        //partition格式形如(sex ='f',class='20100503')从show parttion获取到的格式需要转换。
        //String convertpartition = HiveExecutSql.convertPartition(partition);
        HiveExecutSql.addPartition(databaseName,tableName,partition,targetProperty);
        return true;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 16:07
       * @description 获取源表所有库
        * @param
       * @return
       */
    private static List<HiveDataBase> getHiveDatabases(DBSettings sourceProperty) throws SQLException {
        List<HiveDataBase> dataBases = new ArrayList<>();
        List<String> databaseNames = HiveExecutSql.getDatabases(sourceProperty);
        for(String databaseName: databaseNames){
            HiveDataBase hiveDataBase = new HiveDataBase();
            List<String> tables = HiveExecutSql.getTables(databaseName,sourceProperty);
            hiveDataBase.setDataBaseName(databaseName);
            hiveDataBase.setTables(tables);
            dataBases.add(hiveDataBase);
        }
        return dataBases;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:25
       * @description 获取单个库下的所有表
        * @param
       * @return
       */
    private static List<HiveTable> getHiveTables(DBSettings sourceProperty,HiveDataBase dataBase) throws SQLException {
        List<HiveTable> tables = new ArrayList<>();
        List<String> tableNames = dataBase.getTables();
        String databaseName = dataBase.getDataBaseName();
        for(String tableName:tableNames){
            HiveTable table = new HiveTable();
            List<String> partitions = new ArrayList<>();
            String creatDDL = HiveExecutSql.getHiveCreateDDL(databaseName,tableName,sourceProperty);
            if(creatDDL.contains(HiveKeyWord.PARTITIONED_BY)){
                 partitions = HiveExecutSql.getPartions(databaseName,tableName,sourceProperty);
            }
            table.setCreateTableDDL(creatDDL);
            table.setPartitions(partitions);
            table.setTableName(tableName);
            table.setDatabaseName(databaseName);
            tables.add(table);
        }
        return tables;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:49
       * @description 在目标路径创建库
        * @param
       * @return
       */
    private static boolean createDatabases(DBSettings targetProperty,List<HiveDataBase> dataBases) throws SQLException {
        for(HiveDataBase dataBase:dataBases){
            if(!DEFAULT_DATABASE.equals(dataBase.getDataBaseName())) {
                System.out.println("创建"+dataBase.getDataBaseName()+"库");
                HiveExecutSql.createDatabase(dataBase.getDataBaseName(), targetProperty);
            }
        }
        return true;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 16:50
       * @description 在目标路径创建表
        * @param
       * @return
       */
    private static boolean createTables(DBSettings targetProperty,List<HiveTable> tables) throws SQLException {
        for(HiveTable table:tables){
            System.out.println("创建"+table.getTableName()+"表");
            System.out.println("*************************************************");
            String creatDDL =  HiveExecutSql.formatTableCreateDDL(table.getCreateTableDDL());
            HiveExecutSql.createTable(table.getDatabaseName(),creatDDL,targetProperty);
        }
        return true;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 17:51
       * @description 新增分区
        * @param
       * @return
       */
    private static boolean addPartition(DBSettings targetProperty,HiveTable table) throws SQLException {
        String databaseName = table.getDatabaseName();
        String tableName = table.getTableName();
        List<String> partitions = table.getPartitions();
        for(String partition:partitions){
            System.out.println("创建"+partition+"分区");
            String convertpartition = HiveExecutSql.convertPartition(partition);
            HiveExecutSql.addPartition(databaseName,tableName,convertpartition,targetProperty);
        }
        return true;
    }
}
