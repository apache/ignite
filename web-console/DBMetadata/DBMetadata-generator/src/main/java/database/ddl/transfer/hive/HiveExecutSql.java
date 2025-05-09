package database.ddl.transfer.hive;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.consts.HiveKeyWord;
import database.ddl.transfer.consts.HiveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.hive.HiveExecutSql;
import database.ddl.transfer.utils.DBConnUtils;

import java.sql.*;
import java.util.*;
/**
 *@ClassName HiveExecutSQL
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-03 15:34
 *@Version
 **/
public class HiveExecutSql {
    protected static Logger logger = LoggerFactory.getLogger(HiveExecutSql.class);
    /**
     * @author luoyuntian
     * @date 2020-01-03 15:49
     * @description 根据表名获取建表语句
     * @param
     * @return
     */
    public static String getHiveCreateDDL(String databaseName, String tableName, DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        StringBuilder result = new StringBuilder();
        String sql = "show create table "+databaseName+"."+tableName;
        PreparedStatement ps = null;

        ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            result.append((String) rs.getObject(1));
        }
        rs.close();

        DBConnUtils.closeConnection(con);

        return result.toString();
    }
    /**
     * @author luoyuntian
     * @date 2020-01-03 16:01
     * @description 获取hive库名
     * @param
     * @return
     */
    public static List<String> getDatabases(DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        List<String> databases = new ArrayList<>();
        String sql = "show databases";
        PreparedStatement ps = null;

        ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            databases.add((String) rs.getObject(1));
        }
        rs.close();
        DBConnUtils.closeConnection(con);

        return  databases;
    }

    /**
     * @author luoyuntian
     * @date 2020-01-08 17:26
     * @description 创建库
     * @param
     * @return
     */
    public static void createDatabase(String databaseName,DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        String createDatabase = "create database if not exists "+databaseName;
        PreparedStatement ps = null;
            ps = con.prepareStatement(createDatabase);
            ps.execute();
            DBConnUtils.closeConnection(con);

    }


    /**
     * @author luoyuntian
     * @date 2020-01-03 16:07
     * @description 根据库名获取表名
     * @param
     * @return
     */
    public static List<String> getTables(String databasesName,DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        List<String> tables = new ArrayList<>();
        String selectDatabaseSql = "use "+databasesName;
        String getTables = "show tables";

        PreparedStatement ps = con.prepareStatement(selectDatabaseSql);
        ps.execute();

        PreparedStatement ps2 = con.prepareStatement(getTables);
        ResultSet rs2 = ps2.executeQuery();
        String table = null;
        while(rs2.next()){
            table = (String) rs2.getObject(1);
            tables.add(table);
        }
        rs2.close();


        DBConnUtils.closeConnection(con);

        return tables;
    }
    /**
     * @author luoyuntian
     * @date 2020-01-08 10:07
     * @description 根据表结构获取存储类型
     * @param
     * @return
     */
    public static String getTableStoreType(String createDDL){
        if(createDDL.contains(HiveStoreType.FEATURE_TEXTFILE)){
            return HiveStoreType.TEXTFILE;
        }else if (createDDL.contains(HiveStoreType.FEATURE_ORCFILE)){
            return HiveStoreType.ORCFILE;
        }else if (createDDL.contains(HiveStoreType.FEATURE_PARQUET)){
            return  HiveStoreType.PARQUET;
        }else if (createDDL.contains(HiveStoreType.FEATURE_RCFILE)){
            return HiveStoreType.RCFILE;
        }else {
            return HiveStoreType.SEQUENCEFILE;
        }
    }

    /**
     * @author luoyuntian
     * @date 2020-01-08 10:41
     * @description 将查询出的建表语句格式化
     * @param
     * @return
     */
    public static String formatTableCreateDDL(String createDDL){
        //删除Location字段
        int location_index = createDDL.indexOf(HiveKeyWord.LOCATION);
        int tblproperties_index = createDDL.indexOf(HiveKeyWord.TBLPROPERTIES);
//        String result = createDDL.substring(0,location_index) + createDDL.substring(tblproperties_index);
        String result = createDDL.substring(0,location_index);
        return result;
    }

    /**
     * @author luoyuntian
     * @date 2020-01-08 14:44
     * @description 根据数据库名+建表语句建表
     * @param
     * @return
     */
    public static void  createTable(String database,String createDDL,DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        String changeDatabase = "use "+database;
        PreparedStatement selectDatabase =  con.prepareStatement(changeDatabase);
        selectDatabase.execute();
        String createDDLaddExists = addIfNotExists(createDDL);
        PreparedStatement ps = con.prepareStatement(createDDLaddExists);
        ps.execute();
        DBConnUtils.closeConnection(con);

    }
    /**
       * @author luoyuntian
       * @date 2020-01-16 17:38
       * @description 为建表语句加上if not exists
        * @param
       * @return
       */
    private static String addIfNotExists(String createDDL){
        int index = createDDL.indexOf(HiveKeyWord.TABLE)+5;
        String result = createDDL.substring(0,index)+" "+HiveKeyWord.IF_NOT_EXISTS+createDDL.substring(index);
        return result;
    }


    /**
     * @author luoyuntian
     * @date 2020-01-08 14:58
     * @description 获取partions
     * @param
     * @return
     */
    public static List<String> getPartions(String database,String table,DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        List<String> partitions =  new ArrayList<>();
        String showPartitions = "show partitions "+database+"."+table;
        PreparedStatement ps = con.prepareStatement(showPartitions);
        ResultSet rs = ps.executeQuery();
        String partition = null;
        while(rs.next()){
            partition = (String)rs.getObject(1);
            partitions.add(partition);
        }
        rs.close();

        DBConnUtils.closeConnection(con);
        return partitions;

    }
    /**
     * @author luoyuntian
     * @date 2020-01-08 15:17
     * @description 转换partition格式 eg"sex=f/class=20100501" => "sex='f',class='20100501'"
     * @param
     * @return
     */
    public static String convertPartition(String partition){
        String[] paritionLevel = partition.split("/");
        StringBuilder result = new StringBuilder();
        for(String eachLevel:paritionLevel){
            String[] oneLevel = eachLevel.split("=");
            StringBuilder oneLevelPartition = new StringBuilder(oneLevel[0]).append("=").append("'").append(oneLevel[1]).append("'");
            result.append(oneLevelPartition);
            result.append(",");
        }
        return result.deleteCharAt(result.lastIndexOf(",")).toString();

    }

    /**
     * @author luoyuntian
     * @date 2020-01-08 17:49
     * @description 为表新增分区
     * @param
     * @return
     */
    public static void addPartition(String databaseName,String tableName,String partition,DBSettings property) throws SQLException {
        Connection con = DBConnUtils.getNewConnection(property);
        String addPartitonSQL = "alter table "+databaseName+"."+tableName+" add partition("+partition+")";
        PreparedStatement ps = null;
            ps = con.prepareStatement(addPartitonSQL);
            ps.execute();
            DBConnUtils.closeConnection(con);


    }

}
