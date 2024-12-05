package database.ddl.transfer.hive;

import database.ddl.transfer.bean.HiveConnectionProperty;

import java.sql.*;
/**
 *@ClassName HiveConnUtils
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-03 15:38
 *@Version
 **/

public class HiveConnUtils {
    /**
       * @author luoyuntian
       * @date 2020-01-08 15:56
       * @description 根据配置创建hive连接
        * @param
       * @return
       */
    public static Connection getHiveConnection(HiveConnectionProperty property){
        Connection con = null;
        try {
           Class.forName(property.getDriver());
           con = DriverManager.getConnection(property.getUrl(),property.getUserName(),property.getPassword());
       }catch (Exception e){
           e.printStackTrace();
       }
        return con;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:38
       * @description TODO
        * @param
       * @return
       */
    public static void closeConnection(Connection con){
        try{
            con.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
