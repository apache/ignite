package database.ddl.transfer.utils;

import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.utils.DBUrlUtil;

import java.sql.*;
/**
 *@ClassName HiveConnUtils
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-03 15:38
 *@Version
 **/

public class DBConnUtils {
    /**
       * @author luoyuntian
       * @date 2020-01-08 15:56
       * @description 根据配置创建hive连接
        * @param
       * @return
       */
    public static Connection getNewConnection(DBSettings property){
        Connection con = null;
        try {
           Class.forName(property.getDriverClass());
           String url = DBUrlUtil.generateDataBaseUrl(property);
           con = DriverManager.getConnection(url,property.getUserName(),property.getUserPassword());
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
