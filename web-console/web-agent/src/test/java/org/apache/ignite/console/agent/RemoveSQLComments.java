package org.apache.ignite.console.agent;

import org.apache.ignite.console.agent.db.JdbcQueryExecutor;
import org.apache.ignite.console.agent.utils.SqlStringUtils;

public class RemoveSQLComments {  
	  
    
  
    public static void main(String[] args) {  
        String sql = "SELECT * FROM users WHERE id = 1; -- 这是一个单行注释\n" +  
                     "/* 这是一个多行注释\n" +  
                     "包含多行 */\n" +  
                     "SELECT name FROM users WHERE name = 'O''Reilly' -- 注意这里的单引号";  
  
        String cleanedSql = SqlStringUtils.removeSQLComments(sql);
        System.out.println(cleanedSql);  
    }  
}