package org.apache.ignite.console.agent.db.introspector;

import org.apache.ignite.console.agent.db.DatabaseConfig;
import org.apache.ignite.console.agent.utils.DBMetadataUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class PGIntrospector extends DatabaseIntrospector {

    public PGIntrospector(DBMetadataUtils dbMetadataUtils) {
        super(dbMetadataUtils);
    }

    public PGIntrospector(DBMetadataUtils dbMetadataUtils, boolean forceBigDecimals, boolean useCamelCase) {
        super(dbMetadataUtils, forceBigDecimals, useCamelCase);
    }

    /**
     * 获取表名和注释映射
     *
     * @param config
     * @return
     * @throws java.sql.SQLException
     */
    protected Map<String, String> getTableComments(DatabaseConfig config) throws SQLException {
        Map<String, String> answer = new HashMap<String, String>();
        try {
            PreparedStatement preparedStatement = dbMetadataUtils.getConnection().prepareStatement("select tname,comments from(select relname as TNAME ,col_description(c.oid, 0) as COMMENTS from pg_class c where  relkind = 'r' and relname not like 'pg_%' and relname not like 'sql_%') as temp where comments is not null ");
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                answer.put(rs.getString(dbMetadataUtils.convertLetterByCase("tname")), rs.getString(dbMetadataUtils.convertLetterByCase("comments")));
            }
            closeResultSet(rs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return answer;
    }

    /**
     * 获取表字段注释
     *
     * @param config
     * @return
     * @throws java.sql.SQLException
     */
    @Override
    protected Map<String, Map<String, String>> getColumnComments(DatabaseConfig config) throws SQLException {
        Map<String, Map<String, String>> answer = new HashMap<String, Map<String, String>>();
        try {
            StringBuilder sqlBuilder = new StringBuilder("select tname,cname,comments from( ");
            sqlBuilder.append("SELECT col_description(a.attrelid,a.attnum) as comments,a.attname as cname,c.relname as tname FROM pg_class as c,pg_attribute as a where a.attrelid = c.oid and a.attnum>0 and c.relname not like 'pg_%' and c.relname not like 'sql_%') as temp where comments is not null ");
            PreparedStatement preparedStatement = dbMetadataUtils.getConnection().prepareStatement(sqlBuilder.toString());
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String tname = rs.getString(dbMetadataUtils.convertLetterByCase("tname"));
                if (!answer.containsKey(tname)) {
                    answer.put(tname, new HashMap<String, String>());
                }
                answer.get(tname).put(rs.getString(dbMetadataUtils.convertLetterByCase("cname")), rs.getString(dbMetadataUtils.convertLetterByCase("comments")));
            }
            closeResultSet(rs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return answer;
    }
}
