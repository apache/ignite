package com.shard.jdbc.util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;


import com.shard.jdbc.database.DbInfo;
import com.shard.jdbc.exception.DbException;
import com.shard.jdbc.exception.NoMatchDataSourceException;
import com.shard.jdbc.reader.impl.DbXmlReader;
import com.shard.jdbc.reader.impl.ShardXmlReader;
import com.shard.jdbc.shard.Shard;
import com.shard.jdbc.shard.ShardProperty;
import com.shard.jdbc.shard.ShardProperty.MatchInfo;
import com.shard.jdbc.shard.ShardType;

/**
 * Created by shun on 2015-12-17 14:41.
 */
public class DbUtil {

    final static Logger logger = Logger.getLogger(DbUtil.class.getName());

    private static Map<String, DbInfo> dbInfoMap = new HashMap<String, DbInfo>();
    private static Map<String, Connection> connectionMap = new ConcurrentHashMap<String, Connection>();

    private static List<ShardProperty> shardProperties = new ArrayList<ShardProperty>();
    
    public static void init(String path){
    	dbInfoMap.clear();
    	shardProperties.clear();
    	connectionMap.clear();
    	
        try {
            List<DbInfo> dbInfoList = new DbXmlReader().process(path+"/database.xml", DbInfo.class);
            for (DbInfo dbInfo:dbInfoList) {
                dbInfoMap.put(dbInfo.getId(), dbInfo);
            }

            shardProperties = new ShardXmlReader().process(path+"/shard.xml", ShardProperty.class);
        } catch (Exception e) {
            throw new RuntimeException("error loading database.xml/shard.xml", e);
        }
    }

    /**
     * get connection for the specify dataSource
     * @param dataSourceId
     * @return
     * @throws NoMatchDataSourceException
     */
    public static Connection getConnection(String dataSourceId) throws NoMatchDataSourceException {
        DbInfo dbInfo = dbInfoMap.get(dataSourceId);

        if (dbInfo == null) {
            throw new NoMatchDataSourceException("no match dataSource found for dataSourceId:%s", dataSourceId);
        }
       
        try {
        	 Connection connPooled = connectionMap.get(dataSourceId);
             if (connPooled!= null) {
             	if(!connPooled.isClosed())
             		return connPooled;
             }
             else if(dbInfo.getDriverClass()!=null) {
            	 Class.forName(dbInfo.getDriverClass());
             }
             
            Connection conn = DriverManager.getConnection(dbInfo.getUrl(), dbInfo.getUsername(), dbInfo.getPassword());
            //cached for the future use
            connectionMap.put(dbInfo.getId(), conn);
            return conn;
        } catch (ClassNotFoundException e) {
            logger.warning(e.getMessage());
        } catch (SQLException e) {
        	logger.warning(e.getMessage());
        }
        throw new NoMatchDataSourceException("no match dataSource found for dataSourceId:%s", dataSourceId);
    }

    //mark used for split range value
    private static final String RANGE_SPLITTER = "~";
    //mark used for splitting range and hash-val for the range-hash type
    private static final String RANGE_HASH_SPLITTER = "/";
    //map used to saved the range-hash match list
    private static final Map<String, List<ShardProperty.MatchInfo>> rangeHashMap = new HashMap<String, List<ShardProperty.MatchInfo>>();

    /**
     * get connection by shard configuration for the provided class
     * @param clazz
     * @param shard         shard property
     * @return
     * @throws DbException
     */
    public static Connection getConnection(String clazz, Shard shard) throws DbException {
        //no shard provided
        if (shard == null) {
            return getConnectionForType(clazz);
        }

        String dataSourceId = getDataSourceId(clazz, shard);
        return getConnection(dataSourceId);
    }

    /**
     * get dataSourceId according to the class and shard specify
     * @param clazz     clazz
     * @param shard     shard property
     * @return
     */
    private static String getDataSourceId(String clazz, Shard shard) throws NoMatchDataSourceException {
        for (ShardProperty shardProperty:shardProperties) {
            if (shardProperty.getType() != ShardType.NONE && !shard.getColumn().equalsIgnoreCase(shardProperty.getColumn())) {
                throw new NoMatchDataSourceException("shard propName is error, configuration for node:[%s]'s shard-property is:[%s]," +
                        "but passing is:[%s]", shardProperty.getClazz(), shardProperty.getColumn(), shard.getColumn());
            }

            if (shardProperty.getClazz().equalsIgnoreCase(clazz)) {
                List<ShardProperty.MatchInfo> matchInfoList = shardProperty.getMatchInfoList();
                if (shardProperty.getType().equals(ShardType.HASH)) {
                    //calculate hash value
                    int hashVal = shard.getValue() % matchInfoList.size();
                    for (ShardProperty.MatchInfo matchInfo:matchInfoList) {
                        if (Integer.valueOf(matchInfo.getMatch()) == hashVal) {
                            return matchInfo.getDataSourceId();
                        }
                    }
                } else if (shardProperty.getType().equals(ShardType.RANGE)) {
                    for (ShardProperty.MatchInfo matchInfo:matchInfoList) {
                        //get range value
                        int low = Integer.parseInt(matchInfo.getMatch().split(RANGE_SPLITTER)[0]);
                        int high = Integer.parseInt(matchInfo.getMatch().split(RANGE_SPLITTER)[1]);

                        //check if in the current scope
                        if (shard.getValue() >= low && shard.getValue() <= high) {
                            return matchInfo.getDataSourceId();
                        }
                    }
                } else if (shardProperty.getType().equals(ShardType.RANGE_HASH)) {
                    for (ShardProperty.MatchInfo matchInfo:matchInfoList) {
                        //get range value
                        int low = Integer.parseInt(matchInfo.getMatch().split(RANGE_HASH_SPLITTER)[0].split(RANGE_SPLITTER)[0]);
                        int high = Integer.parseInt(matchInfo.getMatch().split(RANGE_HASH_SPLITTER)[0].split(RANGE_SPLITTER)[1]);

                        //check if in the current scope
                        if (shard.getValue() >= low && shard.getValue() <= high) {
                            List<ShardProperty.MatchInfo> miList = getRangeMatchInfos(matchInfoList, matchInfo.getMatch().split(RANGE_HASH_SPLITTER)[0]);

                            int hashVal = shard.getValue() % miList.size();
                            for (ShardProperty.MatchInfo mi:miList) {
                                if (hashVal == Integer.valueOf(mi.getMatch().split(RANGE_HASH_SPLITTER)[1])) {
                                    return mi.getDataSourceId();
                                }
                            }
                        }
                    }
                } else if (shardProperty.getType().equals(ShardType.NONE)) {
                    return shardProperty.getMatchInfoList().get(0).getDataSourceId();
                }
            }
        }
        throw new NoMatchDataSourceException(String.format("no match datasource for clazz:%s, and shard:[%s]", clazz,
                shard.getValue()));
    }

    /**
     * get specify datasource for the specify clazz and shard
     * @param clazz
     * @param shard
     * @return
     * @throws DbException
     * @throws SQLException
     */
    public static DataSource getDataSource(String clazz, Shard shard) throws DbException, SQLException {
        //no shard provided
        if (shard == null) {
            return getDataSourceForType(clazz);
        }

        String dataSourceId = getDataSourceId(clazz, shard);
        DbInfo dbInfo = dbInfoMap.get(dataSourceId);

        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setPassword(dbInfo.getPassword());
        basicDataSource.setUsername(dbInfo.getUsername());
        basicDataSource.setUrl(dbInfo.getUrl());
        basicDataSource.setDriverClassName(dbInfo.getDriverClass());

        return basicDataSource;
    }

    /**
     * get datasource for specify clazz
     * @param clazz clazz that need to be retrieve but not shard
     * @return
     * @throws NoMatchDataSourceException no match datasource found
     */
    private static DataSource getDataSourceForType(String clazz) throws NoMatchDataSourceException {
        for (ShardProperty shardProperty:shardProperties) {
            if (shardProperty.getClazz().equals(clazz) && shardProperty.getType().equals(ShardType.NONE)) {
                DbInfo dbInfo = dbInfoMap.get(shardProperty.getMatchInfoList().get(0).getDataSourceId());

                BasicDataSource basicDataSource = new BasicDataSource();
                basicDataSource.setDriverClassName(dbInfo.getDriverClass());
                basicDataSource.setUrl(dbInfo.getUrl());
                basicDataSource.setUsername(dbInfo.getUsername());
                basicDataSource.setPassword(dbInfo.getPassword());

                return basicDataSource;
            }
        }
        throw new NoMatchDataSourceException("no match datasource for clazz:%s", clazz);
    }

    /**
     * get the specify rangeVal's range-hash match list
     * @param matchInfoList     all the matches in the current shard node
     * @param rangeVal          range value that need to find the match list
     * @return
     */
    private static List<ShardProperty.MatchInfo> getRangeMatchInfos(List<ShardProperty.MatchInfo> matchInfoList, String rangeVal) {
        //if existed, then get from memory
        if (rangeHashMap.containsKey(rangeVal)) {
            return rangeHashMap.get(rangeVal);
        }

        List<ShardProperty.MatchInfo> miList = new ArrayList<ShardProperty.MatchInfo>();
        for (ShardProperty.MatchInfo matchInfo:matchInfoList) {
            if (matchInfo.getMatch().split(RANGE_HASH_SPLITTER)[0].equals(rangeVal)) {
                miList.add(matchInfo);
            }
        }

        //cached it
        rangeHashMap.put(rangeVal, miList);
        return miList;
    }

    /**
     * getConnection for the not shard specify class
     * @param clazz
     * @return
     * @throws NoMatchDataSourceException
     */
    public static Connection getConnectionForType(String clazz) throws NoMatchDataSourceException {
        for (ShardProperty shardProperty:shardProperties) {
            if (shardProperty.getClazz().equals(clazz) && shardProperty.getType().equals(ShardType.NONE)) {
                return getConnection(shardProperty.getMatchInfoList().get(0).getDataSourceId());
            }
        }
        throw new NoMatchDataSourceException("no match shard datasource");
    }

    /**
     * ShardProperty for the not shard specify class
     * @param clazz
     * @return
     * @throws NoMatchDataSourceException
     */
    public static ShardProperty getShardPropertyForType(String clazz) throws NoMatchDataSourceException {
        for (ShardProperty shardProperty:shardProperties) {
            if (shardProperty.getClazz().equals(clazz)) {
            	if(shardProperty.getType().equals(ShardType.NONE))
            		return null;
            	return shardProperty;
            }
        }
        throw new NoMatchDataSourceException("no match shard datasource");
    }
    
    /**
     * getConnection for the not shard specify class
     * @param clazz
     * @return
     * @throws NoMatchDataSourceException
     */
    public static Collection<DbInfo> getDataNodeListForType(String clazz) {
    	Map<String,DbInfo> map = new HashMap<>(dbInfoMap.size());
    	for (ShardProperty shardProperty:shardProperties) {
            if (shardProperty.getClazz().equals(clazz)) {
            	for(MatchInfo match: shardProperty.getMatchInfoList()) {
            		DbInfo dbInfo = dbInfoMap.get(match.getDataSourceId());
            		if(dbInfo!=null) {
            			map.put(match.getDataSourceId(), dbInfo);
            		
	            		if(shardProperty.getType().equals(ShardType.NONE)) {
	            			break;
	            		}
            		}
            	}
            }
        }
    	if(map.isEmpty()) {
    		//throw new NoMatchDataSourceException("no match shard datasource");
    		return dbInfoMap.values(); //返回全部数据节点
    	}
    	return map.values();
    }
}
