package com.shard.jdbc.reader.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

import com.google.common.base.Strings;
import com.shard.jdbc.database.DbInfo;
import com.shard.jdbc.exception.DuplicateDBException;
import com.shard.jdbc.exception.InvalidShardConfException;
import com.shard.jdbc.reader.Reader;

/**
 * database configuration reader
 * Created by shun on 2015-12-17 14:06.
 */
public class DbXmlReader extends Reader {

    private Logger logger = Logger.getRootLogger();

    private static final String DATABASE_NODE = "database";
    private static final String DRIVER_NODE = "driver";
    private static final String USERNAME_NODE = "username";
    private static final String PASSWORD_NODE = "password";
    private static final String URL_NODE = "url";

    private static final String ID_ATTRIBUTE = "id";

    private Map<String, DbInfo> dbInfoMap = new HashMap<String, DbInfo>();

    @Override
    public <T> List<T> process(String path, Class<T> tClass) throws Exception{
        logger.info(String.format("loading db configuration:[%s]", path));

        SAXBuilder saxBuilder = new SAXBuilder();
        List<T> dbInfoList = new ArrayList<T>();

        Document document = saxBuilder.build(new File(path));
        Element root = document.getRootElement();

        //iterate every database node
        for (Element database: root.getChildren(DATABASE_NODE)) {
            String id = database.getAttributeValue(ID_ATTRIBUTE);
            if (Strings.isNullOrEmpty(id)) {
                throw new InvalidShardConfException("database id is mandatory, please check your database file");
            }

            //if has duplicate id, an exception will be thrown
            if (dbInfoMap.containsKey(id)) {
                throw new DuplicateDBException("database:[%s] has defined, check your file:%s", id, path);
            }

            DbInfo dbInfo = new DbInfo();
            dbInfo.setId(id);
            dbInfo.setDriverClass(database.getChildTextTrim(DRIVER_NODE));
            dbInfo.setPassword(database.getChildTextTrim(PASSWORD_NODE));
            dbInfo.setUrl(database.getChildTextTrim(URL_NODE));
            dbInfo.setUsername(database.getChildTextTrim(USERNAME_NODE));

            //cache it, used to check if duplicated
            dbInfoMap.put(id, dbInfo);
            dbInfoList.add((T)dbInfo);
        }

        return dbInfoList;
    }

}
