package com.shard.jdbc.reader.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.google.common.base.Strings;
import com.shard.jdbc.exception.InvalidShardConfException;
import com.shard.jdbc.reader.Reader;
import com.shard.jdbc.shard.ShardProperty;
import com.shard.jdbc.shard.ShardType;

/**
 * shard configuration reader
 * Created by shun on 2015-12-16 16:10.
 */
public class ShardXmlReader extends Reader {

    private static final Logger logger = LogManager.getRootLogger();

    private static final String CLASS_ATTRIBUTE = "class";
    private static final String TYPE_ATTRIBUTE = "type";
    private static final String COLUMN_ATTRIBUTE = "column";

    private static final String MATCH_ATTRIBUTE = "match";
    private static final String RANGE_ATTRIBUTE = "range";

    private static final String SHARD_NODE = "shard";
    private static final String MATCH_NODE = "match";
    private static final String RANGE_NODE = "range";

    private static final String RANGE_HASH_SPLITTER = "/";

    private static final List<String> existedShardClasses = new ArrayList<String>();

    @Override
    public <T> List<T> process(String path, Class<T> tClass) throws Exception{
        logger.info(String.format("loading shard configuration:[%s]", path));

        SAXBuilder saxBuilder = new SAXBuilder();

        List<T> shardProperties = new ArrayList<T>();
        try {
            Document doc = saxBuilder.build(new File(path));
            Element root = doc.getRootElement();
            for (Element ele: root.getChildren(SHARD_NODE)) {
                if (existedShardClasses.contains(ele.getAttributeValue(CLASS_ATTRIBUTE))) {
                    throw new InvalidShardConfException(String.format("duplicate class:[%s] in shard configuration",
                            ele.getAttributeValue(CLASS_ATTRIBUTE)));
                }

                if (Strings.isNullOrEmpty(ele.getAttributeValue(CLASS_ATTRIBUTE))) {
                    throw new InvalidShardConfException(String.format("invalid configuration for shard node:[%s], missing class attribute",
                            ele.getAttributeValue(CLASS_ATTRIBUTE)));
                }
                if (Strings.isNullOrEmpty(ele.getAttributeValue(TYPE_ATTRIBUTE))) {
                    throw new InvalidShardConfException(String.format("invalid configuration for shard node:[%s], missing type attribute",
                            ele.getAttributeValue(TYPE_ATTRIBUTE)));
                }

                String type = ele.getAttributeValue(TYPE_ATTRIBUTE);

                //if specify shard type, then shard-property node need to be present
                if (type.equals(ShardType.HASH) || type.equals(ShardType.RANGE) || type.equals(ShardType.RANGE_HASH)) {
                    String propName = ele.getAttributeValue(COLUMN_ATTRIBUTE);
                    if (Strings.isNullOrEmpty(propName)) {
                        throw new InvalidShardConfException(String.format("invalid configuration for shard node:[%s], missing column attribute",
                                ele.getAttributeValue(CLASS_ATTRIBUTE)));
                    }                   
                }

                ShardProperty shardProperty = new ShardProperty();
                shardProperty.setClazz(ele.getAttributeValue(CLASS_ATTRIBUTE));
                shardProperty.setType(ele.getAttributeValue(TYPE_ATTRIBUTE));
                shardProperty.setColumn(ele.getAttributeValue(COLUMN_ATTRIBUTE));

                List<ShardProperty.MatchInfo> matchInfoList = new ArrayList<ShardProperty.MatchInfo>();
                //if shard type is range-hash, it has another node range, so join range and match value with /
                if (shardProperty.getType().equals(ShardType.RANGE_HASH)) {
                    List<Element> rangeNodeList = ele.getChildren(RANGE_NODE);
                    for (Element rangeNode:rangeNodeList) {
                        for (Element match:rangeNode.getChildren(MATCH_NODE)) {
                            ShardProperty.MatchInfo matchInfo = new ShardProperty.MatchInfo(
                                    rangeNode.getAttributeValue(RANGE_ATTRIBUTE) + RANGE_HASH_SPLITTER + match.getAttributeValue(MATCH_ATTRIBUTE),
                                    match.getTextTrim());
                            matchInfoList.add(matchInfo);
                        }
                    }
                }
                else if (shardProperty.getType().equals(ShardType.NONE)) {
                    Element match = ele.getChild(MATCH_NODE);
                    if (match != null) {
                        ShardProperty.MatchInfo matchInfo = new ShardProperty.MatchInfo(null, match.getTextTrim());
                        matchInfoList.add(matchInfo);
                    }
                }
                else if (!shardProperty.getType().equals(ShardType.NONE)) {
                    for (Element match:ele.getChildren(MATCH_NODE)) {
                        ShardProperty.MatchInfo matchInfo = new ShardProperty.MatchInfo(
                                match.getAttributeValue(MATCH_ATTRIBUTE), match.getTextTrim());
                        matchInfoList.add(matchInfo);
                    }
                }

                //如果没有Match信息，则直接抛出异常，不进行处理
                if (matchInfoList.size() == 0) {
                    throw new InvalidShardConfException("shard node for class:[%s] seems to be wrong, may be without match node",
                            ele.getAttributeValue(CLASS_ATTRIBUTE));
                }

                //用于判断是否重复
                existedShardClasses.add(ele.getAttributeValue(CLASS_ATTRIBUTE));

                shardProperty.setMatchInfoList(matchInfoList);
                shardProperties.add((T)shardProperty);
            }
        } catch (JDOMException e) {
            logger.error(e);
        } catch (IOException e) {
            logger.error(e);
        }

        return shardProperties;
    }

}
