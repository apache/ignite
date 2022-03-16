package org.elasticsearch.relay;

/**
 * JSON 返回的数据布局格式
 * @author zjf
 *
 */
public enum ResponseFormat {
 FIELDSMETA, // CacheQueryResult { items =List<List<?>>, fieldsMeta=List<> } 
 DATASET,    // List<Map<String,Object>>
 HITS,       // Elasticsearch hits format
}
