package de.kp.works.ignite;


import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.binary.*;
import org.apache.ignite.*;

import de.kp.works.ignite.graph.*;
import org.apache.ignite.cache.*;


import java.util.*;

public final class IgniteUtil{
	public static IgniteUtil MODULE$ = new IgniteUtil();
	/**
	 *  只使用key的前面冒号开头的字符串前缀
	 * @author admin
	 *
	 */
	public static class KeyPrefixAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper{		
		private static final long serialVersionUID = 1L;

		@Override public Object affinityKey(Object key) {
			 if(key instanceof String) {
				 String strKey = key.toString();
				 int pos = strKey.indexOf(':');
				 if(pos>0) {
					 return strKey.substring(0,pos);
				 }
				 return key;
			 }
			 return super.affinityKey(key);
		 }
				
	}
    
    public void createCacheIfNotExists(final Ignite ignite, final String table, final CacheConfiguration<String, BinaryObject> cfg) {
        final boolean exists = ignite.cacheNames().contains(table);
        if (!exists) {
            ignite.createCache(cfg);
        }
    }
    
    public IgniteCache<String, BinaryObject> getOrCreateCache(final Ignite ignite, final String table, final String namespace) {
        final boolean exists = ignite.cacheNames().contains(table);
        return (exists ? ignite.cache(table) : this.createCache(ignite, table, namespace));
    }
    
    public IgniteCache<String, BinaryObject> createCache(final Ignite ignite, final String table, final String namespace) {
        return this.createCache(ignite, table, namespace, CacheMode.PARTITIONED);
    }
    
    public IgniteCache<String, BinaryObject> createCache(final Ignite ignite, final String name, final String namespace, final CacheMode cacheMode) {
        
        ElementType elementType = null;
        /*
         * Retrieve element type from provided
         * cache (table) name
         */
        if (name.equals(namespace + "_" + IgniteConstants.EDGES)) {
            elementType = ElementType.EDGE;
        }
        else if (name.equals(namespace + "_" + IgniteConstants.VERTICES)) {
            elementType = ElementType.VERTEX;
        }
        else if (name.startsWith(namespace + "_")) {
            elementType = ElementType.DOCUMENT;
        }
        else if (name.startsWith(namespace + ".")) {
            elementType = ElementType.DOCUMENT;
        }
        else {
            elementType = ElementType.UNDEFINED;
            throw new RuntimeException("table name not support!"+name);
        }
        final ElementType tableType = elementType;
        final CacheConfiguration<String, BinaryObject> cfg = this.createCacheCfg(name, tableType, cacheMode);
        return ignite.createCache(cfg);
    }
    
    public CacheConfiguration<String, BinaryObject> createCacheCfg(final String table, final ElementType tableType, final CacheMode cacheMode) {
        final QueryEntity qe = this.buildQueryEntity(table, tableType);
        final ArrayList<QueryEntity> qes = new ArrayList<>();
        qes.add(qe);
        final CacheConfiguration<String, BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setName(table);
        cfg.setStoreKeepBinary(false);
        cfg.setIndexedTypes(new Class[] { String.class, BinaryObject.class });
        cfg.setCacheMode(cacheMode);
        cfg.setQueryEntities(qes);
        // add@byron
        cfg.setAffinityMapper(new KeyPrefixAffinityKeyMapper());
        // end@
        return cfg;
    }
    
    public QueryEntity buildQueryEntity(final String table, final ElementType elementType) {
        final QueryEntity qe = new QueryEntity();
        qe.setKeyType("java.lang.String");
        qe.setValueType(table);
        
        if (elementType == ElementType.EDGE) {
        	
        	qe.setFields(this.buildEdgeFields());
            qe.setIndexes(this.buildEdgeIndexs());
            return qe;
        }
        else if(elementType == ElementType.VERTEX) {
        	
        	qe.setFields(this.buildVertexFields());
            qe.setIndexes(this.buildVertexIndexs());
            return qe;
        } 
        if (elementType == null || !elementType.equals(ElementType.DOCUMENT)) {
            throw new RuntimeException(new StringBuilder().append("Table '").append(table).append("' is not supported.").toString());
        }
        qe.setFields(this.buildDocumentFields(qe));
        
        if(!table.endsWith("_index")) {
        	qe.setIndexes(this.buildDocumentIndexs());
        }
        return qe;
    }
    
    public LinkedHashMap<String, String> buildEdgeFields() {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.String");
        fields.put("id_type", "java.lang.String");
        fields.put("label", "java.lang.String");
        fields.put("to", "java.lang.String");
        fields.put("to_type", "java.lang.String");
        fields.put("source", "java.lang.String");
        fields.put("source_type", "java.lang.String");
        fields.put("created_at", "java.lang.Long");
        fields.put("updated_at", "java.lang.Long");
        fields.put("property_key", "java.lang.String");
        fields.put("property_type", "java.lang.String");
        fields.put("property_value", "java.lang.String");
        return fields;
    }
    
    public List<QueryIndex> buildEdgeIndexs() {
        final List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex("source"));
        indexes.add(new QueryIndex("to"));
        return indexes;
    }
    
    public LinkedHashMap<String, String> buildVertexFields() {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.String");
        fields.put("id_type", "java.lang.String");
        fields.put("label", "java.lang.String");
        fields.put("created_at", "java.lang.Long");
        fields.put("updated_at", "java.lang.Long");
        fields.put("property_key", "java.lang.String");
        fields.put("property_type", "java.lang.String");
        fields.put("property_value", "java.lang.String");
        return fields;
    }
    
    public List<QueryIndex> buildVertexIndexs() {
        final List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex("id"));
        indexes.add(new QueryIndex(Arrays.asList("label","property_key"),QueryIndexType.SORTED));        
        return indexes;
    }
    
    public LinkedHashMap<String, String> buildDocumentFields(QueryEntity qe) {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.String");
        fields.put("type", "java.lang.String");        
        fields.put("created_at", "java.lang.Long");
        fields.put("updated_at", "java.lang.Long");
        fields.put("name", "java.lang.String");
        fields.put("title", "java.lang.String");
        fields.put("author", "java.lang.String");
        
        qe.setKeyFieldName("id");
        return fields;
    }
    
    public List<QueryIndex> buildDocumentIndexs() {
        final List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex("id"));
        indexes.add(new QueryIndex("updated_at"));        
        return indexes;
    }
}

