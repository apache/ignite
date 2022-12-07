package de.kp.works.ignite;


import org.apache.ignite.configuration.*;
import org.apache.ignite.binary.*;
import org.apache.ignite.*;

import de.kp.works.ignite.graph.*;
import org.apache.ignite.cache.*;
import java.util.*;

public final class IgniteUtil{
    public static final IgniteUtil MODULE$ = new IgniteUtil();
    
    
    public void createCacheIfNotExists(final Ignite ignite, final String table, final CacheConfiguration<String, BinaryObject> cfg) {
        final boolean exists = ignite.cacheNames().contains(table);
        if (!exists) {
            ignite.createCache((CacheConfiguration)cfg);
        }
    }
    
    public IgniteCache<String, BinaryObject> getOrCreateCache(final Ignite ignite, final String table, final String namespace) {
        final boolean exists = ignite.cacheNames().contains(table);
        return (IgniteCache<String, BinaryObject>)(exists ? ignite.cache(table) : this.createCache(ignite, table, namespace));
    }
    
    public IgniteCache<String, BinaryObject> createCache(final Ignite ignite, final String table, final String namespace) {
        return this.createCache(ignite, table, namespace, CacheMode.REPLICATED);
    }
    
    public IgniteCache<String, BinaryObject> createCache(final Ignite ignite, final String table, final String namespace, final CacheMode cacheMode) {
        final String string = new StringBuilder().append((Object)namespace).append((Object)"_").append((Object)"edges").toString();
        ElementType elementType = null;
        Label_0137: {
            Label_0054: {
                if (table == null) {
                    if (string != null) {
                        break Label_0054;
                    }
                }
                else if (!table.equals(string)) {
                    break Label_0054;
                }
                elementType = ElementType.EDGE;
                break Label_0137;
            }
            final String string2 = new StringBuilder().append((Object)namespace).append((Object)"_").append((Object)"vertices").toString();
            Label_0108: {
                if (table == null) {
                    if (string2 != null) {
                        break Label_0108;
                    }
                }
                else if (!table.equals(string2)) {
                    break Label_0108;
                }
                elementType = ElementType.VERTEX;
                break Label_0137;
            }
            if (!table.startsWith(new StringBuilder().append((Object)namespace).append((Object)"_").toString())) {
                throw new RuntimeException(new StringBuilder().append((Object)"Table '").append((Object)table).append((Object)"' is not supported.").toString());
            }
            elementType = ElementType.DOCUMENT;
        }
        final ElementType tableType = elementType;
        final CacheConfiguration cfg = this.createCacheCfg(table, tableType, cacheMode);
        return (IgniteCache<String, BinaryObject>)ignite.createCache(cfg);
    }
    
    public CacheConfiguration<String, BinaryObject> createCacheCfg(final String table, final ElementType tableType, final CacheMode cacheMode) {
        final QueryEntity qe = this.buildQueryEntity(table, tableType);
        final ArrayList qes = new ArrayList();
        qes.add(qe);
        final CacheConfiguration cfg = new CacheConfiguration();
        cfg.setName(table);
        cfg.setStoreKeepBinary(false);
        cfg.setIndexedTypes(new Class[] { String.class, BinaryObject.class });
        cfg.setCacheMode(cacheMode);
        cfg.setQueryEntities((Collection)qes);
        return (CacheConfiguration<String, BinaryObject>)cfg;
    }
    
    public QueryEntity buildQueryEntity(final String table, final ElementType elementType) {
        final QueryEntity qe = new QueryEntity();
        qe.setKeyType("java.lang.String");
        qe.setValueType(table);
        final ElementType edge = ElementType.EDGE;
        Label_0059: {
            if (elementType == null) {
                if (edge != null) {
                    break Label_0059;
                }
            }
            else if (!elementType.equals(edge)) {
                break Label_0059;
            }
            qe.setFields((LinkedHashMap)this.buildEdgeFields());
            return qe;
        }
        final ElementType vertex = ElementType.VERTEX;
        Label_0097: {
            if (elementType == null) {
                if (vertex != null) {
                    break Label_0097;
                }
            }
            else if (!elementType.equals(vertex)) {
                break Label_0097;
            }
            qe.setFields((LinkedHashMap)this.buildVertexFields());
            return qe;
        }
        final ElementType document = ElementType.DOCUMENT;
        if (elementType == null) {
            if (document != null) {
                throw new RuntimeException(new StringBuilder().append((Object)"Table '").append((Object)table).append((Object)"' is not supported.").toString());
            }
        }
        else if (!elementType.equals(document)) {
            throw new RuntimeException(new StringBuilder().append((Object)"Table '").append((Object)table).append((Object)"' is not supported.").toString());
        }
        qe.setFields((LinkedHashMap)this.buildDocumentFields());
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
        return (LinkedHashMap<String, String>)fields;
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
        return (LinkedHashMap<String, String>)fields;
    }
    
    public LinkedHashMap<String, String> buildDocumentFields() {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.String");
        fields.put("id_type", "java.lang.String");
        fields.put("label", "java.lang.String");
        fields.put("created_at", "java.lang.Long");
        fields.put("updated_at", "java.lang.Long");
        fields.put("name", "java.lang.String");
        fields.put("title", "java.lang.String");
        fields.put("uid", "java.lang.String");
        return (LinkedHashMap<String, String>)fields;
    }
}

