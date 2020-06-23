package de.bwaldvogel.mongo.backend.ignite;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.stream.StreamVisitor;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.PrimaryKeyIndex;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class IgniteDatabase extends AbstractMongoDatabase<Object> {
	public static final String DEFAULT_DB_NAME = "default";
	public static final String SYS_DB_NAME = "sys";
	public static final String INDEX_DB_PREFIX = "INDEX_SYS_";
    private Ignite mvStore;
    
    boolean isKeepBinary = true;
    
    public static String getCacheName(String databaseName,String collectionName) {
    	if(databaseName.equalsIgnoreCase(DEFAULT_DB_NAME)) {
    		return collectionName;
    	}
    	else {
    		return collectionName;
    	}
    }

    public IgniteDatabase(String databaseName, IgniteBackend backend, Ignite mvStore) {
        super(databaseName, backend);
        this.mvStore = mvStore;
        this.isKeepBinary = backend.isKeepBinary();
        initializeNamespacesAndIndexes();
        
        for (String mapName : mvStore.cacheNames()) {
            if (!mapName.startsWith(IgniteDatabase.INDEX_DB_PREFIX)) {
               String fullName = mapName;//.substring(IgniteDatabase.INDEX_PREFIX.length());
               if(mapName.indexOf('.')>0) {
            	   String databaseName2 = Utils.firstFragment(fullName);
            	   if(databaseName2.equalsIgnoreCase(databaseName)) {
   	                	try {
		   	                Document doc = new Document("name",fullName);
		   	                
		   	                String collectionName = extractCollectionNameFromNamespace(fullName);
		   	                MongoCollection<Object> collection = openOrCreateCollection(collectionName, ID_FIELD);
		   	                collections.put(collectionName, collection);
		                 
		   	                this.namespaces.addDocument(doc);
   	                	}
   	                	catch(MongoServerError e) {
   	                		//
   	                	}
                  }
               }
               else {
            	   try {
	            	 Document doc = new Document("name",fullName);
	  	             
	  	               
	  	             String collectionName = extractCollectionNameFromNamespace(fullName);
	  	             MongoCollection<Object> collection = openOrCreateCollection(collectionName, ID_FIELD);
	                 collections.put(collectionName, collection);
	                 
	                 this.namespaces.addDocument(doc);
	                 log.debug("opened collection '{}'", collectionName);
	               }
	               catch(MongoServerError e) {
	              		//
	               }
               }
              
            }
        }
    }
    
    

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName,String indexName, List<IndexKey> keys, boolean sparse) {    	
    	if (keys.size()==1 && keys.get(0).getKey().equalsIgnoreCase(ID_FIELD)) {
    		//return null; //不创建主键索引 add@byron
    		MongoCollection<Object> collection = resolveCollection(collectionName,true);
        	return new PrimaryKeyIndex<Object>(indexName,collection,keys,sparse);
    	}
    	if(!isKeepBinary || isKeepBinary) {
    		CacheConfiguration<KeyValue, Object> cfg = new CacheConfiguration<>();        	
	        cfg.setCacheMode(CacheMode.PARTITIONED);
	        cfg.setName(INDEX_DB_PREFIX+databaseName + "." + collectionName + "_" + indexName(keys));
	        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);  
    		IgniteCache<KeyValue, Object> mvMap = mvStore.getOrCreateCache(cfg);
    		return new IgniteUniqueIndex(mvMap, indexName,keys, sparse);
    	}
    	return null;
    }
    
    
    protected Index<Object> openOrCreateIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
    	if(!isKeepBinary || isKeepBinary) {
    		IgniteEx ignite = (IgniteEx)mvStore;
    		IgniteLuceneIndex index = new IgniteLuceneIndex(ignite.context(),collectionName,indexName,keys,sparse);    		
    		return index;
    	}
    	return null;
     }

    @Override
    public void drop() {
        super.drop();
        
        List<String> maps = mvStore.cacheNames().stream()
            .filter(name -> name.startsWith(databaseName + ".")  ||  name.startsWith(INDEX_DB_PREFIX + databaseName + ".")          
            )
            //.map(mvStore::openMap)
            .collect(Collectors.toList());

        for (String cacheName : maps) {
            mvStore.destroyCache(cacheName);
        }
    }

    static String indexName(List<IndexKey> keys) {
        Assert.notEmpty(keys, () -> "No keys");
        return keys.stream()
            .map(k -> k.getKey() + "." + (k.isAscending() ? "ASC" : "DESC"))
            .collect(Collectors.joining("_"));
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, String idField) {
        String fullCollectionName = getCacheName(databaseName ,collectionName);
        if(this.isKeepBinary) {
        	
	        CacheConfiguration<Object, BinaryObject> cfg = new CacheConfiguration<>();
	
	        cfg.setCacheMode(CacheMode.PARTITIONED);
	        cfg.setName(fullCollectionName);
	        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);     
	      
	        
	        IgniteCache<Object, BinaryObject> dataMap = mvStore.getOrCreateCache(cfg);
	        dataMap = dataMap.withKeepBinary();
	        return new IgniteBinaryCollection(this, collectionName, idField, dataMap);
        }
        else {
        	
        	CacheConfiguration<Object, Document> cfg = new CacheConfiguration<>();
        	
	        cfg.setCacheMode(CacheMode.PARTITIONED);
	        cfg.setName(fullCollectionName);
	        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);  
	        
        	IgniteCache<Object, Document> dataMap = mvStore.getOrCreateCache(cfg);
 	        
 	        return new IgniteCollection(this, collectionName, idField, dataMap);
        }
    }

    @Override
    protected long getStorageSize() {
    	DataStorageMetrics fileStore = mvStore.dataStorageMetrics();
        if (fileStore != null) {
            try {
                return fileStore.getStorageSize();
            } catch (Exception e) {
                throw new RuntimeException("Failed to calculate filestore size", e);
            }
        } else {
            return 0;
        }
    }

    @Override
    protected long getFileSize() {
    	DataStorageMetrics fileStore = mvStore.dataStorageMetrics();
        if (fileStore != null) {
            try {
                return fileStore.getTotalAllocatedSize();
            } catch (Exception e) {
                throw new RuntimeException("Failed to calculate filestore size", e);
            }
        } else {
            return 0;
        }
    }

    @Override
    public void dropCollection(String collectionName) {
        super.dropCollection(collectionName);
        String fullCollectionName = getCacheName(databaseName ,collectionName);
        List<String> maps = mvStore.cacheNames().stream()
                .filter(name -> name.equals(fullCollectionName)  ||  name.startsWith(INDEX_DB_PREFIX + databaseName + "."+collectionName)          
                )              
                .collect(Collectors.toList());
       
        for (String cacheName : maps) {
            mvStore.destroyCache(cacheName);
        }
        
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
        super.moveCollection(oldDatabase, collection, newCollectionName);
        String fullCollectionName = getCacheName(databaseName ,collection.getCollectionName());
        String newFullName = getCacheName(collection.getDatabaseName(), newCollectionName);
        IgniteCache<Object, Object> dataMap = mvStore.cache(fullCollectionName);
        CacheConfiguration cfg = dataMap.getConfiguration(CacheConfiguration.class);
        cfg.setName(newFullName);
        IgniteCache<Object, Object> newDataMap = mvStore.getOrCreateCache(cfg);
        //mvStore.renameMap(dataMap, DATABASES_PREFIX + newFullName);       
       
	      try (IgniteDataStreamer<Object, Object> stmr = mvStore.dataStreamer(fullCollectionName)) {    
	    		stmr.receiver(StreamVisitor.from((key, val) -> {
	    			newDataMap.put(key, val);
	    		}));
	      }
       
        //throw new UnsupportedOperationException();
    }
    
    public Ignite getIgnite() {
    	return this.mvStore;
    }

}
