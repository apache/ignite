package de.bwaldvogel.mongo.backend.ignite;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;
import static de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil.objectToDocument;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.cache.Cache;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.stream.StreamVisitor;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;



public class IgniteDatabase extends AbstractMongoDatabase<Object> {
	public static final String DEFAULT_DB_NAME = "default";
	public static final String SYS_DB_NAME = "admin";
	public static final String INDEX_DB_PREFIX = "INDEXES.";
	
    private Ignite mvStore;
    private IgniteBackend backend;
    
    final boolean isKeepBinary;
    final boolean isGlobal;
    
    public static String getCacheName(String databaseName,String collectionName) {
    	if(databaseName.equalsIgnoreCase(DEFAULT_DB_NAME)) {
    		return collectionName;
    	}
    	else {
    		return collectionName;
    	}    	
    }
    
    public static String getIndexCacheName(String databaseName,String collectionName,String indexName) {
    	return INDEX_DB_PREFIX  + collectionName + "_" + indexName;
    }

    public IgniteDatabase(String databaseName, IgniteBackend backend, Ignite mvStore,CursorRegistry cursorRegistry) {
        super(databaseName, cursorRegistry);
        this.mvStore = mvStore;
        this.backend = backend;
        this.isKeepBinary = backend.isKeepBinary();
        if(!backend.getCfg().isPartitioned()) {
        	isGlobal = true;
        }
        else {
        	isGlobal = false;
        }
        
        initializeNamespacesAndIndexes();        
    }
    
    @Override
    protected String extractCollectionNameFromNamespace(String namespace) {
        if(namespace.startsWith(databaseName+'.')){
        	return namespace.substring(databaseName.length() + 1);
        }
        return namespace;
    }
    
    @Override
    public MongoCollection<Object> resolveCollection(String collectionName, boolean throwIfNotFound) {        
        MongoCollection<Object> collection = super.resolveCollection(collectionName,false);       
        if (collection != null) {
            return collection;
        } else if(mvStore.cacheNames().contains(collectionName)) {
            return resolveOrCreateCollection(collectionName);
        } else {
        	return super.resolveCollection(collectionName, throwIfNotFound);
        }
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName,String indexName, List<IndexKey> keys, boolean sparse) { 
    	
    	if (keys.size()==1 && keys.get(0).getKey().equalsIgnoreCase(ID_FIELD)) {    		
    		IgniteBinaryCollection collection = (IgniteBinaryCollection)resolveCollection(collectionName,true);
        	return new PrimaryKeyIndex<Object>(indexName,collection.dataMap,keys,false);
    	}
    	if(keys.size()>0) {
    		IgniteEx ignite = (IgniteEx)mvStore;
    		IgniteBinaryCollection collection = (IgniteBinaryCollection)resolveCollection(collectionName,true);    		
    		return new IgniteUniqueIndex(ignite.context(), collection, indexName, keys, sparse);
    	}
    	return null;
    }
    
    @Override
    protected Index<Object> openOrCreateSecondaryIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
    	IgniteBinaryCollection collection = (IgniteBinaryCollection)resolveCollection(collectionName,true);
    	
    	if(keys.size()>0 && keys.get(0).isText() ) {
    		String indexType = (String)keys.get(0).textOptions().get("type");
    		if("knnVector".equalsIgnoreCase(indexType)) { // rnnVector
	    		IgniteEx ignite = (IgniteEx)mvStore;
	    		IgniteVectorIndex index = new IgniteVectorIndex(ignite.context(),collection,indexName,keys,sparse); 		
	    		return index;
    		}
    		else if("text".equalsIgnoreCase(indexType)) { // text
	    		IgniteEx ignite = (IgniteEx)mvStore;
	    		IgniteLuceneIndex index = new IgniteLuceneIndex(ignite.context(),collection,indexName,keys,sparse);    		
	    		return index;
    		}
    	}
    	if(keys.size()>0) {
    		IgniteEx ignite = (IgniteEx)mvStore;
    		IgniteLuceneIndex index = new IgniteLuceneIndex(ignite.context(),collection,indexName,keys,sparse);    		
    		return index;
    	}
    	return null;
     }

    @Override
    public void drop(Oplog oplog) {
        super.drop(oplog);
        
        List<String> maps = mvStore.cacheNames().stream()
            .filter(name -> 
            	!name.startsWith("system.")          
            )
            //.map(mvStore::openMap)
            .collect(Collectors.toList());

        for (String cacheName : maps) {
            mvStore.destroyCache(cacheName);
        }
    }
    
    void close() {
    	for( MongoCollection<Object> coll: collections.values()) {
			if(coll instanceof IgniteBinaryCollection) {
				IgniteBinaryCollection collection = (IgniteBinaryCollection) coll;
				collection.close();
			}					
		}
    }

    public static String indexName(List<IndexKey> keys) {
        Assert.notEmpty(keys, () -> "No keys");
        return keys.stream()
            .map(k -> k.getKey())
            .collect(Collectors.joining("_"));
    }
    
    @Override
    protected Iterable<String> listCollectionNamespaces() {    	
    	return mvStore.cacheNames().stream().filter(c-> !c.startsWith(INDEX_DB_PREFIX))
    		.map(n->databaseName+'.'+n)
    		.collect(Collectors.toList());
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, CollectionOptions options) {    	
    	MongoCollection<Object> coll = super.resolveCollection(collectionName, false);
		if(coll!=null) {
			return coll;
		}
        IgniteBackend backend = (IgniteBackend)this.backend;
        IgniteDatabase database = this;
    	String fullCollectionName = getCacheName(databaseName ,collectionName);
    	if(collectionName.equals(NAMESPACES_COLLECTION_NAME) || collectionName.equals(NAMESPACES_VIEWS_NAME) || collectionName.equals(INDEXES_COLLECTION_NAME)) {
    		if(!databaseName.equals("admin")) {
    			database = ((IgniteDatabase)backend.adminDatabase());
    			MongoCollection<Object> systemColl = database.resolveCollection(collectionName, false);
    			if(systemColl==null) {
    				systemColl = database.openOrCreateCollection(collectionName,options);
    			}
    			return systemColl;
    		}
    	}
        
    	Ignite mvStore = database.getIgnite();
        CacheConfiguration<Object, BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setName(fullCollectionName);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);        
        
        if(!this.isGlobal) {        	
	       
	        cfg.setCacheMode(CacheMode.PARTITIONED);	       
	        cfg.setBackups(backend.getCfg().getMdlStorageBackups());
	        
	        IgniteCache<Object, BinaryObject> dataMap = mvStore.getOrCreateCache(cfg).withKeepBinary();
	       
	        return new IgniteBinaryCollection(database, collectionName, options, this.cursorRegistry, dataMap);
        }
        else {
        	
	        cfg.setCacheMode(CacheMode.REPLICATED);
	        
	        IgniteCache<Object, BinaryObject> dataMap = mvStore.getOrCreateCache(cfg).withKeepBinary();
	        return new IgniteBinaryCollection(database, collectionName, options, this.cursorRegistry, dataMap);
        }
    }

    @Override
    protected long getStorageSize() {
    	DataRegionMetrics fileStore = mvStore.dataRegionMetrics(DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME);
        if (fileStore != null) {
            try {
                return fileStore.getTotalUsedSize();
            } catch (Exception e) {
                throw new RuntimeException("Failed to calculate filestore size", e);
            }
        } else {
            return 0;
        }
    }

    @Override
    protected long getFileSize() {
    	DataRegionMetrics fileStore = mvStore.dataRegionMetrics(DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME);
        if (fileStore != null) {
            try {
                return fileStore.getTotalUsedPages();
            } catch (Exception e) {
                throw new RuntimeException("Failed to calculate filestore size", e);
            }
        } else {
            return 0;
        }
    }

    @Override
    public void dropCollection(String collectionName,Oplog oplog) {
        super.dropCollection(collectionName,oplog);
        String fullCollectionName = getCacheName(databaseName ,collectionName);
        List<String> maps = mvStore.cacheNames().stream()
                .filter(name -> 
	                name.equals(fullCollectionName)  
	                ||  name.startsWith(getIndexCacheName(databaseName,collectionName,""))                 
                )              
                .collect(Collectors.toList());
       
        for (String cacheName : maps) {
        	if(!cacheName.startsWith("system.")) {
        		mvStore.destroyCache(cacheName);
        	}            
        }
        
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
    	
        if(collection.getNumIndexes()>1) {
        	super.moveCollection(oldDatabase, collection, newCollectionName);
        	return;
        }
        IgniteDatabase oldIgnitDb = (IgniteDatabase)oldDatabase;
        String oldName = getCacheName(databaseName ,collection.getCollectionName());
        String newName = getCacheName(collection.getDatabaseName(), newCollectionName);
        IgniteBinaryCollection coll = (IgniteBinaryCollection)collection;
        //MongoCollection oldColl = oldDatabase.resolveCollection(newCollectionName, true);
        
        IgniteCache<Object, BinaryObject> dataMap = coll.dataMap;
        CacheConfiguration<Object, BinaryObject> cfg = new CacheConfiguration<>(dataMap.getConfiguration(CacheConfiguration.class));
        cfg.setName(newName);
        IgniteCache<Object, BinaryObject> newDataMap = mvStore.getOrCreateCache(cfg).withKeepBinary();
       
        ScanQuery<Object, BinaryObject> scan = new ScanQuery<>(null);
   	 
		QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);		
	    for (Cache.Entry<Object, BinaryObject> entry: cursor) {	 	    	
	    	//-Document document = objectToDocument(entry.getKey(),entry.getValue(),coll.idField);	    	
	    	newDataMap.put(entry.getKey(), entry.getValue());
	    }
       
	    oldDatabase.unregisterCollection(collection.getCollectionName());
	    collection.renameTo(this, newCollectionName);
        //throw new UnsupportedOperationException();
    }
   
    
    public Ignite getIgnite() {
    	return this.mvStore;
    }	

}
