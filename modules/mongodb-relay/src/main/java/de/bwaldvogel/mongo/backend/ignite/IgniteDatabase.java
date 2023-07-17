package de.bwaldvogel.mongo.backend.ignite;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
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
        mvStore.binary().registerClass(id.LongID.class);
        mvStore.binary().registerClass(id.StringID.class);
    }
    
    @Override
    protected String extractCollectionNameFromNamespace(String namespace) {
        if(StringUtils.startsWith(namespace, databaseName+'.')){
        	return namespace.substring(databaseName.length() + 1);
        }
        return namespace;
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName,String indexName, List<IndexKey> keys, boolean sparse) { 
    	IgniteBackend backend = (IgniteBackend)this.backend;
    	if (keys.size()==1 && keys.get(0).getKey().equalsIgnoreCase(ID_FIELD)) {
    		//return null; //不创建主键索引 add@byron
    		IgniteBinaryCollection collection = (IgniteBinaryCollection)resolveCollection(collectionName,true);
        	return new PrimaryKeyIndex<Object>(indexName,collection.dataMap,keys,false);
    	}
    	if(keys.size()>0) {
    		CacheConfiguration<KeyValue, Object> cfg = new CacheConfiguration<>();        	
	        cfg.setCacheMode(CacheMode.PARTITIONED);
	        cfg.setName(getIndexCacheName(this.databaseName,collectionName,indexName(keys)));
	        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC); 
	        cfg.setBackups(backend.getCfg().getMdlDescStorageBackups());
    		IgniteCache<KeyValue, Object> mvMap = mvStore.getOrCreateCache(cfg);
    		return new IgniteUniqueIndex(mvMap, indexName, keys, sparse);
    	}
    	return null;
    }
    
    @Override
    protected Index<Object> openOrCreateSecondaryIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
    	if(keys.size() ==1 && keys.get(0).isText() ) {
    		String indexType = (String)keys.get(0).textOptions().get("type");
    		if("text".equalsIgnoreCase(indexType)) { // rnnVector
	    		IgniteEx ignite = (IgniteEx)mvStore;
	    		IgniteVectorIndex index = new IgniteVectorIndex(ignite.context(),collectionName,indexName,keys,sparse);    		
	    		return index;
    		}
    	}
    	if(keys.size()>0) {
    		IgniteEx ignite = (IgniteEx)mvStore;
    		IgniteLuceneIndex index = new IgniteLuceneIndex(ignite.context(),collectionName,indexName,keys,sparse);    		
    		return index;
    	}
    	return null;
     }

    @Override
    public void drop(Oplog oplog) {
        super.drop(oplog);
        
        List<String> maps = mvStore.cacheNames().stream()
            .filter(name -> !name.startsWith("system.")          
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
            .map(k -> k.getKey())
            .collect(Collectors.joining("_"));
    }
    
    @Override
    protected Iterable<String> listCollectionNamespaces() {    	
    	return mvStore.cacheNames();
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, CollectionOptions options) {    	
        String fullCollectionName = getCacheName(databaseName ,collectionName);
        
        CacheConfiguration<Object, BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setName(fullCollectionName);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        IgniteBackend backend = (IgniteBackend)this.backend;
        if(!this.isGlobal) {        	
	       
	        cfg.setCacheMode(CacheMode.PARTITIONED);	       
	        cfg.setBackups(backend.getCfg().getMdlStorageBackups());
	        
	        IgniteCache<Object, BinaryObject> dataMap = mvStore.getOrCreateCache(cfg).withKeepBinary();
	       
	        return new IgniteBinaryCollection(this, collectionName, options, this.cursorRegistry, dataMap);
        }
        else {
        	
	        cfg.setCacheMode(CacheMode.REPLICATED);
	        
	        IgniteCache<Object, BinaryObject> dataMap = mvStore.getOrCreateCache(cfg).withKeepBinary();
	        return new IgniteBinaryCollection(this, collectionName, options, this.cursorRegistry, dataMap);
        }
    }

    @Override
    protected long getStorageSize() {
    	DataRegionMetrics fileStore = mvStore.dataRegionMetrics(DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME);
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
                .filter(name -> name.equals(fullCollectionName)  ||  name.startsWith(getIndexCacheName(databaseName,collectionName,""))          
                )              
                .collect(Collectors.toList());
       
        for (String cacheName : maps) {
            mvStore.destroyCache(cacheName);
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
        //MongoCollection newColl = this.resolveCollection(newCollectionName, true);
        //MongoCollection oldColl = oldDatabase.resolveCollection(newCollectionName, true);
        
        IgniteCache<Object, Object> dataMap = oldIgnitDb.mvStore.cache(oldName);
        CacheConfiguration cfg = dataMap.getConfiguration(CacheConfiguration.class);
        cfg.setName(newName);
        IgniteCache<Object, Object> newDataMap = mvStore.getOrCreateCache(cfg);
       
       
	    try (IgniteDataStreamer<Object, Object> stmr = oldIgnitDb.mvStore.dataStreamer(oldName)) {    
			stmr.receiver(StreamVisitor.from((key, val) -> {
				newDataMap.put(key, val);	    			
			}));
	    }
       
	    oldDatabase.unregisterCollection(collection.getCollectionName());
	    collection.renameTo(this, newCollectionName);
        //throw new UnsupportedOperationException();
    }
   
    
    public Ignite getIgnite() {
    	return this.mvStore;
    }	

}
