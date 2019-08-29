package de.bwaldvogel.mongo.backend.ignite;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class IgniteDatabase extends AbstractMongoDatabase<Object> {
   
	public static final String INDEX_FLAG = "_INDEX_";
    private Ignite mvStore;
    
    private boolean isKeepBinary = true;

    public IgniteDatabase(String databaseName, IgniteBackend backend, Ignite mvStore) {
        super(databaseName, backend);
        this.mvStore = mvStore;
        this.isKeepBinary = backend.isKeepBinary();
        initializeNamespacesAndIndexes();
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName, List<IndexKey> keys, boolean sparse) {    	
    	if (keys.size()==1 && keys.get(0).getKey().equalsIgnoreCase(ID_FIELD)) {
    		return null;
    	}
    	if(!isKeepBinary) {
    		IgniteCache<KeyValue, Object> mvMap = mvStore.getOrCreateCache(databaseName + "." + collectionName + "_" + indexName(keys)+INDEX_FLAG);
    		return new IgniteUniqueIndex(mvMap, keys, sparse);
    	}
    	return null;
    }

    @Override
    public void drop() {
        super.drop();

        List<String> maps = mvStore.cacheNames().stream()
            .filter(name -> name.startsWith(databaseName + ".")               
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
        String fullCollectionName = databaseName + "." + collectionName;
        if(this.isKeepBinary) {
	        CacheConfiguration<Object, BinaryObject> cfg = new CacheConfiguration<>();
	
	        cfg.setCacheMode(CacheMode.PARTITIONED);
	        cfg.setName(fullCollectionName);
	        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);     
	      
	        
	        IgniteCache<Object, BinaryObject> dataMap = mvStore.getOrCreateCache(cfg);
	        dataMap = dataMap.withKeepBinary();
	        return new IgniteBinaryCollection(databaseName, collectionName, idField, dataMap);
        }
        else {
        	
        	CacheConfiguration<Object, Document> cfg = new CacheConfiguration<>();
        	
	        cfg.setCacheMode(CacheMode.PARTITIONED);
	        cfg.setName(fullCollectionName);
	        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);  
	        
        	IgniteCache<Object, Document> dataMap = mvStore.getOrCreateCache(cfg);
 	        
 	        return new IgniteCollection(databaseName, collectionName, idField, dataMap);
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
        return getStorageSize();
    }

    @Override
    public void dropCollection(String collectionName) {
        super.dropCollection(collectionName);
        String fullCollectionName = getDatabaseName() + "." + collectionName;
        //IgniteCache<Object, Document> dataMap = mvStore.cache(fullCollectionName);
        //dataMap.destroy();
        mvStore.destroyCache(fullCollectionName);
        
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
        super.moveCollection(oldDatabase, collection, newCollectionName);
        String fullCollectionName = collection.getFullName();
        String newFullName = collection.getDatabaseName() + "." + newCollectionName;
        IgniteCache<Object, Document> dataMap = mvStore.cache(fullCollectionName);
      
        //mvStore.getOrCreateCache(newFullName);
        //mvStore.renameMap(dataMap, DATABASES_PREFIX + newFullName);       
       
        throw new UnsupportedOperationException();
    }

}
