package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamVisitor;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import io.netty.util.internal.StringUtil;

public class IgniteCollection extends AbstractMongoCollection<Object> {

    private static final Logger log = LoggerFactory.getLogger(IgniteCollection.class);

    public final IgniteCache<Object, Document> dataMap;
    
    HashMap<String,FieldType> fields = new HashMap<>();
    
    public IgniteCollection(IgniteDatabase database, String collectionName, String idField, IgniteCache<Object, Document> dataMap) {
        super(database, collectionName, idField);
        this.dataMap = dataMap;
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        
    }

    @Override
    protected int getDataSize() {
    	int size = (int)dataMap.metrics().getCacheSize();
        return size;
    }


    @Override
    protected Object addDocumentInternal(Document document) {
        final Object key;
        if (idField != null) {
            key = Utils.getSubdocumentValue(document, idField);
        } else {
            key = UUID.randomUUID();
        }
        
        boolean rv = dataMap.putIfAbsent(Missing.ofNullable(key), document);
        if(!rv) {
        	throw new DuplicateKeyError(this.getCollectionName(),"Document with key '" + key + "' already existed");
        }
        //Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
        return key;
    }

    @Override
    public int count() {
        return dataMap.size();
    }

    @Override
    protected Document getDocument(Object position) {
        return dataMap.get(position);
    }

    @Override
    protected void removeDocument(Object position) {
        boolean remove = dataMap.remove(position);
        if (!remove) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }
	
    @Override
    protected Object findDocumentPosition(Document document) {
    	 Object key = document.getOrDefault(this.idField, null);
    	 if(key!=null) {
    		 return key;
    	 }    	 
         return null;
    }



    @Override
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
            int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();
        
        ScanQuery<Object, Document> scan = new ScanQuery<>();
	 
		QueryCursor<Cache.Entry<Object, Document>>  cursor = dataMap.query(scan);
		//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
	    for (Cache.Entry<Object, Document> entry: cursor) {	    	
	    	Document document = entry.getValue();
	    	if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
	    }


        sortDocumentsInMemory(matchedDocuments, orderBy);
        return applySkipAndLimit(matchedDocuments, numberToSkip, numberToReturn);

       
    }

    @Override
    protected void handleUpdate(Object position, Document oldDocument,Document document) {
        // noop   	

        dataMap.put(Missing.ofNullable(position), document);
        
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	
    	 ScanQuery<Object, Document> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, Document>>  cursor = dataMap.query(scan);
    	//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(entry.getValue(), entry.getKey()));		
         
    }    
    
    public static String tableOfCache(String cacheName) {
		if(cacheName.startsWith("SQL_")) {
			int pos = cacheName.lastIndexOf('_',5);
			if(pos>0)
				return cacheName.substring(pos+1);
		}
		return cacheName;
	}
    
    public static T2<String,String> typeNameAndKeyField(IgniteCache<?,?> dataMap,Document obj) {
    	String typeName = (String)obj.get("_class");    	
    	String keyField = "id";
    	CacheConfiguration cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	if(!cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		QueryEntity entity = qeit.next();   		
    		keyField = entity.getKeyFieldName();
    		if(StringUtil.isNullOrEmpty(typeName)) {
        		typeName = entity.getValueType();
        	}	
    	}
    	if(StringUtil.isNullOrEmpty(typeName)) {
    		typeName = tableOfCache(dataMap.getName());
    	}	
    	
    	return new T2(typeName,keyField);
    }
    
    public HashMap<String,FieldType> fields(){
    	if(fields.size()>0) return fields;
    	
		for(Index<Object> idx: this.getIndexes()) {
			if(idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;
				if(fields.isEmpty()) {
					igniteIndex.setFirstIndex(true);
				}
				else {
					igniteIndex.setFirstIndex(false);
				}
				for(IndexKey ik: idx.getKeys()) {
					fields.put(ik.getKey(), TextField.TYPE_NOT_STORED);
				}
			}
		}
		return fields;
    }
}
