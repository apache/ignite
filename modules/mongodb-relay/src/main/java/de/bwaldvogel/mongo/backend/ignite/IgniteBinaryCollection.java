package de.bwaldvogel.mongo.backend.ignite;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryFieldEx;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ignite.util.BinaryObjectMatch;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import io.netty.util.internal.StringUtil;

import static de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil.*;

public class IgniteBinaryCollection extends AbstractMongoCollection<Object> {

	public final IgniteCache<Object, BinaryObject> dataMap;
	public long dataSize = 0;
    
    public IgniteBinaryCollection(IgniteDatabase database, String collectionName, String idField, IgniteCache<Object, BinaryObject> dataMap) {
        super(database, collectionName, idField);
        this.dataMap = dataMap;
        dataSize = dataMap.metrics().getCacheSize();
    }
    
    @Override
    protected void indexChanged(Index<Object> index,String op) {
    	for (Index<Object> idx : this.getIndexes()) {
			if (idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;
				igniteIndex.init(this.getCollectionName());
			}
    	}
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
    	dataSize+=sizeDelta;
    }

    @Override
    protected int getDataSize() {
    	int size = (int)dataMap.metrics().getCacheSize();
        return size;
    }


    @Override
    protected Object addDocumentInternal(Document document) {
        Object key = null;
        if (idField != null) {
            key = document.get(idField);
        } 
        
        if(key==null || key==Missing.getInstance()) {
            key = UUID.randomUUID();
        }
        T2<String,String> t2 = typeNameAndKeyField(this.dataMap,document);
    	String typeName = t2.get1();	
    	String keyField = t2.get2();
    	IgniteDatabase database = (IgniteDatabase)this.database;
        T2<Object,BinaryObject> obj = documentToBinaryObject(database.getIgnite().binary(),key,document,typeName,keyField);
       
        boolean rv = dataMap.putIfAbsent(Missing.ofNullable(obj.getKey()), obj.getValue());
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
    	BinaryObject obj = dataMap.get(position);
    	if(obj==null) return null;
    	return binaryObjectToDocument(position,obj,idField);
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
        
        IgniteBiPredicate<Object, BinaryObject> filter = new BinaryObjectMatch(query,this.idField);
        
        ScanQuery<Object, BinaryObject> scan = new ScanQuery<>(query.isEmpty()? null: filter);
	 
		QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
		//Iterator<Cache.Entry<Object, BinaryObject>> it = cursor.iterator();
	    for (Cache.Entry<Object, BinaryObject> entry: cursor) {	 	    	
	    	Document document = binaryObjectToDocument(entry.getKey(),entry.getValue(),this.idField);
	    	
	    	boolean rv = documentMatchesQuery(document, query);    		
	    	if (rv) {	    		
                matchedDocuments.add(document);
            }
	    	else {
	    		//-Assert.isTrue(rv,()->"match not insitense!");
	    	}
	    }

	    sortDocumentsInMemory(matchedDocuments, orderBy);
        return applySkipAndLimit(matchedDocuments, numberToSkip, numberToReturn);
    }
    
    @Override
    public void drop() {
        log.debug("Dropping collection {}", getFullName());
        dataMap.destroy();
    }

    @Override
    protected void handleUpdate(Object key, Document oldDocument,Document document) {
    	T2<String,String> t2 = typeNameAndKeyField(this.dataMap,document);
    	String typeName = t2.get1();    	
    	String keyField = t2.get2();    
    	IgniteDatabase database = (IgniteDatabase)this.database;
    	T2<Object,BinaryObject> obj = documentToBinaryObject(database.getIgnite().binary(),key,document,typeName,keyField);
        
        dataMap.put(Missing.ofNullable(obj.getKey()), obj.getValue());        
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	// Get the data streamer reference and stream data.
    	
    	
    	 ScanQuery<Object, BinaryObject> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
    	//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(binaryObjectToDocument(entry.getKey(),entry.getValue(),this.idField), entry.getKey()));		
         
    }
   
    public static String tableOfCache(String cacheName) {
		if(cacheName.startsWith("SQL_")) {
			int pos = cacheName.lastIndexOf('_',5);
			if(pos>0)
				return cacheName.substring(pos+1);
		}
		return cacheName;
	}
    
    public static T2<String,String> typeNameAndKeyField(IgniteCache<Object,BinaryObject> dataMap,Document obj) {
    	String typeName = (String)obj.get("_class");
    	String shortName = typeName;
    	if(!StringUtil.isNullOrEmpty(typeName)) {
    		int pos = typeName.lastIndexOf('.');
    		shortName = pos>0? typeName.substring(pos+1): typeName;
    	}    	
    	
    	String keyField = "_id";
    	CacheConfiguration<Object,BinaryObject> cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	if(!cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		while(qeit.hasNext()) {
	    		QueryEntity entity = qeit.next();   		
	    		keyField = entity.getKeyFieldName();
	    		if(StringUtil.isNullOrEmpty(typeName)) {
	        		typeName = entity.getValueType();
	        		break;
	        	}
	    		else if(typeName.equalsIgnoreCase(entity.getValueType()) || shortName.equalsIgnoreCase(entity.getTableName())){
	    			break;
	    		}
	    		else {
	    			typeName = entity.getValueType();
	    		}
    		}
    	}
    	if(StringUtil.isNullOrEmpty(typeName)) {
    		typeName = tableOfCache(dataMap.getName());
    	}    	
    	return new T2<>(typeName,keyField);
    }
    
    
}
