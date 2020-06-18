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
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import io.netty.util.internal.StringUtil;

public class IgniteBinaryCollection extends AbstractMongoCollection<Object> {

    private final IgniteCache<Object, BinaryObject> dataMap;
    
    public IgniteBinaryCollection(IgniteDatabase database, String collectionName, String idField, IgniteCache<Object, BinaryObject> dataMap) {
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
        if(dataMap.containsKey(key)) {
        	throw new DuplicateKeyError(this.getCollectionName(),"Document with key '" + key + "' already existed");
        }
        BinaryObject obj = this.documentToBinaryObject(document);
        dataMap.put(Missing.ofNullable(key), obj);
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
    	return this.binaryObjectToDocument(position,obj);
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
    	 BinaryObject obj = this.documentToBinaryObject(document);
    	 ScanQuery<Object, BinaryObject> scan = new ScanQuery<>(
    	            new IgniteBiPredicate<Object, BinaryObject>() {
    	                @Override public boolean apply(Object key, BinaryObject other) {
    	                    return obj.equals(other);
    	                }
    	            }
    	        );
    	 
    	QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
        for (Cache.Entry<Object, BinaryObject> entry : cursor.getAll()) {            
           return entry.getKey();           
        }
        return null;
    }



    @Override
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
            int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();
        
        ScanQuery<Object, BinaryObject> scan = new ScanQuery<>();
	 
		QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
		//Iterator<Cache.Entry<Object, BinaryObject>> it = cursor.iterator();
	    for (Cache.Entry<Object, BinaryObject> entry: cursor) {	 	    	
	    	Document document = this.binaryObjectToDocument(entry.getKey(),entry.getValue());
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

        dataMap.put(Missing.ofNullable(position), documentToBinaryObject(document));
        
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	// Get the data streamer reference and stream data.
    	//try (IgniteDataStreamer<Object, Document> stmr = Ignition.ignite().dataStreamer(dataMap.getName())) {    
    	//	stmr.receiver(StreamVisitor.from((key, val) -> {}));
    	//}
    	
    	 ScanQuery<Object, BinaryObject> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
    	//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(binaryObjectToDocument(entry.getKey(),entry.getValue()), entry.getKey()));		
         
    }
    
    public BinaryObject documentToBinaryObject(Document obj){	
    	Ignite ignite = ((IgniteDatabase) this.database).getIgnite();
    	
    	String typeName = (String)obj.get("_class");
    	
    	CacheConfiguration cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	if(StringUtil.isNullOrEmpty(typeName) && !cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		typeName = qeit.next().getValueType();    		
    	}
    	if(StringUtil.isNullOrEmpty(typeName)) {
    		typeName = dataMap.getName();
    	}
		BinaryObjectBuilder bb = ignite.binary().builder(typeName.toString());
		Set<Map.Entry<String,Object>> ents = obj.entrySet();
	    for(Map.Entry<String,Object> ent: ents){	    	
	    	String $key =  ent.getKey();
	    	Object $value = ent.getValue();
			try {
			
				if($value instanceof List){
					List $arr = (List)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Set){
					Set $arr = (Set)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Document){
					Document $arr = (Document)$value;
					//-$value = new HashMap<String,Object>($arr);
					$value = ($arr.asMap());
				}
				else if($value instanceof Map){
					Map $arr = (Map)$value;
					//$value = new HashMap($arr);
					//$value = ($arr);
				}
				Object bValue = ignite.binary().toBinary($value);
				bb.setField($key, bValue);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }	   
	    return bb.build();
	}
    
    public Document binaryObjectToDocument(Object key,BinaryObject obj){	    	
    	Document doc = new Document();	
    	if(key!=null) {
    		if(key instanceof BinaryObject){
				BinaryObject $arr = (BinaryObject)key;					
				key = $arr.deserialize();
			}	
    		doc.append(this.idField, key);
    	}
	    for(String field: obj.type().fieldNames()){	    	
	    	String $key =  field;
	    	Object $value = obj.field(field);
			try {
			
				if($value instanceof List){
					List $arr = (List)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Set){
					Set $arr = (Set)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Map){
					Map $arr = (Map)$value;
					//-$value = new HashMap<String,Object>($arr);
					$value = new Document($arr);
				}
				if($value instanceof BinaryObject){
					BinaryObject $arr = (BinaryObject)$value;					
					$value = binaryObjectToDocument(null,$arr);
				}				
				doc.append($key, $value);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    return doc;
	}    

}
