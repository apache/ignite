package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

public class IgniteCollection extends AbstractMongoCollection<Object> {

    private static final Logger log = LoggerFactory.getLogger(IgniteCollection.class);

    private final IgniteCache<Object, Document> dataMap;
    

    public IgniteCollection(String databaseName, String collectionName, String idField, IgniteCache<Object, Document> dataMap) {
        super(databaseName, collectionName, idField);
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

        Document previous = dataMap.getAndPut(Missing.ofNullable(key), document);
        Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
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
    	 ScanQuery<Object, Document> scan = new ScanQuery<>(
    	            new IgniteBiPredicate<Object, Document>() {
    	                @Override public boolean apply(Object key, Document other) {
    	                    return document.equals(other);
    	                }
    	            }
    	        );
    	 
    	QueryCursor<Cache.Entry<Object, Document>>  cursor = dataMap.query(scan);
        for (Cache.Entry<Object, Document> entry : cursor.getAll()) {            
           return entry.getKey();           
        }
        return null;
    }


    @Override
    protected Iterable<Document> matchDocuments(Document query, Iterable<Object> positions, Document orderBy, int numberToSkip, int numberToReturn) {

        List<Document> matchedDocuments = new ArrayList<>();

        for (Object position : positions) {
            Document document = getDocument(position);
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        sortDocumentsInMemory(matchedDocuments, orderBy);

        if (numberToSkip > 0) {
            matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    @Override
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
            int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();
        
        ScanQuery<Object, Document> scan = new ScanQuery<>(            
	        );
	 
		QueryCursor<Cache.Entry<Object, Document>>  cursor = dataMap.query(scan);
		//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
	    for (Cache.Entry<Object, Document> entry: cursor) {	    	
	    	Document document = entry.getValue();
	    	if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
	    }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == 1) {
                    // already sorted
                } else if (sortValue == -1) {
                    Collections.reverse(matchedDocuments);
                }
            } else {
                matchedDocuments.sort(new DocumentComparator(orderBy));
            }
        }

        if (numberToSkip > 0) {
            if (numberToSkip < matchedDocuments.size()) {
                matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
            } else {
                return Collections.emptyList();
            }
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    @Override
    protected void handleUpdate(Document document) {
        // noop
    	//add@byron
    	final Object key;
        if (idField != null) {
            key = Utils.getSubdocumentValue(document, idField);
        } else {
            key = UUID.randomUUID();
        }

        dataMap.put(Missing.ofNullable(key), document);
        
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	// Get the data streamer reference and stream data.
    	//try (IgniteDataStreamer<Object, Document> stmr = Ignition.ignite().dataStreamer(dataMap.getName())) {    
    	//	stmr.receiver(StreamVisitor.from((key, val) -> {}));
    	//}
    	
    	 ScanQuery<Object, Document> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, Document>>  cursor = dataMap.query(scan);
    	//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(entry.getValue(), entry.getKey()));		
         
    }

}
