package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.cache.Cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiPredicate;

import com.google.common.collect.Sets;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.ComposeKeyValue;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ignite.util.BinaryObjectMatch;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.IllegalOperationException;
import de.bwaldvogel.mongo.exception.TypeMismatchException;
import io.netty.util.internal.StringUtil;

import static de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil.*;

public class IgniteBinaryCollection extends AbstractMongoCollection<Object> {

	public final IgniteCache<Object, BinaryObject> dataMap;	
	public final String idField;
	private boolean readOnly = false;
    
    public IgniteBinaryCollection(IgniteDatabase database, String collectionName, CollectionOptions options,
            CursorRegistry cursorRegistry, IgniteCache<Object, BinaryObject> dataMap) {
        super(database, collectionName,options,cursorRegistry);
        this.dataMap = dataMap;
        this.idField = options.getIdField();
        if(collectionName.startsWith("igfs-internal-")) { // igfs
        	this.readOnly = true;
        }
        if(collectionName.startsWith("wc_")) { // web-console
        	this.readOnly = true;
        }
    }
    

    @Override
    public void addIndex(Index<Object> index) {
    	if(index==null) return; //add@byron
    	super.addIndex(index);
    	indexChanged(index,"add");
    }    
   
    protected void indexChanged(Index<Object> index,String op) {
    	boolean firstLuneceIndex = true;
    	for (Index<Object> idx : this.getIndexes()) {
			if (idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;
				if(op.equals("reIndex"))
					igniteIndex.init(this);
				if(op.equals("add")) {
					igniteIndex.setFirstIndex(firstLuneceIndex);
					firstLuneceIndex = false;
				}
			}
			if (idx instanceof IgniteVectorIndex) {
				IgniteVectorIndex igniteIndex = (IgniteVectorIndex) idx;
				if(op.equals("reIndex"))
					igniteIndex.init(this);
			}
    	}
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
    	
    }
    
    protected boolean tracksDataSize() {
        return false;
    }

    @Override
    protected int getDataSize() {
    	int size = (int)dataMap.metrics().getCacheSize();
        return size;
    }


    @Override
    protected Object addDocumentInternal(Document document) {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}
        Object key = null;
        if (idField != null) {
            key = document.get(idField);
        }
        
        if(key==null || key==Missing.getInstance()) {
            key = UUID.randomUUID();
        }
        T3<String,String,String> t2 = typeNameAndKeyField(this.dataMap,document);
    	String typeName = t2.get2();	
    	String keyField = t2.get3();
    	IgniteDatabase database = (IgniteDatabase)this.getDatabase();
    	try {
    		key = toBinaryKey(key);
    		BinaryObject obj = documentToBinaryObject(database.getIgnite().binary(),typeName,document,idField);
    		boolean rv = dataMap.putIfAbsent(key, obj);
            if(!rv) {
            	throw new DuplicateKeyError(this.getCollectionName(),"Document with key '" + key + "' already existed");
            }
            //Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
            return key;
    	}
    	catch(BinaryObjectException e) {
    		throw new TypeMismatchException(e.getMessage());
    	}
    	catch(IgniteException e) {
    		throw new BadValueException(e.getMessage());
    	}
       
        
    }

    @Override
    public int count() {
    	if(this.getCollectionName().equals("system.namespaces")) {
    		IgniteDatabase database = (IgniteDatabase)this.getDatabase();
    		return ((List)database.listCollectionNamespaces()).size();
    	}
        return dataMap.size();
    }

    @Override
    protected Document getDocument(Object position) {
    	// position with score
    	if(position instanceof IdWithMeta) { // _key,_score,_indexValue
    		IdWithMeta idWithScore = (IdWithMeta)position;    		
    		position = idWithScore.key;    		
    		Object obj = dataMap.get(position);
        	if(obj==null) return null;
        	Document doc = objectToDocument(position,obj,idField);
        	Document meta = (Document) doc.computeIfAbsent("_meta", (k)-> idWithScore.meta);
        	if(meta!=null && idWithScore.meta!=null && idWithScore.meta!=meta) {
        		meta.putAll(idWithScore.meta);
        	}
        	
        	return doc;
    	}
    	Object obj = dataMap.get(position);
    	if(obj==null) return null;
    	return objectToDocument(position,obj,idField);
    }

    @Override
    protected void removeDocument(Object position) {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}
        boolean remove = dataMap.remove(position);
        if (!remove) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }

    @Override
    protected Object findDocumentPosition(Document document) {
    	 Object key = document.getOrDefault(this.idField, null);
    	 if(key!=null) {
    		 key = toBinaryKey(key);
    		 return key;
    	 }    	 
         return null;
    }
    
    @Override
    protected QueryResult queryDocuments(Document query, Document orderBy, int numberToSkip, int limit, int batchSize,
            Document fieldSelector) {
		query = Utils.normalizeDocument(query);
		
		List<Iterable<Object>> indexResult = new ArrayList<>();
		
		for (Index<Object> index : getIndexes()) {
			if (index.canHandle(query)) {
				Iterable<Object> positions = index.getPositions(query);
				if(index.isUnique() || (positions instanceof List && ((List)positions).size()==0)) {
					return matchDocuments(query, positions, orderBy, numberToSkip, limit, batchSize, fieldSelector);
				}
				else {
					indexResult.add(positions);
				}
			}
		}
		if(indexResult.size()==1) {
			return matchDocuments(query, indexResult.get(0), orderBy, numberToSkip, limit, batchSize, fieldSelector);
		}
		else if(indexResult.size()>1) {			
			final LinkedHashMap<Object,Object> resultMap = new LinkedHashMap<>();
			HashSet<Object> ids = new HashSet<>();
			int n = 0;
			for(Iterable<Object> positions: indexResult) {
				for(Object position: positions) {
					Object rawPosition = position;
					if(position instanceof IdWithMeta) {
						IdWithMeta idWithScore = (IdWithMeta)position;    		
			    		rawPosition = idWithScore.key;				    		
					}
					
					if(n==0) {
						resultMap.put(rawPosition, position);
					}
					else {
						Object originPosition = resultMap.get(rawPosition);
						if(originPosition!=null) {
							if(position instanceof IdWithMeta) {
								IdWithMeta idWithScore = (IdWithMeta)position;  
								if(originPosition instanceof IdWithMeta) {
									IdWithMeta idWithScoreOrigin = (IdWithMeta)originPosition;
									if(idWithScoreOrigin.meta!=null) {
										if(idWithScore.meta!=null)
											idWithScoreOrigin.meta.putAll(idWithScore.meta);
									}
									else {
										resultMap.put(rawPosition, position);
									}
								}
								else {
									resultMap.put(rawPosition, position);
								}
								
							}
							ids.add(rawPosition);
						}
					}						
				}					
				
				if(n>0) {
					Set<Object> removes = Sets.difference(resultMap.keySet(),ids);
					Sets.newCopyOnWriteArraySet(removes).forEach(id->{
						resultMap.remove(id);
					});
					ids.clear();
				}					
				n++;
			}
			
			return matchDocuments(query, resultMap.values(), orderBy, numberToSkip, limit, batchSize, fieldSelector);
		}
		
		return matchDocuments(query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
	}


    @Override
	protected QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn,
			int batchSize, Document fieldSelector) {
    	Iterable<Document> list = this.matchDocuments(query, orderBy, numberToSkip, numberToReturn);
    	Stream<Document> documentStream = StreamSupport.stream(list.spliterator(), false);
    	return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, numberToReturn, batchSize, fieldSelector);
		
	}
    
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();
        
        IgniteBiPredicate<Object, Object> filter = new BinaryObjectMatch(query,this.idField);
        
        ScanQuery<Object, Object> scan = new ScanQuery<>(query.isEmpty()? null: filter);
	 
		QueryCursor<Cache.Entry<Object, Object>>  cursor = dataMap.query(scan);
		//Iterator<Cache.Entry<Object, BinaryObject>> it = cursor.iterator();
	    for (Cache.Entry<Object, Object> entry: cursor) {	 	    	
	    	Document document = objectToDocument(entry.getKey(),entry.getValue(),this.idField);	    	
	    	matchedDocuments.add(document);
	    }
	    
	    //-sortDocumentsInMemory(matchedDocuments, orderBy);
	    return matchedDocuments;
    }
    
    protected void sortDocumentsInMemory(List<Document> documents, Document orderBy) {
        DocumentComparator documentComparator = deriveComparator(orderBy);
        if (documentComparator != null) {
            documents.sort(documentComparator);
        } else if (isNaturalDescending(orderBy)) {
            Collections.reverse(documents);
        }
    }

    
    @Override
    public void drop() {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}
    	dataMap.clear();
    	if(!this.getCollectionName().startsWith("system.")) {    		
    		dataMap.destroy();
    	}
    }
    
    public void close() {
    	for(Index<Object> idx: this.getIndexes()) {
    		if (idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;				
				igniteIndex.close();
			}
			if (idx instanceof IgniteVectorIndex) {
				IgniteVectorIndex igniteIndex = (IgniteVectorIndex) idx;				
				igniteIndex.close();
			}
    	}
    }

    @Override
    protected void handleUpdate(Object key, Document oldDocument,Document document) {
    	if(readOnly) {
    		throw new IllegalOperationException("This collection is read only!");
    	}
    	T3<String,String,String> t2 = typeNameAndKeyField(this.dataMap,document);
    	String typeName = t2.get2();    	
    	String keyField = t2.get3();
    	IgniteDatabase database = (IgniteDatabase)this.getDatabase();
    	BinaryObject obj = documentToBinaryObject(database.getIgnite().binary(),typeName,document,idField);
        
        dataMap.put(toBinaryKey(key), obj);
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	// Get the data streamer reference and stream data.    	
    	
    	 ScanQuery<Object, BinaryObject> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
    	//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(objectToDocument(entry.getKey(),entry.getValue(),this.idField), entry.getKey()));		
         
    }
   
    public static String tableOfCache(String cacheName) {
		if(cacheName.startsWith("SQL_")) {
			int pos = cacheName.lastIndexOf('_',5);
			if(pos>0)
				return cacheName.substring(pos+1);
		}
		return cacheName;
	}
    
    public T3<String,String,String> typeNameAndKeyField(IgniteCache<Object,BinaryObject> dataMap,Document obj) {
    	String typeName = (String)obj.get("_class");
    	String shortName = typeName;
    	if(!StringUtil.isNullOrEmpty(typeName)) {
    		int pos = typeName.lastIndexOf('.');
    		shortName = pos>0? typeName.substring(pos+1): typeName;
    	}    	
    	
    	String keyField = idField;
    	CacheConfiguration<Object,BinaryObject> cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	String schema = cfg.getSqlSchema();
    	if(schema==null) {
    		schema = cfg.getName();
    	}
    	if(!cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		while(qeit.hasNext()) {
	    		QueryEntity entity = qeit.next();   		
	    		keyField = entity.getKeyFieldName()!=null?entity.getKeyFieldName():keyField;
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
    	return new T3<>(schema,typeName,keyField);
    }    
    
}
