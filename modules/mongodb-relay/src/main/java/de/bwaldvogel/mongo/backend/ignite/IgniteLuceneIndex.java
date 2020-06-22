package de.bwaldvogel.mongo.backend.ignite;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import javax.cache.Cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.FullTextLucene;
import org.apache.ignite.cache.LuceneIndexAccess;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;


import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.QueryOperator;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.ignite.IgniteUniqueIndex.EntrySet;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;


public class IgniteLuceneIndex extends Index<Object>{
	
	private final String cacheName;
	
	private IgniteH2Indexing idxing;
	
	private LuceneIndexAccess indexAccess;
	
	private final GridKernalContext ctx;
	
	private GridBinaryMarshaller marshaller;
	
	private boolean isFirstIndex = false;
	
	private long docCount = 0;
	 
	 /** */
    private String[] idxdFields = null;   
    
	protected IgniteLuceneIndex(GridKernalContext ctx,String collectionName, String name, List<IndexKey> keys, boolean sparse) {
		super(name, keys, sparse);
		this.ctx = ctx;
		this.cacheName = collectionName;		
		init(collectionName);
		     	 
	}

	
	public void init(String cacheName) {
		if(indexAccess==null) {
			try {
				indexAccess = LuceneIndexAccess.getIndexAccess(ctx, cacheName);
				
				marshaller = PlatformUtils.marshaller();
				String typeName = IgniteCollection.tableOfCache(cacheName);
				Map<String, FieldType> fields = indexAccess.fields(typeName);
		    	for(IndexKey ik: this.getKeys()) {
					fields.put(ik.getKey(), TextField.TYPE_NOT_STORED);
				}
                 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
		
		idxing = (IgniteH2Indexing)ctx.query().getIndexing();
	}

	@Override
	public Object getPosition(Document document) {
		//Set<KeyValue> keyValues = getKeyValues(document);
		Object key = document.getOrDefault("_id", null);
	   	 if(key!=null) {
	   		 return key;
	   	 }
		return null;
	}

	@Override
	public void checkAdd(Document document, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {
		// index all field
		if(collection instanceof IgniteCollection) {
			IgniteCollection coll = (IgniteCollection) collection;
			T2<String,String> t2 = IgniteCollection.typeNameAndKeyField(coll.dataMap,document);
	    	String typeName = t2.get1();    	
	    	String keyField = t2.get2();    
	    	
	    	if(idxdFields==null) {
	    		Set<String> fields = coll.fields().keySet();
	    		idxdFields = new String[fields.size()];
	    		fields.toArray(idxdFields);
	    	}
	    	
	    	if(!this.isFirstIndex)
	    		return ;
	    	Object key = document.getOrDefault(coll.idField, null);
	    	Field.Store storeText = indexAccess.config.isStoreTextFieldValue()?  Field.Store.YES : Field.Store.NO;

	    	org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();

	        boolean stringsFound = false;
	        
	        Object[] row = new Object[idxdFields.length]; 
	        for (int i = 0, last = idxdFields.length; i < last; i++) {
	            Object fieldVal = document.get(idxdFields[i]);
	            row[i] = fieldVal;
	        }
	        byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(key),false);
	        BytesRef keyByteRef = new BytesRef(keyBytes);
	        Term term = new Term(KEY_FIELD_NAME, keyByteRef);
	         // build doc body
	        try {
				stringsFound = FullTextLucene.FullTextTrigger.buildDocument(doc,this.idxdFields,null,row,storeText);
				if (!stringsFound) {
	            	indexAccess.writer.deleteDocuments(term);

	                return; // We did not find any strings to be indexed, will not store data at all.
	            }
				
				 doc.add(new StringField(KEY_FIELD_NAME, keyByteRef, Field.Store.YES));
		         doc.add(new StringField(FullTextLucene.FIELD_TABLE, typeName, Field.Store.YES));
		            
		         // Next implies remove than add atomically operation.
		         long seq = indexAccess.writer.updateDocument(term, doc);
		         docCount = seq;
		         
		         indexAccess.increment();
		            
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	       
	            
		}
		
		else if(collection instanceof IgniteBinaryCollection) {
			IgniteBinaryCollection coll = (IgniteBinaryCollection) collection;
			T2<String,String> t2 = IgniteCollection.typeNameAndKeyField(coll.dataMap,document);
	    	String typeName = t2.get1();    	
	    	String keyField = t2.get2();    
	    	Map<String, FieldType> fields = indexAccess.fields(typeName);
	    	for(IndexKey ik: this.getKeys()) {
				fields.put(ik.getKey(), TextField.TYPE_NOT_STORED);
			}
		}
	}

	@Override
	public Object remove(Document document,MongoCollection<Object> collection) {
		// TODO Auto-generated method stub
		if(collection instanceof IgniteCollection && this.isFirstIndex) {
			try {
				IgniteCollection coll = (IgniteCollection) collection;
				Object key = document.getOrDefault(coll.idField, null);
				if(key!=null) {
					byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(key),false);
			        BytesRef keyByteRef = new BytesRef(keyBytes);
			        Term term = new Term(KEY_FIELD_NAME, keyByteRef);
					indexAccess.writer.deleteDocuments(term);
				}
	        }
	        catch (IOException e) {
	            e.printStackTrace();
	        }
	        finally {
	        	indexAccess.increment();
	        }
		}
		return null;
	}

	@Override
	public boolean canHandle(Document query) {

        if (!query.keySet().containsAll(keySet())) {
            return false;
        }

        if (isSparse() && query.values().stream().allMatch(Objects::isNull)) {
            return false;
        }

        for (String key : keys()) {
            Object queryValue = query.get(key);
            if (queryValue instanceof Document) {
                if (isCompoundIndex()) {
                    // https://github.com/bwaldvogel/mongo-java-server/issues/80
                    // Not yet supported. Use some other index, or none:
                    return false;
                }
                if (BsonRegularExpression.isRegularExpression(queryValue)) {
                    continue;
                }
                if (BsonRegularExpression.isTextSearchExpression(queryValue)) {
                    continue;
                }
                for (String queriedKeys : ((Document) queryValue).keySet()) {
                    if (isInQuery(queriedKeys)) {
                        // okay
                    } else if (queriedKeys.startsWith("$")) {
                        // not yet supported
                        return false;
                    }
                }
            }
        }

        return true;
		
	}

	@Override
	public Iterable<Object> getPositions(Document query) {

		 KeyValue queriedKeys = getQueriedKeys(query);

	     for (Object queriedKey : queriedKeys.iterator()) {
	         if (BsonRegularExpression.isRegularExpression(queriedKey)) {
	             if (isCompoundIndex()) {
	                 throw new UnsupportedOperationException("Not yet implemented");
	             }
	             List<Object> positions = new ArrayList<>();
	             for (Entry<KeyValue, Object> entry : getFullTextIterable(queriedKey)) {
	                 KeyValue obj = entry.getKey();
	                 if (obj.size() == 1) {
	                     Object o = obj.get(0);
	                     if (o instanceof String) {
	                         BsonRegularExpression regularExpression = BsonRegularExpression.convertToRegularExpression(queriedKey);
	                         Matcher matcher = regularExpression.matcher(o.toString());
	                         if (matcher.find()) {
	                             positions.add(entry.getValue());
	                         }
	                     }
	                 }
	             }
	             return positions;
	         } 	         
	         else if (BsonRegularExpression.isTextSearchExpression(queriedKey)) {
	             if (isCompoundIndex()) {
	                 throw new UnsupportedOperationException("Not yet implemented");
	             }
	             List<Object> positions = new ArrayList<>();
	             for (Entry<KeyValue, Object> entry : getFullTextIterable(queriedKey)) {
	                 KeyValue obj = entry.getKey();
	                 if (obj.size() == 1) {
	                     Object o = obj.get(0);
	                     positions.add(entry.getValue());
	                 }
	             }
	             return positions;
	         } 
	         else if (queriedKey instanceof Document) {
	             if (isCompoundIndex()) {
	                 throw new UnsupportedOperationException("Not yet implemented");
	             }
	             Document keyObj = (Document) queriedKey;
	             if (Utils.containsQueryExpression(keyObj)) {
	                 String expression = CollectionUtils.getSingleElement(keyObj.keySet(),
	                     () -> new UnsupportedOperationException("illegal query key: " + queriedKeys));

	                 if (expression.startsWith("$")) {
	                     return getPositionsForExpression(keyObj, expression);
	                 }
	             }
	         }
	     }

	     List<Object> positions = getPosition(queriedKeys);
	     if (positions == null) {
	         return Collections.emptyList();
	     }
	     return positions;
	}

	@Override
	public long getCount() {
		// TODO Auto-generated method stub
		return docCount;
	}

	@Override
	public long getDataSize() {
		// TODO Auto-generated method stub
		return docCount*8;
	}

	@Override
	public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateInPlace(Document oldDocument, Document newDocument, Object position,
			MongoCollection<Object> collection) throws KeyConstraintError {		
		 if (true || !nullAwareEqualsKeys(oldDocument, newDocument)) {
	            Object removedPosition = remove(oldDocument,collection);
	            if (removedPosition != null) {
	                Assert.equals(removedPosition, position);
	            }
	            add(newDocument, position, collection);
	        }
	}

	@Override
	public void drop() {
		// TODO Auto-generated method stub
		
	}

	 private Iterable<Object> getPositionsForExpression(Document keyObj, String operator) {
        if (isInQuery(operator)) {
            @SuppressWarnings("unchecked")
            Collection<Object> objects = (Collection<Object>) keyObj.get(operator);
            Collection<Object> queriedObjects = new TreeSet<>(ValueComparator.asc());
            queriedObjects.addAll(objects);

            List<Object> allKeys = new ArrayList<>();
            for (Object object : queriedObjects) {
            	 Object keyValue = Utils.normalizeValue(object);
                 List<Object> keys = getPosition(KeyValue.valueOf(keyValue));
                 if (keys != null) {
                     allKeys.addAll(keys);
                 }
            }

            return allKeys;
        } else {
            throw new UnsupportedOperationException("unsupported query expression: " + operator);
        }
    }
	 
	 
	 protected List<Object> getPosition(KeyValue keyValue) {
		 
		 LuceneIndexAccess access = indexAccess;
		
	    	
		 List<Object> result = new ArrayList<>();
	        try {
	        	
	        	String cacheName = access.cacheName();
	        	ClassLoader ldr = null;
	            
	            GridCacheAdapter cache = null;
	            if (ctx != null){
	            	cache = ctx.cache().internalCache(cacheName);  
	            	if(cache==null) {
	            		cache = ctx.cache().internalCache(this.cacheName);  
	            	}
	            }
	            if (cache != null && ctx.deploy().enabled())
	                ldr = cache.context().deploy().globalLoader();
	            
	            access.flush();
	            
	            // take a reference as the searcher may change
	            IndexSearcher searcher = access.searcher;
	            // reuse the same analyzer; it's thread-safe;
	            // also allows subclasses to control the analyzer used.
	            Analyzer analyzer = access.writer.getAnalyzer(); 	            
	            // Filter expired items.
	            Query filter = LongPoint.newRangeQuery(FullTextLucene.EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(),Long.MAX_VALUE);

	            BooleanQuery.Builder query = new BooleanQuery.Builder();
	            int n = 0;
	            for(IndexKey key: this.getKeys()) {
	            	Object obj = keyValue.get(n);
	            	
	            	if(obj instanceof Number) {
	            		if(obj instanceof Long || obj instanceof Integer || obj instanceof Short) {
	            			Query termQuery = LongPoint.newExactQuery(key.getKey(),((Number)obj).longValue());
	            			query.add(termQuery, BooleanClause.Occur.MUST);
	            		}
	            		else {
	            			double d = ((Number)obj).doubleValue();
	            		}
	            	}
	            	else if(obj instanceof byte[]) {
	            		 Term term = new  Term(key.getKey(),new BytesRef((byte[])obj));
		            	 Query termQuery = new TermQuery(term);
		            	 query.add(termQuery, BooleanClause.Occur.MUST);
	            	}
	            	else {
		            	 Term term = new  Term(key.getKey(),obj.toString());
		            	 Query termQuery = new TermQuery(term);
		            	 query.add(termQuery, BooleanClause.Occur.MUST);
	            	}
	            	n++;	            	
	            }
	            //query.add(filter, BooleanClause.Occur.FILTER);
	            
	            int limit = 0;
	            // Lucene 3 insists on a hard limit and will not provide
	            // a total hits value. Take at least 100 which is
	            // an optimal limit for Lucene as any more
	            // will trigger writing results to disk.
	            int maxResults = Integer.MAX_VALUE;
	            TopDocs docs = searcher.search(query.build(), maxResults);
	            if (limit == 0) {
	                limit = (int)docs.totalHits;
	            }
	            result = new ArrayList<>(limit);
	            for (int i = 0;  i < limit ; i++) {
	                ScoreDoc sd = docs.scoreDocs[i];
	                org.apache.lucene.document.Document doc = searcher.doc(sd.doc);
	                float score = sd.score;                
	               
	                Object k = ctx.cacheObjects().unmarshal(cache.context().cacheObjectContext(),doc.getBinaryValue(KEY_FIELD_NAME).bytes, ldr);
	                             	
	                result.add(k);
	                
	            }
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	        return result;
	 }
	    
	    
	 /**
	  *   对字符串字段进行搜索查询，支持模糊匹配
	  * @param text
	  * @return
	  */
    protected Iterable<Entry<KeyValue, Object>> getFullTextIterable(Object exp) {
    	 LuceneIndexAccess access = indexAccess;
    	 String field = this.getKeys().get(0).getKey();
    	
		 List<Entry<KeyValue, Object>> result = new ArrayList<>();
	        try {
	        	
	        	String cacheName = access.cacheName();
	        	ClassLoader ldr = null;
	            
	            GridCacheAdapter cache = null;
	            if (ctx != null){
	            	cache = ctx.cache().internalCache(cacheName);   
	            	if(cache==null) {
	            		cache = ctx.cache().internalCache(this.cacheName);  
	            	}
	            }
	            if (cache != null && ctx.deploy().enabled())
	                ldr = cache.context().deploy().globalLoader();
	            
	            access.flush();
	            
	            // take a reference as the searcher may change
	            IndexSearcher searcher = access.searcher;
	            // reuse the same analyzer; it's thread-safe;
	            // also allows subclasses to control the analyzer used.
	            Analyzer analyzer = access.writer.getAnalyzer(); 	            
	            Object text = null;
	            QueryParser parser = new QueryParser(field, access.getQueryAnalyzer()); //定义查询分析器
			    if(exp instanceof Map) {
			    	text = ((Map<String, Object>)exp).get(BsonRegularExpression.TEXT);
			    }
			    else {
			    	text = exp;
			    }
	            Query query = parser.parse(text.toString());
	        
	            
	            int limit = 0;
	            // Lucene 3 insists on a hard limit and will not provide
	            // a total hits value. Take at least 100 which is
	            // an optimal limit for Lucene as any more
	            // will trigger writing results to disk.
	            int maxResults = Integer.MAX_VALUE;
	            TopDocs docs = searcher.search(query, maxResults);
	            if (limit == 0) {
	                limit = (int)docs.totalHits;
	            }
	            result = new ArrayList<>(limit);
	            for (int i = 0;  i < limit ; i++) {
	                ScoreDoc sd = docs.scoreDocs[i];
	                org.apache.lucene.document.Document doc = searcher.doc(sd.doc);
	                float score = sd.score;                
	               
	                Object k = ctx.cacheObjects().unmarshal(cache.context().cacheObjectContext(),doc.getBinaryValue(KEY_FIELD_NAME).bytes, ldr);
	                String v = doc.get(field);     
	                
	                result.add(new IgniteBiTuple<KeyValue, Object>(KeyValue.valueOf(v),k));
	                
	            }
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	        return result;
    	
       
    }
	 
    private static boolean isInQuery(String key) {
        return key.equals(QueryOperator.IN.getValue());
    }

	public boolean isFirstIndex() {
		return isFirstIndex;
	}

	public void setFirstIndex(boolean isFirstIndex) {
		this.isFirstIndex = isFirstIndex;
	}

	/**
     * @return Cache object context.
     */
    private CacheObjectContext objectContext() {
        if (ctx == null)
            return null;

        return ctx.cache().internalCache(cacheName).context().cacheObjectContext();
    }
}
