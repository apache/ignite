package de.bwaldvogel.mongo.backend.ignite;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.FullTextLucene;
import org.apache.ignite.cache.LuceneIndexAccess;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.Term;

import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.ComposeKeyValue;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.QueryOperator;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;

public class IgniteLuceneIndex extends Index<Object> {
	private static final Logger log = LoggerFactory.getLogger(IgniteLuceneIndex.class);

	private final String cacheName;

	private LuceneIndexAccess indexAccess;

	private final GridKernalContext ctx;	

	private GridBinaryMarshaller marshaller;
	
	private IgniteH2Indexing igniteH2Indexing = null;
	
	private boolean isFirstIndex = false;
	
	private Map<String, Float> weights = new HashMap<>();

	private long docCount = 0;
	
	private String idField = "_id";

	/** */
	private String[] idxdFields = null;
	private FieldType[] idxdTypes = null;

	public IgniteLuceneIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys,	boolean sparse) {
		super(name, keys, true);  // lucene index always is sparse
		this.ctx = ctx;
		this.cacheName = collection.getCollectionName();
		this.idField = collection.idField;
		// init field weight
		for(IndexKey indexKey: keys) {
			if(indexKey.textOptions()!=null) {
				if(indexKey.textOptions().containsKey("weight")) {
					float w = Float.valueOf(indexKey.textOptions().get("weight").toString());
					weights.put(indexKey.getKey(), w);
				}
				else {
					weights.put(indexKey.getKey(), 1.0f);
				}
			}
		}
		
		init(collection);
	}

	public void init(IgniteBinaryCollection coll) {
		if (indexAccess == null) {
			try {
				indexAccess = LuceneIndexAccess.getIndexAccess(ctx, cacheName);

				CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl) ctx.cacheObjects();

				marshaller = cacheObjProc.marshaller();				
				
				T3<String, String,String> t2 = coll.typeNameAndKeyField(coll.dataMap, new Document());
				String schemaName = t2.get1();
				String typeName = t2.get2();
				String keyField = t2.get3();
				
				Map<String, FieldType> fields = indexAccess.fields(typeName);
				for (IndexKey ik : this.getKeys()) {
					if (ik.isText()) {
						fields.putIfAbsent(ik.getKey(), TextField.TYPE_NOT_STORED);
					} else {
						fields.putIfAbsent(ik.getKey(), StringField.TYPE_STORED);
					}
				}
				
				igniteH2Indexing = (IgniteH2Indexing) ctx.query().getIndexing();

			} catch (IOException e) {
				log.error("create luence index failed:",e);
			}
		}
		this.idxdFields = null;
		this.idxdTypes = null;
	}

	private boolean hasIgniteLuenceIndex(String typeName) {
		if(igniteH2Indexing!=null) {
			return false;
		}
		@Nullable
		Collection<TableDescriptor> tables = igniteH2Indexing.schemaManager().tablesForCache(cacheName);
		for(TableDescriptor table: tables) {
			if (table.type().name().equalsIgnoreCase(typeName) && table.type().textIndex() != null) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public Object getPosition(Document document) {
		// Set<KeyValue> keyValues = getKeyValues(document);
		Object key = document.getOrDefault(idField, null);
		if (key != null) {
			return DocumentUtil.toBinaryKey(key);
		}
		return null;
	}	

	@Override
	public void checkAdd(Document document, MongoCollection<Object> collection) {
		if (!this.isFirstIndex)
			return;
		if(igniteH2Indexing!=null) {
			// 获取当前文档的类型,优先使用_class字段，然后是Cache QueryEntity
			
			if(document.containsKey("_class")) {
				String typeName = (String)document.get("_class");
				
				Map<String, FieldType> fields = indexAccess.fields(typeName);
				for (IndexKey ik : this.getKeys()) {
					if (ik.isText()) {
						fields.putIfAbsent(ik.getKey(), TextField.TYPE_NOT_STORED);
					} else {
						fields.putIfAbsent(ik.getKey(), StringField.TYPE_STORED);
					}
				}
			}

			
		}

	}
	
	private BytesRef marshalKeyField(Object key) {		
		byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(key), false);		
		return new BytesRef(keyBytes);
	}
	
	private Object unmarshalKeyField(BytesRef bytes, GridCacheAdapter cache, ClassLoader ldr) throws IgniteCheckedException {
		byte[] keyBytes = bytes.bytes;
		Object k = ctx.cacheObjects().unmarshal(cache.context().cacheObjectContext(), keyBytes, ldr);
		return k;
	}

	public HashMap<String, FieldType> fieldsMapping(MongoCollection<Object> collection) {
		HashMap<String, FieldType> fields = new HashMap<>();
		for (Index<Object> idx : collection.getIndexes()) {
			if (idx instanceof IgniteLuceneIndex) {
				IgniteLuceneIndex igniteIndex = (IgniteLuceneIndex) idx;
				igniteIndex.init((IgniteBinaryCollection)collection);				
				for (IndexKey ik : idx.getKeys()) {
					if (ik.isText()) {
						fields.putIfAbsent(ik.getKey(), TextField.TYPE_NOT_STORED);
					} else {
						fields.putIfAbsent(ik.getKey(), StringField.TYPE_STORED);
					}
				}
			}
		}
		return fields;
	}

	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {
		if (!this.isFirstIndex)
			return;
		
		
		IgniteBinaryCollection coll = (IgniteBinaryCollection) collection;
		T3<String, String,String> t2 = coll.typeNameAndKeyField(coll.dataMap, document);
		String schemaName = t2.get1();
		String typeName = t2.get2();
		String keyField = t2.get3();
		
		// 使用Ignite自己的luence索引
		if(hasIgniteLuenceIndex(typeName)) {
			return ;
		}
		
		// index all field
		if (idxdFields == null || idxdFields.length==0) {
			Map<String, FieldType> fields = fieldsMapping(collection);
			idxdFields = new String[fields.size()];
			idxdTypes = new FieldType[fields.size()];
			int i = 0;
			for (Map.Entry<String, FieldType> ft : fields.entrySet()) {
				idxdFields[i] = ft.getKey();
				idxdTypes[i++] = ft.getValue();
			}
		}

		org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();

		boolean stringsFound = false;

		Object[] row = new Object[idxdFields.length];
		
		
		for (int i = 0, last = idxdFields.length; i < last; i++) {
			Object fieldVal = document.get(idxdFields[i]);			
			
			//byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(fieldVal), false);
			//BytesRef keyByteRef = new BytesRef(keyBytes);
			row[i] = fieldVal;
		}
		
		
		BytesRef keyByteRef = marshalKeyField(position);
		Term term = new Term(KEY_FIELD_NAME, keyByteRef);
		// build doc body
		try {
			stringsFound = FullTextLucene.buildDocument(doc, idxdFields, idxdTypes, null, row);
			if (!stringsFound) {
				indexAccess.writer.deleteDocuments(term);

				return; // We did not find any strings to be indexed, will not store data at all.
			}
			
			doc.add(new StringField(KEY_FIELD_NAME, keyByteRef, Field.Store.YES));
			doc.add(new StoredField(FullTextLucene.FIELD_TABLE, typeName));

			// Next implies remove than add atomically operation.
			docCount = indexAccess.writer.updateDocument(term, doc);
			
			indexAccess.increment();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Object remove(Document document, MongoCollection<Object> collection) {		
		if(!this.isFirstIndex) {
			return null;
		}
		IgniteBinaryCollection coll = (IgniteBinaryCollection) collection;
		Object key = getPosition(document);
		try {
			
			if (key != null) {				
				BytesRef keyByteRef = marshalKeyField(key);
				Term term = new Term(KEY_FIELD_NAME, keyByteRef);
				long seq = indexAccess.writer.deleteDocuments(term);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			indexAccess.increment();
		}
		return null;
	}

	@Override
	public boolean canHandle(Document query) {

		if (this.isTextIndex() && BsonRegularExpression.isTextSearchExpression(query)) {
			return !true;
		}
		
		if (this.isTextIndex() && BsonRegularExpression.isRegularExpression(query)) {
			return true;
		}
		
		if (!CollectionUtils.containsAny(query.keySet(), keySet())) {
			return false;
		}

		if (isSparse() && query.values().stream().allMatch(Objects::isNull)) {
			return false;
		}

		for (String key : keys()) {
			Object queryValue = query.get(key);
			if (queryValue instanceof Document) {
				Document queryDoc = (Document) queryValue;
				if (BsonRegularExpression.isRegularExpression(queryValue)) {
					continue;
				}
				if (BsonRegularExpression.isTextSearchExpression(queryValue)) {
					continue;
				}
				for (String queriedKeys : queryDoc.keySet()) {
					if (isInQuery(queriedKeys)) {
						// okay
					} 
					else if (this.isTextIndex() && queriedKeys.startsWith("$search")) {
						// not yet supported
						return true;
					}
					else if (queriedKeys.startsWith("$type") || queriedKeys.startsWith("$exists")
						    || queriedKeys.startsWith("$mod")|| queriedKeys.startsWith("$size") ) {
						// not yet supported
						if(queryDoc.size()==1) {
							return false;
						}
					}
				}
			}
		}

		return true;

	}

	@Override
	public Iterable<Object> getPositions(Document query) {
		final KeyValue queriedKeys = getQueriedKeys(query);
		KeyValue searchKey = queriedKeys;		
		int n = 0;
		BooleanQuery.Builder queryBuider = new BooleanQuery.Builder();
		Document $text = null;
		for (Object queriedKey : queriedKeys.iterator()) {
			IndexKey indexKey = this.getKeys().get(n);
			// for $text value  { textField : { $text: 'keyword' } }			
			if (BsonRegularExpression.isRegularExpression(queriedKey)) { // { textField : { $regex: 'keyword' } }
				
				List<Object> positions = new ArrayList<>();
				for (IdWithMeta obj : getFullTextList(indexKey, queriedKey)) {					
					if (obj.key!=null) { // k, score, v
						Object v = obj.indexValue;
						if (v!=null) {
							BsonRegularExpression regularExpression = BsonRegularExpression.convertToRegularExpression(queriedKey);
							Matcher matcher = regularExpression.matcher(v.toString());
							if (matcher.find()) {
								positions.add(obj.key);
							}
						}
					}
				}
				query.remove(indexKey.getKey());
				return positions;
				
			} 
			else if (BsonRegularExpression.isTextSearchExpression(queriedKey)) { // { textField : { $text: 'keyword' } }
				
				List<IdWithMeta> positions = getFullTextList(indexKey, queriedKey);				
				query.remove(indexKey.getKey());
				return (List)positions;
			} 			
			else if (queriedKey instanceof Document) {
				if (isCompoundIndex() && !this.isTextIndex()) {
					throw new UnsupportedOperationException("Not yet implemented");
				}
				Document keyObj = (Document) queriedKey;
				if (Utils.containsQueryExpression(keyObj)) {
					Set<String> expression = keyObj.keySet();
					
					if (expression.contains(QueryOperator.IN.getValue())) {						
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.IN.getValue()), QueryOperator.IN);					
						queryBuider.add(termQuery, BooleanClause.Occur.MUST);						
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.NOT_IN.getValue())) {						
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.NOT_IN.getValue()), QueryOperator.NOT_IN);					
						
						queryBuider.add(termQuery, BooleanClause.Occur.MUST_NOT);						
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.EQUAL.getValue())) {
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.EQUAL.getValue()), QueryOperator.EQUAL);
						
						queryBuider.add(termQuery, BooleanClause.Occur.MUST);
						query.remove(indexKey.getKey());						
					}
					else if (expression.contains(QueryOperator.NOT_EQUALS.getValue())) {											
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.NOT_EQUALS.getValue()), QueryOperator.NOT_EQUALS);
						
						queryBuider.add(termQuery, BooleanClause.Occur.MUST_NOT);
						query.remove(indexKey.getKey());						
					}
					else if (expression.contains(QueryOperator.GREATER_THAN.getValue())) {
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.GREATER_THAN.getValue()), QueryOperator.GREATER_THAN);
						
						queryBuider.add(termQuery, BooleanClause.Occur.FILTER);
						query.remove(indexKey.getKey());						
					}
					else if (expression.contains(QueryOperator.GREATER_THAN_OR_EQUAL.getValue())) {
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.GREATER_THAN_OR_EQUAL.getValue()), QueryOperator.GREATER_THAN_OR_EQUAL);
						
						queryBuider.add(termQuery, BooleanClause.Occur.FILTER);
						query.remove(indexKey.getKey());						
					}
					else if (expression.contains(QueryOperator.LESS_THAN.getValue())) {
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.LESS_THAN.getValue()), QueryOperator.LESS_THAN);
						
						queryBuider.add(termQuery, BooleanClause.Occur.FILTER);
						query.remove(indexKey.getKey());						
					}
					else if (expression.contains(QueryOperator.LESS_THAN_OR_EQUAL.getValue())) {
						Query termQuery = getQueryValueForExpression(indexKey,keyObj.get(QueryOperator.LESS_THAN_OR_EQUAL.getValue()), QueryOperator.LESS_THAN_OR_EQUAL);
						
						queryBuider.add(termQuery, BooleanClause.Occur.FILTER);
						query.remove(indexKey.getKey());						
					}					
				}				
			}
			else if (queriedKey instanceof String) {					
				String keyString = (String) queriedKey;
				if (!keyString.isEmpty()) {
					if(this.isTextIndex()) {
						SimpleQueryParser parser = new SimpleQueryParser(indexAccess.analyzerWrapper, weights); // 定义查询分析器 // 定义查询分析器
						parser.setDefaultOperator(BooleanClause.Occur.MUST);
						Query textQuery = parser.parse(keyString);
						queryBuider.add(textQuery, BooleanClause.Occur.MUST);
					}
					else {
						Query termQuery = getQueryValueForExpression(indexKey,keyString, QueryOperator.EQUAL);						
						queryBuider.add(termQuery, BooleanClause.Occur.MUST);
						query.remove(indexKey.getKey());		
					}
				}
			}
			
			// for { $text : { $search: 'keyword' } } || { $text : { $knnVector: [0.1,0.4,0.6] } }
			if(queriedKey == null && this.isTextIndex() && query.containsKey("$text")) {
				queriedKey = query.get("$text");
				if (queriedKey instanceof Document) {
					$text = (Document)queriedKey;
				}
				else {
					$text = new Document("$text", queriedKey);
				}
				query.remove("$text");
				
			}
			
			n++;
		}

		List<IdWithMeta> positions = getPosition(queryBuider,searchKey,$text);
		if (positions == null) {
			return Collections.emptyList();
		}
		return (List)positions;
	}

	@Override
	public long getCount() {
		if(docCount>0) {
			return docCount;
		}
		return 1+indexAccess.writer.getDocStats().numDocs;
	}

	@Override
	public long getDataSize() {
		long siz = 0;
		for(SegmentCommitInfo seg: indexAccess.writer.getMergingSegments()) {
			try {
				siz += seg.sizeInBytes();
			} 
			catch (IOException e) {
			}
		}
		return siz;
	}

	@Override
	public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Object> collection) {
		
	}

	@Override
	public void updateInPlace(Document oldDocument, Document newDocument, Object position,
			MongoCollection<Object> collection) throws KeyConstraintError {
		if (!nullAwareEqualsKeys(oldDocument, newDocument)) {
			Object removedPosition = remove(oldDocument, collection);
			if (removedPosition != null) {
				Assert.equals(removedPosition, position);
			}
			add(newDocument, position, collection);
		}
	}

	@Override
	public void drop() {
		String typeName = IgniteBinaryCollection.tableOfCache(cacheName);
		Map<String, FieldType> fields = indexAccess.fields(typeName);
		for (IndexKey ik : this.getKeys()) {
			fields.remove(ik.getKey());
		}		
		this.indexAccess = null;
	}
	
	void close() {
		LuceneIndexAccess.removeIndexAccess(indexAccess);
	}

	private Query getQueryValueForExpression(IndexKey key, Object value, QueryOperator operator) {
		if (operator==QueryOperator.IN || operator==QueryOperator.NOT_IN) {
			@SuppressWarnings("unchecked")
			Collection<Object> objects = (Collection<Object>) value;
			Collection<Object> queriedObjects = new TreeSet<>(ValueComparator.asc());
			queriedObjects.addAll(objects);

			List<Object> allKeys = new ArrayList<>();
			for (Object object : queriedObjects) {
				Object keyValue = Utils.normalizeValue(object);
				allKeys.add(keyValue);
			}
			
			BytesRef[] terms = new BytesRef[allKeys.size()];
			for(int i=0;i<terms.length;i++) {
				Object item = allKeys.get(i);
				if(item instanceof byte[]) {
					BytesRef term = new BytesRef((byte[])item);
					terms[i] = term;
				}
				else {
					BytesRef term = new BytesRef(item.toString());
					terms[i] = term;
				}										
			}
			
			Query termQuery = new TermInSetQuery(key.getKey(),terms);
			return termQuery;
		}
		else if (operator==QueryOperator.EQUAL || operator==QueryOperator.NOT_EQUALS) {
			Object obj = value;
			Query termQuery;
			if(obj instanceof ObjectId || obj instanceof UUID) {									
				byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(obj),false);
	            BytesRef keyByteRef = new BytesRef(keyBytes);	
	            Term term = new Term(key.getKey(), keyByteRef);
				termQuery = new TermQuery(term);
				
	     	} else if (obj instanceof Number) {
				if (obj instanceof Long ) {
					 termQuery = LongPoint.newExactQuery(key.getKey(), ((Number) obj).longValue());
					
				}
				else if (obj instanceof Integer || obj instanceof Short) {
					termQuery = IntPoint.newExactQuery(key.getKey(), ((Number) obj).intValue());
					
				}
				else if (obj instanceof Float ) {
					termQuery = FloatPoint.newExactQuery(key.getKey(), ((Number) obj).floatValue());
					
				}
				else {
					double d = ((Number) obj).doubleValue();						
					termQuery = DoublePoint.newExactQuery(key.getKey(),d);					
				}
				
			} else if (obj instanceof byte[]) {
				Term term = new Term(key.getKey(), new BytesRef((byte[]) obj));
				termQuery = new TermQuery(term);
				
			} else {
				Term term = new Term(key.getKey(), obj.toString());
				termQuery = new TermQuery(term);				
			}
			return termQuery;
		}
		else if (operator==QueryOperator.GREATER_THAN || operator==QueryOperator.GREATER_THAN_OR_EQUAL) {
			Object obj = value;
			Query filter;
			if (obj instanceof Number) {
				if (obj instanceof Long ) {					
					filter = LongPoint.newRangeQuery(key.getKey(), ((Number) obj).longValue(),Long.MAX_VALUE);
					
				}
				else if (obj instanceof Integer || obj instanceof Short) {
					filter = IntPoint.newRangeQuery(key.getKey(), ((Number) obj).intValue(),Integer.MAX_VALUE);
					
				}
				else if (obj instanceof Float ) {
					filter = FloatPoint.newRangeQuery(key.getKey(), ((Number) obj).floatValue(),Float.MAX_VALUE);
					
				}
				else {
					double d = ((Number) obj).doubleValue();						
					filter = DoublePoint.newRangeQuery(key.getKey(),d,Double.MAX_VALUE);					
				}
				
			} else if (obj instanceof byte[]) {				
				filter = BinaryPoint.newRangeQuery(key.getKey(),(byte[]) obj, null);
				
			} else {				
				filter = SortedDocValuesField.newSlowRangeQuery(key.getKey(), new BytesRef(obj.toString()), null,true,false);				
			}
			return filter;
		}
		else if (operator==QueryOperator.LESS_THAN || operator==QueryOperator.LESS_THAN_OR_EQUAL) {
			Object obj = value;
			Query filter;
			if (obj instanceof Number) {
				if (obj instanceof Long ) {					
					filter = LongPoint.newRangeQuery(key.getKey(), Long.MIN_VALUE, ((Number) obj).longValue());
					
				}
				else if (obj instanceof Integer || obj instanceof Short) {
					filter = IntPoint.newRangeQuery(key.getKey(), Integer.MIN_VALUE, ((Number) obj).intValue());
					
				}
				else if (obj instanceof Float ) {
					filter = FloatPoint.newRangeQuery(key.getKey(),Float.MIN_VALUE, ((Number) obj).floatValue());
					
				}
				else {
					double d = ((Number) obj).doubleValue();						
					filter = DoublePoint.newRangeQuery(key.getKey(),Double.MIN_VALUE, d);					
				}
				
			} else if (obj instanceof byte[]) {				
				filter = BinaryPoint.newRangeQuery(key.getKey(), null, (byte[]) obj);
				
			} else {				
				filter = SortedDocValuesField.newSlowRangeQuery(key.getKey(), null, new BytesRef(obj.toString()), false, true);				
			}
			return filter;
		}
		else {
			throw new UnsupportedOperationException("unsupported query expression: " + operator);
		}
	}
	/**
	 * 对所有字段使用lucene索引进行查询, for $text
	 * @param keyValue
	 * @return
	 */
	protected List<IdWithMeta> getPosition(BooleanQuery.Builder query, KeyValue keyValue, Document $text) {
		List<IdWithMeta> result = new ArrayList<>();
		LuceneIndexAccess access = indexAccess;

		try {

			String cacheName = access.cacheName();
			ClassLoader ldr = null;

			GridCacheAdapter<KeyValue,Object> cache = null;
			if (ctx != null) {
				cache = ctx.cache().internalCache(cacheName);
				if (cache == null) {
					cache = ctx.cache().internalCache(this.cacheName);
				}
			}
			if (cache != null && ctx.deploy().enabled())
				ldr = cache.context().deploy().globalLoader();

			access.flush();
			
			int limit = 0;
			SortField sortField = null;

			// take a reference as the searcher may change
			IndexSearcher searcher = access.searcher;
			// reuse the same analyzer; it's thread-safe;
			// also allows subclasses to control the analyzer used.			
			// Filter expired items.
			//-Query filter = LongPoint.newRangeQuery(FullTextLucene.EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(),Long.MAX_VALUE);
			// query.add(filter, BooleanClause.Occur.FILTER);
			
			
			String defaultTextField = null;
			int n = 0;
			for (IndexKey key : this.getKeys()) {				
				if (key.isText() && defaultTextField==null) {
					defaultTextField = key.getKey();
				}				
				n++;
			}
			
			
			if ($text!=null) {
				Map<String, Object> opt = ((Map<String, Object>) $text);
				Object obj = null;
				if(opt.containsKey("$search")) {
					obj = opt.get("$search"); // 更复杂，支持多种字段
					
					StandardQueryParser parser = new StandardQueryParser(access.analyzerWrapper); // 定义查询分析器
					parser.setFieldsBoost(weights);
					Query textQuery = parser.parse(obj.toString(),defaultTextField);
					query.add(textQuery, BooleanClause.Occur.MUST);
				}
				else if(opt.containsKey("$text")) {
					obj = opt.get("$text"); // 更精细，只支持文本字段
					
					SimpleQueryParser parser = new SimpleQueryParser(access.analyzerWrapper, weights); // 定义查询分析器 // 定义查询分析器
					parser.setDefaultOperator(BooleanClause.Occur.MUST);
					Query textQuery = parser.parse(obj.toString());
					query.add(textQuery, BooleanClause.Occur.MUST);
				}					
				
				if(opt.containsKey("$limit")) {
					limit = Integer.parseInt(opt.get("$limit").toString());
				}
				
				if(opt.containsKey("$sort")) {
					String sortOpt = opt.get("$sort").toString();
					sortField = new SortField(sortOpt,Type.DOUBLE, true);
				}				
				
			}
			
			// Lucene 3 insists on a hard limit and will not provide
			// a total hits value. Take at least 100 which is
			// an optimal limit for Lucene as any more
			// will trigger writing results to disk.
			int maxResults = GridLuceneIndex.DEAULT_LIMIT;
			if(limit>0) {
				maxResults = limit;
			}
			
			TopDocs docs = null;
			if(sortField==null)
				docs = searcher.search(query.build(), maxResults);
			else
				docs = searcher.search(query.build(), maxResults, new Sort(sortField));
			
			limit = docs.scoreDocs.length;
			
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				ScoreDoc sd = docs.scoreDocs[i];
				org.apache.lucene.document.Document doc = searcher.doc(sd.doc);
				float score = sd.score;

				Object k = unmarshalKeyField(doc.getBinaryValue(KEY_FIELD_NAME), cache, ldr);
				
				Document meta = new Document("searchScore",score);
				if(i==0) {
					meta.append("totalHits", docs.totalHits.value);
				}

				result.add(new IdWithMeta(k,false,meta));

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 对指定的字符串字段进行搜索$search查询，支持模糊匹配
	 * 
	 * @param text 
	 * @return 字段值，_key
	 */
	protected List<IdWithMeta> getFullTextList(IndexKey indexKey, Object exp) {
		LuceneIndexAccess access = indexAccess;		
		int limit = 0;
		List<IdWithMeta> result = new ArrayList<>();
		try {
			String field = indexKey.getKey();
			String cacheName = access.cacheName();
			ClassLoader ldr = null;

			GridCacheAdapter<KeyValue, Object> cache = null;
			if (ctx != null) {
				cache = ctx.cache().internalCache(cacheName);
				if (cache == null) {
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
			SortField sortField = null;
			Object text = exp;
			Query textQuery = null;
			if (exp instanceof Map) {
				Map<String, Object> opt = ((Map) exp);
				if(opt.containsKey(BsonRegularExpression.REGEX)) {
					text = opt.get(BsonRegularExpression.REGEX);
					RegexpQuery regQuery = new RegexpQuery(new Term(field,text.toString()));					
					textQuery = regQuery;
				}
				else if(opt.containsKey(BsonRegularExpression.TEXT)) {
					text = opt.get(BsonRegularExpression.TEXT);
					SimpleQueryParser parser = new SimpleQueryParser(access.getFieldAnalyzer(field), field); // 定义查询分析器
					parser.setDefaultOperator(BooleanClause.Occur.MUST);
					textQuery = parser.parse(text.toString());
				}
				else if(opt.containsKey(BsonRegularExpression.SEARCH)) {
					text = opt.get(BsonRegularExpression.SEARCH);
					// 更复杂，支持多种字段
					
					StandardQueryParser parser = new StandardQueryParser(access.analyzerWrapper); // 定义查询分析器
					parser.setFieldsBoost(weights);
					textQuery = parser.parse(text.toString(),field);
					
				}
				
				if(opt.containsKey("$limit")) {
					limit = Integer.parseInt(opt.get("$limit").toString());
				}
				
				if(opt.containsKey("$sort")) {
					String sortOpt = opt.get("$sort").toString();
					sortField = new SortField(sortOpt,Type.DOUBLE, true);
				}
			}
			if(textQuery==null) {
				throw new IllegalArgumentException("Query strign is not set!");
			}
			
			Query query = textQuery;
			
			// Lucene 3 insists on a hard limit and will not provide
			// a total hits value. Take at least 100 which is
			// an optimal limit for Lucene as any more
			// will trigger writing results to disk.
			int maxResults = GridLuceneIndex.DEAULT_LIMIT;
			if(limit>0) {
				maxResults = limit;
			}
			TopDocs docs = null;
			if(sortField==null)
				docs = searcher.search(query, maxResults);
			else
				docs = searcher.search(query, maxResults, new Sort(sortField));
			limit = docs.scoreDocs.length;
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				ScoreDoc sd = docs.scoreDocs[i];
				org.apache.lucene.document.Document doc = searcher.doc(sd.doc);
				float score = sd.score;

				Object k = unmarshalKeyField(doc.getBinaryValue(KEY_FIELD_NAME), cache, ldr);
				String v = doc.get(field);
				
				Document meta = new Document("searchScore",score);
				if(i==0) {
					meta.append("totalHits", docs.totalHits.value);
				}
				result.add(new IdWithMeta(k,false,meta).indexValue(v));

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

	void setFirstIndex(boolean isFirstIndex) {
		this.isFirstIndex = isFirstIndex;
	}
	
	public boolean isTextIndex() {
		for(IndexKey ind: this.getKeys()) {
			if(ind.isText()) return true;
		}
		return false;
	}

}
