package de.bwaldvogel.mongo.backend.ignite;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;

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
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;

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

public class IgniteLuceneIndex extends Index<Object> {

	private final String cacheName;

	private LuceneIndexAccess indexAccess;

	private final GridKernalContext ctx;

	private GridBinaryMarshaller marshaller;

	private boolean isFirstIndex = false;

	private long docCount = 0;
	
	private String keyField = "_id";

	/** */
	private String[] idxdFields = null;
	private FieldType[] idxdTypes = null;

	protected IgniteLuceneIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys,	boolean sparse) {
		super(name, keys, sparse);
		this.ctx = ctx;
		this.cacheName = collection.getCollectionName();
		this.keyField = collection.idField;

		init();

	}

	public void init() {
		if (indexAccess == null) {
			try {
				indexAccess = LuceneIndexAccess.getIndexAccess(ctx, cacheName);

				CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl) ctx.cacheObjects();

				marshaller = cacheObjProc.marshaller();
				// marshaller = PlatformUtils.marshaller();
				String typeName = IgniteBinaryCollection.tableOfCache(cacheName);
				Map<String, FieldType> fields = indexAccess.fields(typeName);
				for (IndexKey ik : this.getKeys()) {
					fields.put(ik.getKey(), TextField.TYPE_NOT_STORED);
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.idxdFields = null;
		this.idxdTypes = null;
	}

	@Override
	public Object getPosition(Document document) {
		// Set<KeyValue> keyValues = getKeyValues(document);
		Object key = document.getOrDefault(keyField, null);
		if (key != null) {
			return DocumentUtil.toBinaryKey(key);
		}
		return null;
	}	

	@Override
	public void checkAdd(Document document, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub

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
				igniteIndex.init();
				if (fields.isEmpty()) {
					igniteIndex.setFirstIndex(true);
				} else {
					igniteIndex.setFirstIndex(false);
				}
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

		String typeName = collection.getCollectionName();
		Object key = null;
		String keyField = "_id";
		if (collection instanceof IgniteBinaryCollection) {

			if (!this.isFirstIndex)
				return;
			
			IgniteBinaryCollection coll = (IgniteBinaryCollection) collection;
			T2<String, String> t2 = coll.typeNameAndKeyField(coll.dataMap, document);
			typeName = t2.get1();
			keyField = t2.get2();
			key = document.getOrDefault(coll.idField, null);
			Map<String, FieldType> fields = indexAccess.fields(typeName);
			for (IndexKey ik : this.getKeys()) {
				if (ik.isText()) {
					fields.putIfAbsent(ik.getKey(), TextField.TYPE_NOT_STORED);
				} else {
					fields.putIfAbsent(ik.getKey(), StringField.TYPE_STORED);
				}
			}

			IgniteH2Indexing idxing = (IgniteH2Indexing) ctx.query().getIndexing();

			@Nullable
			Collection<TableDescriptor> tables = idxing.schemaManager().tablesForCache(cacheName);
			for(TableDescriptor table: tables) {
				if (table.type().name().equalsIgnoreCase(typeName) && table.type().textIndex() != null) {
					return;
				}
			}
		}

		org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();

		boolean stringsFound = false;

		Object[] row = new Object[idxdFields.length];
		for (int i = 0, last = idxdFields.length; i < last; i++) {
			Object fieldVal = document.get(idxdFields[i]);
			if (fieldVal == null) {
				continue;
			}
			if (idxdTypes[i].tokenized() && fieldVal instanceof CharSequence) {
				row[i] = new TextField(idxdFields[i],fieldVal.toString(),Store.NO);				
			}
			else if(fieldVal instanceof Number) {
				Number obj = (Number) fieldVal;
				if (obj instanceof Long) {
					row[i] = new LongPoint(idxdFields[i], (obj).longValue());
					
				} 
				else if (obj instanceof Integer || obj instanceof Short) {
					row[i] = new IntPoint(idxdFields[i], (obj).intValue());
					
				} 
				else if (obj instanceof Float) {
					row[i] = new FloatPoint(idxdFields[i], (obj).floatValue());
					
				}
				else {
					double d = ((Number) obj).doubleValue();						
					row[i] = new DoublePoint(idxdFields[i], d);
				}
			} 
			else {
				//byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(fieldVal), false);
				//BytesRef keyByteRef = new BytesRef(keyBytes);
				row[i] = fieldVal;
			}
		}
		
		BytesRef keyByteRef = marshalKeyField(position);
		Term term = new Term(KEY_FIELD_NAME, keyByteRef);
		// build doc body
		try {
			stringsFound = FullTextLucene.buildDocument(doc, this.idxdFields, idxdTypes, null, row);
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
				if (isCompoundIndex() && !this.isTextIndex()) {
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
					} 
					else if (this.isTextIndex() && queriedKeys.startsWith("$search")) {
						// not yet supported
						return true;
					}
					else if (queriedKeys.startsWith("$")) {
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
		final KeyValue queriedKeys = getQueriedKeys(query);
		KeyValue searchKey = queriedKeys;		
		int n = 0;
		for (Object queriedKey : queriedKeys.iterator()) {
			// for $text value  { textField : { $text: 'keyword' } }			
			if (BsonRegularExpression.isRegularExpression(queriedKey)) { // { textField : { $regex: 'keyword' } }
				
				List<Object> positions = new ArrayList<>();
				for (KeyValue obj : getFullTextList(this.keys().get(n), queriedKey)) {					
					if (obj.size() > 2) { // k, score, v
						Object v = obj.get(2);
						if (v!=null) {
							BsonRegularExpression regularExpression = BsonRegularExpression.convertToRegularExpression(queriedKey);
							Matcher matcher = regularExpression.matcher(v.toString());
							if (matcher.find()) {
								positions.add(obj.get(0));
							}
						}
					}
				}
				query.remove(this.keys().get(n));
				return positions;
			} else if (BsonRegularExpression.isTextSearchExpression(queriedKey)) { // { textField : { $text: 'keyword' } }
				
				List<KeyValue> positions = getFullTextList(this.keys().get(n), queriedKey);				
				query.remove(this.keys().get(n));
				return (List)positions;
			} 
			
			// for { $text : { $search: 'keyword' } } || { $text : { $knnVector: [0.1,0.4,0.6] } }
			if(queriedKey == null && this.isTextIndex() && query.containsKey("$text")) {
				queriedKey = query.get("$text");
			}
			if (queriedKey instanceof Document) {
				if (isCompoundIndex() && !this.isTextIndex()) {
					throw new UnsupportedOperationException("Not yet implemented");
				}
				Document keyObj = (Document) queriedKey;
				if (Utils.containsQueryExpression(keyObj)) {
					Set<String> expression = keyObj.keySet();
					
					if (expression.contains(QueryOperator.SEARCH.getValue())) {											
						searchKey = searchKey.copyFrom(n, keyObj);
						query.remove(this.keys().get(n));
					}					
					else if (expression.contains(QueryOperator.IN.getValue())) {
						return (List)getPositionsForExpression(keyObj, QueryOperator.IN.getValue());
					}
				}
			}
			n++;
		}

		List<KeyValue> positions = getPosition(searchKey);
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
		return indexAccess.writer.getDocStats().numDocs;
	}

	@Override
	public long getDataSize() {		
		return indexAccess.writer.getFlushingBytes();		
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
	}

	private Iterable<KeyValue> getPositionsForExpression(Document keyObj, String operator) {
		if (isInQuery(operator)) {
			@SuppressWarnings("unchecked")
			Collection<Object> objects = (Collection<Object>) keyObj.get(operator);
			Collection<Object> queriedObjects = new TreeSet<>(ValueComparator.asc());
			queriedObjects.addAll(objects);

			List<KeyValue> allKeys = new ArrayList<>();
			for (Object object : queriedObjects) {

				Object keyValue = Utils.normalizeValue(object);
				List<KeyValue> keys = getPosition(KeyValue.valueOf(keyValue));
				if (keys != null) {
					allKeys.addAll(keys);
				} else {
					return null;
				}

			}

			return allKeys;
		}
		else {
			throw new UnsupportedOperationException("unsupported query expression: " + operator);
		}
	}

	protected List<KeyValue> getPosition(KeyValue keyValue) {
		List<KeyValue> result = new ArrayList<>();
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
			String scoreField = null;

			// take a reference as the searcher may change
			IndexSearcher searcher = access.searcher;
			// reuse the same analyzer; it's thread-safe;
			// also allows subclasses to control the analyzer used.			
			// Filter expired items.
			//-Query filter = LongPoint.newRangeQuery(FullTextLucene.EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(),Long.MAX_VALUE);

			BooleanQuery.Builder query = new BooleanQuery.Builder();
			int n = 0;
			for (IndexKey key : this.getKeys()) {
				Object obj = keyValue.get(n);
				if(obj == null) {
					n++;
					continue;
				}
				
				if (obj instanceof Map) {
					Map<String, Object> opt = ((Map<String, Object>) obj);
					if(opt.containsKey("$search")) {
						obj = opt.get("$search");
					}
					else if(opt.containsKey("$knnVector")) {
						obj = opt.get("$knnVector");
					}
					
					if(opt.containsKey("$limit")) {
						limit = Integer.parseInt(opt.get("$limit").toString());
					}				
				}
				
				if(key.isText()) {
					QueryParser parser = new QueryParser(key.getKey(), access.getFieldAnalyzer(key.getKey())); // 定义查询分析器
					
					Query textQuery = parser.parse(obj.toString());
					query.add(textQuery, BooleanClause.Occur.MUST);
				}
				else if(obj instanceof ObjectId || obj instanceof UUID) {									
					byte[] keyBytes = marshaller.marshal(ctx.grid().binary().toBinary(obj),false);
		            BytesRef keyByteRef = new BytesRef(keyBytes);	
		            Term term = new Term(key.getKey(), keyByteRef);
					Query termQuery = new TermQuery(term);
					query.add(termQuery, BooleanClause.Occur.MUST);
		     	}

				else if (obj instanceof Number) {
					if (obj instanceof Long ) {
						Query termQuery = LongPoint.newExactQuery(key.getKey(), ((Number) obj).longValue());
						query.add(termQuery, BooleanClause.Occur.MUST);
					}
					else if (obj instanceof Integer || obj instanceof Short) {
						Query termQuery = IntPoint.newExactQuery(key.getKey(), ((Number) obj).intValue());
						query.add(termQuery, BooleanClause.Occur.MUST);
					}
					else if (obj instanceof Float ) {
						Query termQuery = FloatPoint.newExactQuery(key.getKey(), ((Number) obj).floatValue());
						query.add(termQuery, BooleanClause.Occur.MUST);
					}
					else {
						double d = ((Number) obj).doubleValue();						
						Query termQuery = DoublePoint.newExactQuery(key.getKey(),d);
						query.add(termQuery, BooleanClause.Occur.MUST);
					}
				} else if (obj instanceof byte[]) {
					Term term = new Term(key.getKey(), new BytesRef((byte[]) obj));
					Query termQuery = new TermQuery(term);
					query.add(termQuery, BooleanClause.Occur.MUST);
				} else {
					Term term = new Term(key.getKey(), obj.toString());
					Query termQuery = new TermQuery(term);
					query.add(termQuery, BooleanClause.Occur.MUST);
				}
				n++;
			}
			// query.add(filter, BooleanClause.Occur.FILTER);
			
			// Lucene 3 insists on a hard limit and will not provide
			// a total hits value. Take at least 100 which is
			// an optimal limit for Lucene as any more
			// will trigger writing results to disk.
			int maxResults = Integer.MAX_VALUE;
			TopDocs docs = searcher.search(query.build(), maxResults);
			limit = (int) docs.totalHits.value;
			
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				ScoreDoc sd = docs.scoreDocs[i];
				org.apache.lucene.document.Document doc = searcher.doc(sd.doc);
				float score = sd.score;

				Object k = unmarshalKeyField(doc.getBinaryValue(KEY_FIELD_NAME), cache, ldr);

				result.add(KeyValue.valueOf(k,score));

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
	protected List<KeyValue> getFullTextList(String field, Object exp) {
		LuceneIndexAccess access = indexAccess;		
		int limit = 0;
		List<KeyValue> result = new ArrayList<>();
		try {

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
			
			Object text = exp;
			QueryParser parser = new QueryParser(field, access.getFieldAnalyzer(field)); // 定义查询分析器
			if (exp instanceof Map) {
				Map<String, Object> opt = ((Map<String, Object>) exp);
				if(opt.containsKey(BsonRegularExpression.REGEX)) {
					text = opt.get(BsonRegularExpression.REGEX);
				}
				else if(opt.containsKey(BsonRegularExpression.TEXT)) {
					text = opt.get(BsonRegularExpression.TEXT);
				}
				else if(opt.containsKey(BsonRegularExpression.SEARCH)) {
					text = opt.get(BsonRegularExpression.SEARCH);
				}
				
				if(opt.containsKey("$limit")) {
					limit = Integer.parseInt(opt.get("$limit").toString());
				}				
			}
			
			Query query = parser.parse(text.toString());
			
			// Lucene 3 insists on a hard limit and will not provide
			// a total hits value. Take at least 100 which is
			// an optimal limit for Lucene as any more
			// will trigger writing results to disk.
			int maxResults = Integer.MAX_VALUE;
			if(limit>0) {
				maxResults = limit;
			}
			TopDocs docs = searcher.search(query, maxResults);
			limit = (int) docs.totalHits.value;
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				ScoreDoc sd = docs.scoreDocs[i];
				org.apache.lucene.document.Document doc = searcher.doc(sd.doc);
				float score = sd.score;

				Object k = unmarshalKeyField(doc.getBinaryValue(KEY_FIELD_NAME), cache, ldr);
				String v = doc.get(field);

				result.add(KeyValue.valueOf(v,score,k));

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
	
	public boolean isTextIndex() {
		for(IndexKey ind: this.getKeys()) {
			if(ind.isText()) return true;
		}
		return false;
	}

}
