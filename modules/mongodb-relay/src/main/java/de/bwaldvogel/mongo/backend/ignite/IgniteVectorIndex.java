package de.bwaldvogel.mongo.backend.ignite;

import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.transformToListOrdered;
import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.tryToAddIntoHeap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.dataset.feature.extractor.ExtractionUtils.IntCoordObjectLabelVectorizer;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.KNNPartitionDataBuilder;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndexType;
import org.apache.ignite.ml.math.distances.CosineSimilarity;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.QueryOperator;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.ignite.util.DocumentUtil;
import de.bwaldvogel.mongo.backend.ignite.util.EmbeddingUtil;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public class IgniteVectorIndex extends Index<Object> {
	
	 /** Learning environment builder. */
    protected LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /** Learning Environment. */
    protected LearningEnvironment environment = envBuilder.buildForTrainer();

	private final String cacheName;	
	
	private IgniteCache<Object, Vector> vecIndex;	
	
	private final GridKernalContext ctx;
	
	
	/** Dataset with {@link SpatialIndex} as a partition data. */
    private Dataset<EmptyContext, SpatialIndex<Object>> knnDataset;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure;
    
    /** Index type. */
    private SpatialIndexType idxType = SpatialIndexType.KD_TREE; 
	
	private String keyField = "_id";
	
	private int K = 20;
	
	
	static class EmbeddingIntCoordObjectLabelVectorizer implements FeatureLabelExtractor<Object,Vector,Object>{
		
		private static final long serialVersionUID = 1L;

		@Override
		public LabeledVector<Object> apply(Object k, Vector v) {			
			return extract(k, v);
		}

		@Override
		public LabeledVector<Object> extract(Object k, Vector v) {
			LabeledVector<Object> f = new  LabeledVector<>(v,k);
			return f;
		}		
		
	}
	

	protected IgniteVectorIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys, boolean sparse) {
		super(name, keys, sparse);
		this.ctx = ctx;
		this.cacheName = collection.getCollectionName();
		this.keyField = collection.idField;
		this.distanceMeasure = new CosineSimilarity();
		//this.distanceMeasure = new EuclideanDistance();
		if(sparse) {
			idxType = SpatialIndexType.ARRAY;
		}
		
		
		CacheConfiguration<Object, Vector> cfg = new CacheConfiguration<>();        	
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(IgniteDatabase.getIndexCacheName(collection.getDatabaseName(),this.cacheName,this.getName()));
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC); 
        cfg.setBackups(0);
       
        vecIndex = ctx.grid().getOrCreateCache(cfg);
        
		init();
	}
	

	public void init() {
		if (this.knnDataset == null) {
			try {
				CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl) ctx.cacheObjects();				
				 
				EmbeddingIntCoordObjectLabelVectorizer vectorizer = new EmbeddingIntCoordObjectLabelVectorizer();
		                   

				CacheBasedDatasetBuilder<Object, Vector> datasetBuilder = new CacheBasedDatasetBuilder<>(ctx.grid(), vecIndex);
				
				knnDataset = datasetBuilder.build(
		            envBuilder,
		            new EmptyContextBuilder<>(),
		            new KNNPartitionDataBuilder<>(vectorizer, idxType, distanceMeasure),
		            environment
		        );

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
	}
	
	public Vector computeEmbedding(Document document) {
		Vector vec = null;
		for(String field : keys()) {
			Object val = document.get(field);
			vec = computeValueEmbedding(val);
			if(vec!=null) break;
		}
		return vec;
	}
	
	

	public Vector computeValueEmbedding(Object val) {
		Vector vec = null;
		if(val instanceof float[]) {
			float[] fdata = (float[])val;
			double[] data = new double[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = fdata[i];
			}
			vec = new DenseVector(data);
		}
		else if(val instanceof double[]) {
			vec = new DenseVector((double[])val);
		}
		else if(val instanceof Number[]) {
			Number[] fdata = (Number[])val;
			double[] data = new double[fdata.length];
			for(int i=0;i<fdata.length;i++) {
				data[i] = fdata[i].doubleValue();
			}
			vec = new DenseVector(data);
		}
		else if(val instanceof List) {
			List<Number> fdata = (List)val;
			double[] data = new double[fdata.size()];
			for(int i=0;i<fdata.size();i++) {
				data[i] = fdata.get(i).doubleValue();
			}
			vec = new DenseVector(data);
		}
		else if(val instanceof String) {
			if(this.isSparse()) {
				vec = EmbeddingUtil.textTwoGramVec(val.toString());
			}
			else {			
				vec = EmbeddingUtil.textXlmVec(val.toString());
			}
		}
		return vec;
	}

	
	  /** {@inheritDoc} */
    public List<LabeledVector<Object>> findKClosest(int k, Vector pnt) {
    	List<LabeledVector<Object>> res = knnDataset.compute(spatialIdx -> spatialIdx.findKClosest(k, pnt), (a, b) -> {
            Queue<PointWithDistance<Object>> heap = new PriorityQueue<>(distanceMeasure.isSimilarity()?Collections.reverseOrder():null);
            tryToAddIntoHeap(heap, k, pnt, a, distanceMeasure);
            tryToAddIntoHeap(heap, k, pnt, b, distanceMeasure);
            return transformToListOrdered(heap);
        });

        return res == null ? Collections.emptyList() : res;
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


	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {		

		String typeName = collection.getCollectionName();
		
		boolean stringsFound = false;

		Vector vec = this.computeEmbedding(document);
		if(vec==null) {
			return ;
		}
		
		// build doc body
		try {
			
			vecIndex.put(position, vec);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Object remove(Document document, MongoCollection<Object> collection) {
		IgniteBinaryCollection coll = (IgniteBinaryCollection) collection;
		Object key = getPosition(document);
		try {
			vecIndex.remove(key);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return key;
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
					return false;
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
					else if (this.isTextIndex() && queriedKeys.startsWith("$rnnVector")) {
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
			if (isCompoundIndex()) {
				throw new UnsupportedOperationException("Not yet implemented");
			}
					
			// for $text value  { textField : { $text: 'keyword' } }
			if (BsonRegularExpression.isTextSearchExpression(queriedKey)) {				
				List<Object> positions = new ArrayList<>();
				for (Entry<KeyValue, Object> entry : getVectorTextList(queriedKey)) {
					KeyValue obj = entry.getKey();
					if (obj.size() >= 1) {
						Object o = obj.get(0);
						if(o!=null) {
							positions.add(entry.getValue());
						}
					}
				}
				query.remove(this.keys().get(n));
				return positions;
			} 
			
			// for { $text : { $search: 'keyword' } } || { $text : { $rnnVector: [0.1,0.4,0.6] } }
			if(queriedKey == null && this.isTextIndex() && query.containsKey("$text")) {
				queriedKey = query.get("$text");
			}	
			if (queriedKey instanceof Document) {				
				Document keyObj = (Document) queriedKey;
				if (Utils.containsQueryExpression(keyObj)) {
					Set<String> expression = keyObj.keySet();
					
					if (expression.contains(QueryOperator.SEARCH.getValue())) {						
						searchKey = searchKey.copyFrom(n, keyObj);						
					}					
					else if (expression.contains(QueryOperator.IN.getValue())) {
						return getPositionsForExpression(keyObj, QueryOperator.IN.getValue());
					}
				}
			}
			n++;
		}

		List<Object> positions = getPosition(searchKey);
		if (positions == null) {
			return Collections.emptyList();
		}
		return positions;
	}

	@Override
	public long getCount() {
		
		return vecIndex.localSizeLong(CachePeekMode.ALL);
	}

	@Override
	public long getDataSize() {		
		return vecIndex.localMetrics().getCacheSize();
	}

	@Override
	public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Object> collection) {
		// TODO Auto-generated method stub

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
		try {
			this.knnDataset.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.vecIndex.destroy();		
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

	protected List<Object> getPosition(KeyValue keyValue) {
		List<Object> result = new ArrayList<>();		

		try {
			int n = 0;
			int limit = 0;
			
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
				
				Vector vec = this.computeValueEmbedding(obj);
				
				int maxResults = limit;
				if(limit==0) {
					maxResults = K;
				}
				List<LabeledVector<Object>> docs = this.findKClosest(maxResults, vec);
				limit = (int) docs.size();
				for (int i = 0; i < limit; i++) {
					LabeledVector<Object> sd = docs.get(i);				
					float score = sd.weight();
					if(!distanceMeasure.isSimilarity() || score>0) {
						result.add(sd.label());
					}

				}
				n++;
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 对字符串字段进行向量搜索查询，支持模糊匹配
	 * 
	 * @param text
	 * @return
	 */
	protected List<Entry<KeyValue, Object>> getVectorTextList(Object exp) {

		List<Entry<KeyValue, Object>> result = new ArrayList<>();
		try {
			int limit = 0;
			String scoreField = null;
			Object obj = exp;
			if (exp instanceof Map) {
				Map<String, Object> opt = ((Map<String, Object>) obj);
				
				if(opt.containsKey("$knnVector")) {
					obj = opt.get("$knnVector");
				}				
				else if(opt.containsKey("$search")) {
					obj = opt.get("$search");
				}
				else if(opt.containsKey("$text")) {
					obj = opt.get("$text");
				}				
				
				if(opt.containsKey("$limit")) {
					limit = Integer.parseInt(opt.get("$limit").toString());
				}				
			}

			Vector vec = this.computeValueEmbedding(obj);
			
			int maxResults = limit;
			if(limit==0) {
				maxResults = K;
			}
			List<LabeledVector<Object>> docs = this.findKClosest(maxResults, vec);
			limit = (int) docs.size();
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				
				LabeledVector<Object> doc = docs.get(i);
				float score = doc.weight();
				if(!distanceMeasure.isSimilarity() || score>0) {
					Object k = doc.label();
					Vector v = doc.features();

					result.add(new IgniteBiTuple<KeyValue, Object>(KeyValue.valueOf(v), k));
				}
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}

	private static boolean isInQuery(String key) {
		return key.equals(QueryOperator.IN.getValue());
	}
	
	public boolean isTextIndex() {
		return true;
	}

}
