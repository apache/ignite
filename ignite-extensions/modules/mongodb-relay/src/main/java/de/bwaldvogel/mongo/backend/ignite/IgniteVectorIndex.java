package de.bwaldvogel.mongo.backend.ignite;

import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.transformToListOrdered;
import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.tryToAddIntoHeap;

import java.io.Serializable;
import java.nio.file.Paths;
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
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.dataset.feature.extractor.ExtractionUtils.IntCoordObjectLabelVectorizer;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.KNNPartitionDataBuilder;
import org.apache.ignite.ml.knn.ann.ANNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.knn.utils.PointWithDistanceUtil;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndexType;
import org.apache.ignite.ml.math.distances.CosineSimilarity;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.DotProductSimilarity;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.distances.JaccardIndex;
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
	
	/** knn */
    private KNNClassificationModel<Object> knnModel;
    /** ann */
    private ANNClassificationModel<Object> annModel;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure;
    
    private String indexType = SpatialIndexType.BALL_TREE.name();
	
	private String idField = "_id";
	
	private int K = 20;
	
	private int dimensions = 768;
	
	private boolean defaultANN = false;
	
	private String embeddingModelName = "text2vec-base-chinese-paraphrase";
	
	private String tokenizerModelName = "tokenizers/perceiver-ar-xlnet-large";
	
	private String modelUrl = null;	
	
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
	

	public IgniteVectorIndex(GridKernalContext ctx, IgniteBinaryCollection collection, String name, List<IndexKey> keys, boolean sparse) {
		super(name, keys, sparse);
		this.ctx = ctx;
		this.cacheName = collection.getCollectionName();
		this.idField = collection.idField;
		this.distanceMeasure = new CosineSimilarity();
		
		if(sparse) {
			indexType = SpatialIndexType.ARRAY.name();
		}
		
		Document options = keys.get(0).textOptions();
		if(options!=null) {
			
			dimensions = Integer.parseInt(options.getOrDefault("dimensions", dimensions).toString());
			
			String similarity = options.getOrDefault("similarity", "").toString();
			if(similarity.equals("euclidean ")) {
				this.distanceMeasure = new EuclideanDistance();
			}
			else if(similarity.equals("cosine")) {
				this.distanceMeasure = new CosineSimilarity();
			}
			else if(similarity.equals("dotProduct")) {
				this.distanceMeasure = new DotProductSimilarity();
			}
			else if(!similarity.isBlank()) {
				try {
					this.distanceMeasure = DistanceMeasure.of(similarity);
				} catch (InstantiationException e) {
					e.printStackTrace();
				}
			}			
			
			String indexType = options.getOrDefault("indexType", "").toString().toUpperCase();
			if(!indexType.isBlank()) {
				if(indexType.startsWith("ANN") || indexType.startsWith("IVF")) {
					defaultANN =  true;
				}
				this.indexType = indexType;
			}
			
			// 句向量模型
			embeddingModelName = (String)options.getOrDefault("modelId", "chinese");
			if(embeddingModelName.equals("chinese")) {
				embeddingModelName = "text2vec-base-chinese-paraphrase";
			}
			else if(embeddingModelName.equals("multilingual")) {
				embeddingModelName = "paraphrase-xlm-r-multilingual";
			}	
			// IDF词典模型
			tokenizerModelName = (String)options.getOrDefault("tokenizerId", tokenizerModelName);
		}
		
		String igniteHome = ctx.grid().configuration().getIgniteHome();
		
		try {
			IgniteFileSystem fs = ctx.grid().fileSystem("models");
			if(fs.exists(new IgfsPath(embeddingModelName))) {
				modelUrl = "s3://models/" + (sparse?tokenizerModelName:embeddingModelName);
			}
			else {
				modelUrl = igniteHome+"/models/"+(sparse?tokenizerModelName:embeddingModelName);
			}
		}
		catch(IllegalArgumentException e) {			
			modelUrl = igniteHome+"/models/"+(sparse?tokenizerModelName:embeddingModelName);
		}
		
		CacheConfiguration<Object, Vector> cfg = new CacheConfiguration<>();        	
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(IgniteDatabase.getIndexCacheName(collection.getDatabaseName(),this.cacheName,this.getName()));
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC); 
        cfg.setBackups(0);
        
        vecIndex = ctx.grid().getOrCreateCache(cfg);
        
        init(collection);
	}
	

	public void init(IgniteBinaryCollection collection) {
				
	}
	
	public KNNClassificationModel<Object> knnModel(){
		if(this.knnModel==null) {
			synchronized(this){
				if(this.knnModel==null) {
					/** Index type. */
				    SpatialIndexType idxType = SpatialIndexType.KD_TREE;
				    try {
				    	if(indexType.startsWith("ANN") || indexType.startsWith("IVF")) {
							defaultANN =  true;
						}
				    	else {
				    		idxType = SpatialIndexType.valueOf(indexType.toUpperCase());
				    	}
				    }
				    catch(Exception e) {
				    	
				    }
				    
					EmbeddingIntCoordObjectLabelVectorizer vectorizer = new EmbeddingIntCoordObjectLabelVectorizer();
					CacheBasedDatasetBuilder<Object, Vector> datasetBuilder = new CacheBasedDatasetBuilder<>(ctx.grid(), vecIndex);
					
					CacheBasedDataset<Object, Vector, EmptyContext, SpatialIndex<Object>> knnDataset = datasetBuilder.build(
				            envBuilder,
				            new EmptyContextBuilder<>(),
				            new KNNPartitionDataBuilder<>(vectorizer, idxType, distanceMeasure),
				            environment
				        );
					this.knnModel = new KNNClassificationModel<Object>(knnDataset,distanceMeasure,this.K, false);
				}
			}			
		}
		return knnModel;
	}
	
	public ANNClassificationModel<Object> annModel() {
		if(this.annModel==null) {
			synchronized(this){
				if(this.annModel==null) {
					EmbeddingIntCoordObjectLabelVectorizer vectorizer = new EmbeddingIntCoordObjectLabelVectorizer();
					CacheBasedDatasetBuilder<Object, Vector> datasetBuilder = new CacheBasedDatasetBuilder<>(ctx.grid(), vecIndex);
					int K = 2+(int)Math.sqrt(dimensions*2);
					ANNClassificationTrainer<Object> annTrainer = new ANNClassificationTrainer<Object>()
							.withEnvironmentBuilder(envBuilder)
							.withMaxIterations(2+K/5)
							.withDistance(distanceMeasure)
							.withK(K);
					
					annModel = annTrainer.update(annModel, datasetBuilder, vectorizer);
					annModel.withWeighted(true);
					annModel.withDistanceMeasure(distanceMeasure);
				}
			}		
		}
		return annModel;
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
				vec = EmbeddingUtil.textTwoGramVec(val.toString(),modelUrl);
			}
			else {			
				vec = EmbeddingUtil.textXlmVec(val.toString(),modelUrl);
			}
		}
		return vec;
	}
	
	  /** {@inheritDoc} */
    public List<LabeledVector<Object>> findKClosest(int k, Vector pnt, boolean ann) {
    	if(ann) {
    		List<LabeledVector<Object>> list = this.annModel().findKClosestLabels(k, pnt, this.vecIndex);
    		return list;
    	}
    	
    	Collection<PointWithDistance<Object>> res = knnModel().findKClosest(k, pnt);   	

        return res == null ? Collections.emptyList() : PointWithDistanceUtil.transformToListOrdered(res);
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
	}


	@Override
	public void add(Document document, Object position, MongoCollection<Object> collection) {		

		String typeName = collection.getCollectionName();
		
		// build doc body
		try {
			Vector vec = this.computeEmbedding(document);
			if(vec==null) {
				vecIndex.removeAsync(position);
				return ;
			}
			
			vecIndex.putAsync(position, vec);

		} catch (Exception e) {			
			e.printStackTrace();
		}
	}

	@Override
	public Object remove(Document document, MongoCollection<Object> collection) {		
		Object key = getPosition(document);
		try {
			vecIndex.removeAsync(key);
			
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
				Document queryDoc = (Document) queryValue;
				
				if (BsonRegularExpression.isTextSearchExpression(queryValue)) {
					continue;
				}
				for (String queriedKeys : ((Document) queryValue).keySet()) {
					if (isInQuery(queriedKeys)) {
						// okay
					} 
					else if (queriedKeys.startsWith("$search")) {						
						return true;
					}
					else if (queriedKeys.startsWith("$knnVector") || queriedKeys.startsWith("$annVector")) {						
						return true;
					}
					else if (queriedKeys.startsWith("$")) {
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
		List<Object> all = new ArrayList<>(this.K);
		
		for (Object queriedKey : queriedKeys.iterator()) {
			IndexKey indexKey = this.getKeys().get(n);			
					
			// for $text value  { textField : { $text: 'keyword' } }
			if (BsonRegularExpression.isTextSearchExpression(queriedKey)) {				
				List<IdWithMeta> positions = getVectorTextList(indexKey,queriedKey);				
				query.remove(indexKey.getKey());
				all.addAll(positions);
				searchKey = searchKey.copyFrom(n, null);
				n++;
				continue;
			}
			
			// for { $text : { $search: 'keyword' } } || { $text : { $knnVector: [0.1,0.4,0.6] } }
			if(queriedKey == null && this.isTextIndex() && query.containsKey("$text")) {
				queriedKey = query.get("$text");
			}	
			if (queriedKey instanceof Document) {				
				Document keyObj = (Document) queriedKey;
				if (Utils.containsQueryExpression(keyObj)) {
					Set<String> expression = keyObj.keySet();
					
					if (expression.contains(QueryOperator.SEARCH.getValue())) {						
						searchKey = searchKey.copyFrom(n, keyObj);						
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.KNN_VECTOR.getValue())) {						
						searchKey = searchKey.copyFrom(n, keyObj);
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.ANN_VECTOR.getValue())) {						
						searchKey = searchKey.copyFrom(n, keyObj);
						query.remove(indexKey.getKey());
					}
					else if (expression.contains(QueryOperator.IN.getValue())) {
						List<IdWithMeta> positions = getPositionsForInExpression(indexKey,keyObj.get(QueryOperator.IN.getValue()), QueryOperator.IN.getValue());
						query.remove(indexKey.getKey());						
						all.addAll(positions);
						searchKey = searchKey.copyFrom(n, null);
					}
				}
			}
			n++;
		}

		List<IdWithMeta> positions = getPosition(searchKey);
		if (positions!=null) {
			if(all.size()==0) {
				return (List)positions;
			}
			all.addAll(positions);
		}
		return all;
	}

	@Override
	public long getCount() {		
		return vecIndex.sizeLong(CachePeekMode.ALL);
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
			if (this.knnModel != null) {
				this.knnModel.close();
			}	
			if (this.annModel != null) {
				this.annModel.close();
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.vecIndex.destroy();		
	}
	
	void close() {
		try {
			if (this.knnModel != null) {
				this.knnModel.close();
				this.knnModel = null;
			}			
			if (this.annModel != null) {
				this.annModel.close();
				this.annModel = null;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private List<IdWithMeta> getPositionsForInExpression(IndexKey indexKey, Object value, String operator) {
		if (isInQuery(operator)) {
			@SuppressWarnings("unchecked")
			Collection<Object> objects = (Collection<Object>) value;
			Collection<Object> queriedObjects = new TreeSet<>(ValueComparator.asc());
			queriedObjects.addAll(objects);

			List<IdWithMeta> allKeys = new ArrayList<>();
			for (Object object : queriedObjects) {
				
				List<IdWithMeta> keys = getVectorTextList(indexKey,object);
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
	/**
	 *  可以获取多个字段的匹配结果，拼接在一起返回
	 * @param keyValue
	 * @return
	 */
	protected List<IdWithMeta> getPosition(KeyValue keyValue) {
		List<IdWithMeta> result = new ArrayList<>();		

		try {
			int n = 0;
			int limit = 0;
			float scoreMax = Float.MAX_VALUE;
			for (IndexKey key : this.getKeys()) {
				Object obj = keyValue.get(n);
				if(obj == null) {
					n++;
					continue;
				}
				boolean useAnn = this.defaultANN;
				if (obj instanceof Map) {
					Map<String, Object> opt = (Map) obj;
					if(opt.containsKey("$knnVector")) {
						obj = opt.get("$knnVector");
						useAnn = false;
					}
					else if(opt.containsKey("$annVector")) {
						obj = opt.get("$annVector");
						useAnn = true;
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
					
					if(opt.containsKey("$max")) {
						scoreMax = Float.parseFloat(opt.get("$max").toString());
					}
					
					if(opt.containsKey("$indexType")) {					
						String indexType = opt.get("$indexType").toString().toUpperCase();
						if(!indexType.equals(this.indexType)) {
							this.close();
							this.indexType = indexType;
						}						
					}
				}				
				
				Vector vec = this.computeValueEmbedding(obj);
				
				int maxResults = limit;
				if(limit==0) {
					maxResults = K;
				}
				List<LabeledVector<Object>> docs = this.findKClosest(maxResults, vec, useAnn);
				limit = (int) docs.size();
				for (int i = 0; i < limit; i++) {
					LabeledVector<Object> sd = docs.get(i);				
					float score = sd.weight();
					if(score<=scoreMax) {		// 越小越好	
						result.add(new IdWithMeta(sd.label(),true,new Document("vectorScore",score)));
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
	protected List<IdWithMeta> getVectorTextList(IndexKey indexKey, Object exp) {
		List<IdWithMeta> result = new ArrayList<>();
		try {
			int limit = 0;			
			Object obj = exp;
			float scoreMax = Float.MAX_VALUE;
			boolean useAnn = defaultANN;
			if (exp instanceof Map) {
				Map<String, Object> opt = (Map) obj;
				
				if(opt.containsKey("$knnVector")) {
					obj = opt.get("$knnVector");
					useAnn = false;
				}
				else if(opt.containsKey("$annVector")) {
					obj = opt.get("$annVector");
					useAnn = true;
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
				
				if(opt.containsKey("$max")) {
					scoreMax = Float.parseFloat(opt.get("$max").toString());
				}
				
				if(opt.containsKey("$indexType")) {					
					String indexType = opt.get("$indexType").toString().toUpperCase();
					if(!indexType.equals(this.indexType)) {
						this.close();
						this.indexType = indexType;
					}
				}
			}

			Vector vec = this.computeValueEmbedding(obj);
			
			int maxResults = limit;
			if(limit==0) {
				maxResults = K;
			}
			List<LabeledVector<Object>> docs = this.findKClosest(maxResults, vec, useAnn);
			limit = (int) docs.size();
			result = new ArrayList<>(limit);
			for (int i = 0; i < limit; i++) {
				
				LabeledVector<Object> doc = docs.get(i);
				float score = doc.weight();
				if(score<=scoreMax) { // 越小越好
					Object k = doc.label();
					Vector v = doc.features();
					
					result.add(new IdWithMeta(k,true,new Document("vectorScore",score)));
				}
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}	
	
	public boolean isTextIndex() {
		return true;
	}
	
	private static boolean isInQuery(String key) {
		return key.equals(QueryOperator.IN.getValue());
	}

}
