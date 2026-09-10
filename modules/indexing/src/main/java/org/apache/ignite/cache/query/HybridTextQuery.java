package org.apache.ignite.cache.query;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.jetbrains.annotations.Nullable;


/**
 * A hybrid query that performs both text and vector search.
 * By default, it behaves as a standard TextQuery. When a vector is provided,
 * it performs a hybrid search combining Lucene's keyword and KNN queries.
 *
 * @param <K> Cache key type.
 * @param <V> Cache value type.
 */
public final class HybridTextQuery<K, V> extends TextQuery<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Vector field name. */
    private String vectorFieldName;

    /** Query vector. */
    private float[] vector;

    private byte[] bytesVector;

    /** Number of nearest neighbors. */
    private int k;

    /** Distance metric for vector search. */
    private VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;

    /** Hybrid ranking strategy. */
    private HybridStrategy hybridStrategy = HybridStrategy.RRF;

    /** Weight for vector portion in WEIGHTED_SUM strategy. */
    private float vectorWeight = 0.5f;

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param txt Search string.
     */
    public HybridTextQuery(String type, String txt) {
        super(type,txt);
        setText(txt);
    }


    /**
     * Create scan query with filter.
     *
     * @param filter Filter. If {@code null} then all entries will be returned.
     */
    public HybridTextQuery(String type, String txt,@Nullable IgniteBiPredicate<K, V> filter) {
        super(type, txt);
        setFitler(filter);
    }

    /**
     * Constructs a pure text search query.
     *
     * @param type Value type.
     * @param txt Text query string.
     */
    public HybridTextQuery(Class<V> type, String txt) {
        super(type,txt);
    }

    /**
     * Constructs a hybrid text + vector search query.
     *
     * @param type Value type.
     * @param txt Text query string.
     * @param vectorFieldName Name of the vector field.
     * @param vector Query vector.
     * @param k Number of nearest neighbors.
     */
    public HybridTextQuery(Class<V> type, String txt, String vectorFieldName, float[] vector, int k) {
        super(type,txt);
        this.vectorFieldName = vectorFieldName;
        this.vector = vector;
        this.k = k;
    }

    public HybridTextQuery(String type, String txt, String vectorFieldName, float[] vector, int k) {
        super(type,txt);
        this.vectorFieldName = vectorFieldName;
        this.vector = vector;
        this.k = k;
    }

    /**
     * Sets the vector field and query vector for hybrid search.
     *
     * @param vectorFieldName Name of the vector field (annotated with @QueryVectorField).
     * @param vector The query vector (float array).
     * @param k Number of nearest neighbors to return from the vector portion.
     * @return this TextQuery instance for chaining.
     */
    public HybridTextQuery<K, V> setVectorQuery(String vectorFieldName, float[] vector, int k) {
        this.vectorFieldName = vectorFieldName;
        this.vector = vector;
        this.k = k;
        return this;
    }

    public HybridTextQuery<K, V> setVectorQuery(String vectorFieldName, byte[] vector, int k) {
        this.vectorFieldName = vectorFieldName;
        this.bytesVector = vector;
        this.k = k;
        return this;
    }

    /**
     * Sets the distance metric for the vector part.
     *
     * @param metric Distance metric.
     * @return this TextQuery instance for chaining.
     */
    public HybridTextQuery<K, V> setSimilarityFunction(VectorSimilarityFunction metric) {
        this.similarityFunction = metric;
        return this;
    }

    /**
     * Sets the hybrid ranking strategy.
     *
     * @param strategy Hybrid ranking strategy.
     * @return this TextQuery instance for chaining.
     */
    public HybridTextQuery<K, V> setHybridStrategy(HybridStrategy strategy) {
        this.hybridStrategy = strategy;
        return this;
    }

    /**
     * Sets the weight for the vector portion in WEIGHTED_SUM strategy.
     *
     * @param weight Vector weight (0-1).
     * @return this TextQuery instance for chaining.
     */
    public HybridTextQuery<K, V> setVectorWeight(float weight) {
        this.vectorWeight = Math.max(0, Math.min(1, weight));
        return this;
    }

    /**
     * @return Vector field name.
     */
    public String getVectorFieldName() {
        return vectorFieldName;
    }

    /**
     * @return Query vector.
     */
    public float[] getVector() {
        return vector;
    }

    /**
     * @return Query vector.
     */
    public byte[] getBytesVector() {
        return bytesVector;
    }

    /**
     * @return Number of nearest neighbors.
     */
    public int getK() {
        return k;
    }

    /**
     * @return Distance metric.
     */
    public VectorSimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    /**
     * @return Hybrid strategy.
     */
    public HybridStrategy getHybridStrategy() {
        return hybridStrategy;
    }

    /**
     * @return Vector weight.
     */
    public float getVectorWeight() {
        return vectorWeight;
    }

    /**
     * @return {@code true} if this is a hybrid query with vector component.
     */
    public boolean isHybridQuery() {
        return vectorFieldName != null && vector != null && vector.length > 0;
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HybridTextQuery.class, this);
    }
}