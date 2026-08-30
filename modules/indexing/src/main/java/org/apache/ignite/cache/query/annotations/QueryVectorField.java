package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Annotates a field in a cache value class to be indexed as a vector field
 * for approximate nearest neighbor (ANN) search.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface QueryVectorField {
    /**
     * @return Vector dimension.
     */
    int dimension();

    /**
     * @return Vector data type.
     */
    VectorEncoding dataType() default VectorEncoding.FLOAT32;

    /**
     * @return Distance metric for similarity search.
     */
    VectorSimilarityFunction similarity() default VectorSimilarityFunction.COSINE;

    /**
     * @return Whether this field should be indexed for vector search.
     */
    boolean indexed() default true;

    /**
     * @return Optional field name (defaults to Java field name).
     */
    String name() default "";
}
