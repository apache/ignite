package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


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
    int dimension() default 1024;


    /**
     * @return Distance metric for similarity search. {@link org.apache.lucene.index.VectorSimilarityFunction}
     * {COSINE,EUCLIDEAN,DOT_PRODUCT,MAXIMUM_INNER_PRODUCT}
     */
    String similarity() default "COSINE";

    /**
     * @return Whether this field should be indexed for vector search.
     */
    boolean indexed() default true;

    /**
     * @return Optional field name (defaults to Java field name).
     */
    String name() default "";
}
