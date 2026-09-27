package org.apache.ignite.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.lucene.index.VectorSimilarityFunction;

import javax.cache.Cache;

/**
 * Example demonstrating hybrid text + vector search.
 */
public class HybridSearchExample {
    /**
     * Example cache value class with vector field.
     */
    public static class QDocument {
        @QueryTextField
        private String title;

        @QueryTextField
        private String content;

        @QueryVectorField(dimension = 384, similarity = "COSINE")
        private float[] embedding;

        // Getters and setters...
        public String getTitle() { return title; }
        public void setTitle(String title) { this.title = title; }
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
        public float[] getEmbedding() { return embedding; }
        public void setEmbedding(float[] embedding) { this.embedding = embedding; }
    }

    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            // Create cache with indexing
            CacheConfiguration<Long, QDocument> cfg = new CacheConfiguration<>("documents");
            cfg.setIndexedTypes(Long.class, QDocument.class);

            IgniteCache<Long, QDocument> cache = ignite.getOrCreateCache(cfg);

            // Insert documents with embeddings
            QDocument doc1 = new QDocument();
            doc1.setTitle("Apache Ignite Introduction");
            doc1.setContent("Apache Ignite is an in-memory computing platform...");
            doc1.setEmbedding(new float[384]); // Fill with actual embedding
            doc1.getEmbedding()[0]=1;
            cache.put(1L, doc1);

            QDocument doc2 = new QDocument();
            doc2.setTitle("Vector Search in Lucene");
            doc2.setContent("Lucene supports vector search using KNN...");
            doc2.setEmbedding(new float[384]); // Fill with actual embedding
            doc2.getEmbedding()[1]=1;
            cache.put(2L, doc2);

            // Perform hybrid search
            float[] queryVector = new float[384]; // Fill with query embedding
            queryVector[0]=1;
            HybridTextQuery<Long, QDocument> hybridQuery = new HybridTextQuery<>(QDocument.class, "CONTENT:\"in-memory computing\"");

            hybridQuery.setVectorQuery("embedding".toUpperCase(), queryVector, 10)
                    .setHybridStrategy(HybridStrategy.RRF)
                    .setVectorWeight(0.7f);


            // Execute hybrid search
            try (QueryCursor<Cache.Entry<Long, QDocument>> cursor = cache.query(hybridQuery)) {
                for (Cache.Entry<Long, QDocument> entry : cursor) {
                    System.out.println("Result: " + entry.getValue().getTitle());
                }
            }
        }
    }
}
