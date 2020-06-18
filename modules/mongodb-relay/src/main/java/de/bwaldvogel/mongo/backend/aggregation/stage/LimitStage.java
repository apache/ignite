package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Document;

public class LimitStage implements AggregationStage {

    private final long maxSize;

    public LimitStage(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.limit(maxSize);
    }

}
