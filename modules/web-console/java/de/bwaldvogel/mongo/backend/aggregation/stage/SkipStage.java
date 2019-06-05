package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Document;

public class SkipStage implements AggregationStage {

    private final long numSkip;

    public SkipStage(long numSkip) {
        this.numSkip = numSkip;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.skip(numSkip);
    }
}
