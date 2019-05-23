package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Document;

public interface AggregationStage {

    Stream<Document> apply(Stream<Document> stream);

}
