package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.bson.Document;

public class MatchStage implements AggregationStage {

    private final QueryMatcher queryMatcher = new DefaultQueryMatcher();
    private final Document query;

    public MatchStage(Document query) {
        this.query = query;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.filter(document -> queryMatcher.matches(document, query));
    }
}
