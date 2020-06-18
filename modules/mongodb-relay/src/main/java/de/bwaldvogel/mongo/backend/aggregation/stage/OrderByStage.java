package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.bson.Document;

public class OrderByStage implements AggregationStage {

    private final DocumentComparator documentComparator;

    public OrderByStage(Document orderBy) {
        this.documentComparator = new DocumentComparator(orderBy);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.sorted(documentComparator);
    }

}
