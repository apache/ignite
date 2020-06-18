package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.time.Instant;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

public class IndexStatsStage implements AggregationStage {

    private final MongoCollection<?> collection;

    public IndexStatsStage(MongoCollection<?> collection) {
        this.collection = collection;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return collection.getIndexes().stream()
            .map(index -> {
                Document indexStats = new Document();
                indexStats.append("name", index.getName());
                Document key = new Document();
                for (IndexKey indexKey : index.getKeys()) {
                    key.append(indexKey.getKey(), indexKey.isAscending() ? 1 : -1);
                }
                indexStats.append("key", key);
                indexStats.append("host", Utils.getHostName());
                indexStats.append("spec",
                    new Document("key", key)
                        .append("name", index.getName())
                        .append("ns", collection.getFullName())
                        .append("v", 2)
                );
                indexStats.append("accesses", new Document()
                    .append("ops", 0L)
                    .append("since", Instant.now()));
                return indexStats;
            });
    }

}
