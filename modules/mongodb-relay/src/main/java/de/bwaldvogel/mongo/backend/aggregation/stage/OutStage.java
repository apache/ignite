package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Collections;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class OutStage implements AggregationStage {

    private final MongoDatabase database;
    private final String collectionName;

    public OutStage(MongoDatabase database, Object collectionName) {
        this.database = database;
        this.collectionName = (String) collectionName;
        if (this.collectionName.contains("$")) {
            throw new MongoServerError(17385, "Can't $out to special collection: " + this.collectionName);
        }
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        String tempCollectionName = "_tmp" + System.currentTimeMillis() + "_" + this.collectionName;
        MongoCollection<?> tempCollection = database.createCollectionOrThrowIfExists(tempCollectionName);
        stream.forEach(document -> tempCollection.insertDocuments(Collections.singletonList(document)));
        database.moveCollection(database, tempCollection, this.collectionName);
        return Stream.empty();
    }

}
