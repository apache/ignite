package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Map.Entry;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class AddFieldsStage implements AggregationStage {

    private final Document addFields;

    public AddFieldsStage(Document addFields) {
        if (addFields.isEmpty()) {
            throw new MongoServerError(40177, "Invalid $addFields :: caused by :: specification must have at least one field");
        }
        this.addFields = addFields;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::projectDocument);
    }

    Document projectDocument(Document document) {
        Document clone = document.clone();
        putRecursively(clone, addFields);
        return clone;
    }

    private void putRecursively(Document target, Document source) {
        for (Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            Object value = Expression.evaluateDocument(entry.getValue(), target);
            if (value instanceof Document) {
                Document subDocument = (Document) target.compute(key, (k, oldValue) -> {
                    if (!(oldValue instanceof Document)) {
                        return new Document();
                    } else {
                        return oldValue;
                    }
                });
                putRecursively(subDocument, (Document) value);
            } else {
                if (value instanceof Missing) {
                    target.remove(key);
                } else {
                    target.put(key, value);
                }
            }
        }
    }

}
