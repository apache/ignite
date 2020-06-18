package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnsetStage implements AggregationStage {

    private final List<String> unsetPaths = new ArrayList<>();

    public UnsetStage(Object input) {
        if (input instanceof Collection<?>) {
            for (Object fieldPath : (Collection<?>) input) {
                addFieldPath(fieldPath);
            }
        } else {
            addFieldPath(input);
        }
    }

    private void addFieldPath(Object fieldPath) {
        if (!(fieldPath instanceof String)) {
            throw new MongoServerError(31120, "$unset specification must be a string or an array containing only string values");
        }
        String fieldPathString = (String) fieldPath;
        if (fieldPathString.isEmpty()) {
            throw new MongoServerError(40352, "Invalid $project :: caused by :: FieldPath cannot be constructed with empty string");
        }
        unsetPaths.add(fieldPathString);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::unsetDocumentFields);
    }

    private Document unsetDocumentFields(Document document) {
        Document result = document.cloneDeeply();
        for (String unsetPath : unsetPaths) {
            Utils.removeSubdocumentValue(result, unsetPath);
        }
        return result;
    }

}
