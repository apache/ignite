package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ProjectStage implements AggregationStage {

    private final Document projection;
    private final boolean hasInclusions;

    public ProjectStage(Document projection) {
        if (projection.isEmpty()) {
            throw new MongoServerError(40177, "Invalid $project :: caused by :: specification must have at least one field");
        }
        this.projection = projection;
        this.hasInclusions = hasInclusions(projection);
    }

    private static boolean hasInclusions(Document projection) {
        for (Entry<String, Object> entry : projection.entrySet()) {
            Object projectionValue = entry.getValue();
            if (Utils.isTrue(projectionValue)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::projectDocument);
    }

    Document projectDocument(Document document) {
        Document result = new Document();

        if (!projection.containsKey(ID_FIELD)) {
            putIfContainsField(document, result, ID_FIELD);
        }

        Map<String, Object> effectiveProjection = calculateEffectiveProjection(document);

        for (Entry<String, Object> entry : effectiveProjection.entrySet()) {
            String field = entry.getKey();
            Object projectionValue = entry.getValue();
            if (projectionValue instanceof Number || projectionValue instanceof Boolean) {
                if (Utils.isTrue(projectionValue)) {
                    putIfContainsField(document, result, field);
                }
            } else if (projectionValue == null) {
                result.put(field, null);
            } else {
                Object value = Expression.evaluateDocument(projectionValue, document);
                if (!(value instanceof Missing)) {
                    result.put(field, value);
                }
            }
        }
        return result;
    }

    private Map<String, Object> calculateEffectiveProjection(Document document) {
        if (hasInclusions) {
            return projection;
        }

        Map<String, Object> effectiveProjection = new LinkedHashMap<>(projection);

        for (String documentField : document.keySet()) {
            if (!effectiveProjection.containsKey(documentField)) {
                effectiveProjection.put(documentField, Boolean.TRUE);
            }
        }
        return effectiveProjection;
    }

    private static void putIfContainsField(Document input, Document result, String field) {
        if (input.containsKey(field)) {
            result.put(field, input.get(field));
        }
    }
}
