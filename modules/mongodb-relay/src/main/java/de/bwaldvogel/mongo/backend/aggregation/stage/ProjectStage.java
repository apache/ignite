package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

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
        validateProjection();
    }

    private static boolean hasInclusions(Document projection) {
        return projection.values().stream().anyMatch(Utils::isTrue);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::projectDocument);
    }

    Document projectDocument(Document document) {
        final Document result;
        if (hasInclusions) {
            result = new Document();
            if (!projection.containsKey(ID_FIELD)) {
                putIfContainsField(document, result, ID_FIELD);
            }
        } else {
            result = document.cloneDeeply();
        }

        for (Entry<String, Object> entry : projection.entrySet()) {
            String field = entry.getKey();
            Object projectionValue = entry.getValue();
            if (isNumberOrBoolean(projectionValue)) {
                if (Utils.isTrue(projectionValue)) {
                    putIfContainsField(document, result, field);
                } else {
                    Utils.removeSubdocumentValue(result, field);
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

    private void validateProjection() {
        if (hasInclusions) {
            boolean nonIdExclusion = projection.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(ID_FIELD))
                .map(Entry::getValue)
                .filter(ProjectStage::isNumberOrBoolean)
                .anyMatch(entry -> !Utils.isTrue(entry));
            if (nonIdExclusion) {
                throw new MongoServerError(40178,
                    "Bad projection specification, cannot exclude fields other than '_id' in an inclusion projection: "
                        + projection.toString(true, "{ ", " }"));
            }
        }
    }

    private static boolean isNumberOrBoolean(Object projectionValue) {
        return projectionValue instanceof Number || projectionValue instanceof Boolean;
    }

    private static void putIfContainsField(Document input, Document result, String key) {
        Object value = Utils.getSubdocumentValue(input, key);
        if (!(value instanceof Missing)) {
            Utils.changeSubdocumentValue(result, key, value);
        }
    }
}
