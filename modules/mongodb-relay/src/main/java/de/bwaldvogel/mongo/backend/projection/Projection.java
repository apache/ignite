package de.bwaldvogel.mongo.backend.projection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.backend.QueryOperator;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;

public class Projection {

    public static Document projectDocument(Document document, Document fields, String idField) {
        validateFields(fields);

        if (document == null) {
            return null;
        }

        Document newDocument = new Document();

        // implicitly add _id if not mentioned
        // http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#return-the-specified-fields-and-the-id-field-only
        if (!fields.containsKey(idField)) {
            newDocument.put(idField, document.get(idField));
        }

        if (onlyExclusions(fields)) {
            newDocument.putAll(document);
            for (String excludedField : fields.keySet()) {
                newDocument.remove(excludedField);
            }
        } else {
            for (Entry<String, Object> entry : fields.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (Utils.isTrue(value)) {
                    projectField(document, newDocument, key, value);
                }
            }
        }

        return newDocument;
    }

    private static void validateFields(Document fields) {
        for (Entry<String, Object> entry : fields.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Document) {
                Document document = (Document) value;
                if (document.size() > 1) {
                    throw new BadValueException(">1 field in obj: " + document.toString(true, "{ ", " }"));
                }
            }
        }
    }

    private static boolean onlyExclusions(Document fields) {
        for (String key : fields.keySet()) {
            if (Utils.isTrue(fields.get(key))) {
                return false;
            }
        }
        return true;
    }

    private static void projectField(Document document, Document newDocument, String key, Object projectionValue) {
        if (key.contains(Utils.PATH_DELIMITER)) {
            List<String> pathFragments = Utils.splitPath(key);

            String mainKey = pathFragments.get(0);
            String subKey = Utils.joinTail(pathFragments);

            Object object = document.get(mainKey);
            // do not project the subdocument if it is not of type Document
            if (object instanceof Document) {
                Document subDocument = (Document) newDocument.computeIfAbsent(mainKey, k -> new Document());
                projectField((Document) object, subDocument, subKey, projectionValue);
            } else if (object instanceof List) {
                List<?> values = (List<?>) object;
                List<Document> projectedValues = (List<Document>) newDocument.computeIfAbsent(mainKey, k -> new ArrayList<>());
                boolean wasEmpty = projectedValues.isEmpty();
                int idx = 0;

                for (Object value : values) {
                    if (value instanceof Document) {
                        final Document projectedDocument;
                        if (wasEmpty) {
                            projectedDocument = new Document();
                            projectedValues.add(projectedDocument);
                        } else {
                            projectedDocument = projectedValues.get(idx);
                        }
                        projectField((Document) value, projectedDocument, subKey, projectionValue);
                        idx++;
                    }
                }
            }
        } else {
            Object value = document.getOrMissing(key);

            if (projectionValue instanceof Document) {
                Document projectionDocument = (Document) projectionValue;
                if (projectionDocument.keySet().equals(Collections.singleton(QueryOperator.ELEM_MATCH.getValue()))) {
                    Document elemMatch = (Document) projectionDocument.get(QueryOperator.ELEM_MATCH.getValue());
                    projectElemMatch(newDocument, elemMatch, key, value);
                } else if (projectionDocument.keySet().equals(Collections.singleton("$slice"))) {
                    Object slice = projectionDocument.get("$slice");
                    projectSlice(newDocument, slice, key, value);
                } else {
                    throw new IllegalArgumentException("Unsupported projection: " + projectionValue);
                }
            } else if (!(value instanceof Missing)) {
                newDocument.put(key, value);
            }
        }
    }

    private static void projectElemMatch(Document newDocument, Document elemMatch, String key, Object value) {
        QueryMatcher queryMatcher = new DefaultQueryMatcher();
        if (value instanceof List) {
            ((List<?>) value).stream()
                .filter(sourceObject -> sourceObject instanceof Document)
                .filter(sourceObject -> queryMatcher.matches((Document) sourceObject, elemMatch))
                .findFirst()
                .ifPresent(v -> newDocument.put(key, Collections.singletonList(v)));
        }
    }

    private static void projectSlice(Document newDocument, Object slice, String key, Object value) {
        if (!(value instanceof List)) {
            newDocument.put(key, value);
            return;
        }
        List<?> values = (List<?>) value;
        int fromIndex = 0;
        int toIndex = values.size();
        if (slice instanceof Integer) {
            int num = ((Integer) slice).intValue();
            if (num < 0) {
                fromIndex = values.size() + num;
            } else {
                toIndex = num;
            }
        } else if (slice instanceof List) {
            List<?> sliceParams = (List<?>) slice;
            if (sliceParams.size() != 2) {
                throw new BadValueException("$slice array wrong size");
            }
            if (sliceParams.get(0) instanceof Number) {
                fromIndex = ((Number) sliceParams.get(0)).intValue();
            }
            if (fromIndex < 0) {
                fromIndex += values.size();
            }
            int limit = 0;
            if (sliceParams.get(1) instanceof Number) {
                limit = ((Number) sliceParams.get(1)).intValue();
            }
            if (limit <= 0) {
                throw new BadValueException("$slice limit must be positive");
            }
            toIndex = fromIndex + limit;
        } else {
            throw new BadValueException("$slice only supports numbers and [skip, limit] arrays");
        }
        List<?> slicedValue = values.subList(Math.max(0, fromIndex), Math.min(values.size(), toIndex));
        newDocument.put(key, slicedValue);
    }
}
