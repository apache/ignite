package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.FailedToParseException;

public class ArrayFilters {

    private final Map<String, Object> values;

    private ArrayFilters(Map<String, Object> values) {
        this.values = values;
    }

    static ArrayFilters parse(Document query, Document updateQuery) {
        @SuppressWarnings("unchecked")
        List<Document> arrayFilters = (List<Document>) query.getOrDefault("arrayFilters", Collections.emptyList());
        return parse(arrayFilters, updateQuery);
    }

    private static ArrayFilters parse(List<Document> arrayFilters, Document updateQuery) {
        Map<String, Object> arrayFilterMap = new LinkedHashMap<>();
        for (Document arrayFilter : arrayFilters) {
            if (arrayFilter.isEmpty()) {
                throw new FailedToParseException("Cannot use an expression without a top-level field name in arrayFilters");
            }

            List<String> topLevelFieldNames = arrayFilter.keySet().stream()
                .map(Utils::firstFragment)
                .distinct()
                .collect(Collectors.toList());

            if (topLevelFieldNames.size() > 1) {
                throw new FailedToParseException("Error parsing array filter :: caused by ::" +
                    " Expected a single top-level field name, found '" + topLevelFieldNames.get(0) + "' and '" + topLevelFieldNames.get(1) + "'");
            }

            String topLevelFieldName = CollectionUtils.getSingleElement(topLevelFieldNames);
            if (!topLevelFieldName.matches("^[a-zA-Z0-9]+$")) {
                throw new BadValueException("Error parsing array filter :: caused by :: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found '" + topLevelFieldName + "'");
            }

            Object filter = createFilter(arrayFilter);
            if (arrayFilterMap.put(topLevelFieldName, filter) != null) {
                throw new FailedToParseException("Found multiple array filters with the same top-level field name " + topLevelFieldName);
            }
        }

        if (!arrayFilterMap.isEmpty()) {
            validate(updateQuery, arrayFilterMap);
        }

        return new ArrayFilters(arrayFilterMap);
    }

    private static Object createFilter(Document arrayFilter) {
        Document filter = new Document();

        for (Entry<String, Object> entry : arrayFilter.entrySet()) {
            List<String> pathFragments = Utils.splitPath(entry.getKey());
            String tailPath = Utils.joinTail(pathFragments);
            Object query = entry.getValue();
            if (tailPath.isEmpty()) {
                Assert.hasSize(arrayFilter.keySet(), 1);
                return query;
            } else {
                filter.put(tailPath, query);
            }
        }
        return filter;
    }

    private static Object createFilter(List<String> pathFragments, Object query) {
        List<String> tail = Utils.getTail(pathFragments);
        if (tail.isEmpty()) {
            return query;
        } else {
            return new Document(Utils.joinPath(tail), query);
        }
    }

    private static void validate(Document updateQuery, Map<String, ?> arrayFilterMap) {
        Set<String> allKeys = updateQuery.values().stream()
            .filter(Document.class::isInstance)
            .map(Document.class::cast)
            .flatMap(d -> d.keySet().stream())
            .collect(Collectors.toSet());

        for (String identifier : arrayFilterMap.keySet()) {
            if (allKeys.stream().noneMatch(key -> key.contains(toPositionalOperator(identifier)))) {
                throw new FailedToParseException("The array filter for identifier '" + identifier
                    + "' was not used in the update " + updateQuery.toString(true, "{ ", " }"));
            }
        }
    }

    public static ArrayFilters empty() {
        return new ArrayFilters(Collections.emptyMap());
    }

    private boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public String toString() {
        return Json.toJsonValue(getValues());
    }

    private Object getArrayFilterQuery(String key) {
        if (isPositionalAll(key)) {
            return new Document(QueryOperator.EXISTS.getValue(), true);
        }
        return values.get(extractKeyFromPositionalOperator(key));
    }

    private static String toPositionalOperator(String key) {
        return "$[" + key + "]";
    }

    private static String extractKeyFromPositionalOperator(String operator) {
        if (!isPositionalOperator(operator)) {
            throw new IllegalArgumentException("Illegal key: " + operator);
        }
        return operator.substring("$[".length(), operator.length() - "]".length());
    }

    private static boolean isPositionalOperator(String key) {
        return key.startsWith("$[") && key.endsWith("]");
    }

    List<String> calculateKeys(Document document, String key) {
        if (isPositionalOperator(key)) {
            throw new BadValueException("Cannot have array filter identifier (i.e. '$[<id>]') element in the first position in path '" + key + "'");
        }

        List<String> pathFragments = Utils.splitPath(key);
        return calculateKeys(document, pathFragments, "");
    }

    private List<String> calculateKeys(Object object, List<String> pathFragments, String path) {
        if (pathFragments.isEmpty()) {
            return Collections.singletonList(path);
        }
        String fragment = pathFragments.get(0);

        if (!isPositionalOperator(fragment)) {
            String nextPath = Utils.joinPath(path, fragment);
            List<String> tail = Utils.getTail(pathFragments);
            Object subObject = Utils.getFieldValueListSafe(object, fragment);
            return calculateKeys(subObject, tail, nextPath);
        }

        if (object instanceof Missing) {
            throw new BadValueException("The path '" + path + "' must exist in the document in order to apply array updates.");
        } else if (!(object instanceof List)) {
            String previousKey = Utils.getLastFragment(path);
            String element = Json.toJsonValue(object, true, "{ ", " }");
            throw new BadValueException("Cannot apply array updates to non-array element " + previousKey + ": " + element);
        }

        List<?> values = (List<?>) object;
        Object arrayFilterQuery = getArrayFilterQuery(fragment);
        QueryMatcher queryMatcher = new DefaultQueryMatcher();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (queryMatcher.matchesValue(arrayFilterQuery, value)) {
                List<String> remaining = Utils.getTail(pathFragments);
                String nextPath = Utils.joinPath(path, String.valueOf(i));
                List<String> subKeys = calculateKeys(value, remaining, nextPath);
                keys.addAll(subKeys);
            }
        }

        return keys;
    }

    private static boolean isPositionalAll(String key) {
        return key.equals("$[]");
    }

    Map<String, Object> getValues() {
        return values;
    }

    boolean canHandle(String key) {
        if (!isEmpty()) {
            return true;
        } else {
            return Utils.splitPath(key).stream().anyMatch(ArrayFilters::isPositionalAll);
        }
    }

}
