package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Arrays;
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
            if (arrayFilter.size() > 1) {
                List<String> keys = new ArrayList<>(arrayFilter.keySet());
                throw new FailedToParseException("Error parsing array filter :: caused by ::" +
                    " Expected a single top-level field name, found '" + keys.get(0) + "' and '" + keys.get(1) + "'");
            }
            Entry<String, Object> entry = arrayFilter.entrySet().iterator().next();

            List<String> pathFragments = splitPath(entry.getKey());
            String identifier = pathFragments.get(0);
            if (!identifier.matches("^[a-zA-Z0-9]+$")) {
                throw new BadValueException("Error parsing array filter :: caused by :: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found '" + identifier + "'");
            }

            Object query = entry.getValue();
            Object filter = createFilter(pathFragments, query);
            if (arrayFilterMap.put(identifier, filter) != null) {
                throw new FailedToParseException("Found multiple array filters with the same top-level field name " + identifier);
            }
        }

        if (!arrayFilterMap.isEmpty()) {
            validate(updateQuery, arrayFilterMap);
        }

        return new ArrayFilters(arrayFilterMap);
    }

    private static Object createFilter(List<String> pathFragments, Object query) {
        List<String> tail = getTail(pathFragments);
        if (tail.isEmpty()) {
            return query;
        } else {
            return new Document(joinPath(tail), query);
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

    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public String toString() {
        return Json.toJsonValue(getValues());
    }

    private Object getArrayFilterQuery(String key) {
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

        List<String> pathFragments = splitPath(key);
        String path = pathFragments.get(0);
        return calculateKeys(document, pathFragments, path);
    }

    private List<String> calculateKeys(Document document, List<String> pathFragments, String path) {
        String firstFragment = pathFragments.get(0);
        Object subObject = Utils.getSubdocumentValue(document, firstFragment);
        if (subObject instanceof Missing) {
            throw new BadValueException("The path '" + path + "' must exist in the document in order to apply array updates.");
        }

        String nextFragment = pathFragments.get(1);
        if (isPositionalOperator(nextFragment)) {
            if (!(subObject instanceof List)) {
                throw new BadValueException("Cannot apply array updates to non-array element " + firstFragment + ": " + Json.toJsonValue(subObject));
            }
            List<?> values = (List<?>) subObject;
            Object arrayFilterQuery = getArrayFilterQuery(nextFragment);
            QueryMatcher queryMatcher = new DefaultQueryMatcher();
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                if (queryMatcher.matchesValue(arrayFilterQuery, values.get(i))) {
                    List<String> remaining = pathFragments.subList(2, pathFragments.size());
                    keys.add(joinPath(path, String.valueOf(i), remaining));
                }
            }

            return keys;
        } else {
            String nextPath = joinPath(path, nextFragment);
            if (!(subObject instanceof Document)) {
                throw new BadValueException("The path '" + nextPath + "' must exist in the document in order to apply array updates.");
            }
            Document subDocument = (Document) subObject;
            List<String> tail = getTail(pathFragments);
            return calculateKeys(subDocument, tail, nextPath);
        }
    }

    Map<String, Object> getValues() {
        return values;
    }

    private static String joinPath(String first, String second, List<String> rest) {
        List<String> fragments = new ArrayList<>();
        fragments.add(first);
        fragments.add(second);
        fragments.addAll(rest);
        return joinPath(fragments);
    }

    private static String joinPath(String... fragments) {
        return joinPath(Arrays.asList(fragments));
    }

    private static String joinPath(List<String> fragments) {
        return String.join(".", fragments);
    }

    private static List<String> splitPath(String input) {
        return Arrays.asList(input.split("\\."));
    }

    private static List<String> getTail(List<String> pathFragments) {
        return pathFragments.subList(1, pathFragments.size());
    }

}
