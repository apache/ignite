package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.Constants;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.Accumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.SumAccumulator;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class BucketStage implements AggregationStage {

    private final Object groupByExpression;
    private final List<?> boundaries;
    private final Object defaultValue;
    private final Document output;

    public BucketStage(Document document) {
        groupByExpression = validateGroupBy(document.get("groupBy"));
        boundaries = getAndValidateBoundaries(document.get("boundaries"));
        defaultValue = getAndValidateDefault(document.getOrMissing("default"));
        output = getAndValidateOutput(document.get("output"));
    }

    private void validateValuePresent(Object value) {
        if (value == null) {
            throw new MongoServerError(40198, "$bucket requires 'groupBy' and 'boundaries' to be specified.");
        }
    }

    private Object validateGroupBy(Object groupBy) {
        validateValuePresent(groupBy);
        if (!(groupBy instanceof String || groupBy instanceof Document)) {
            String value = Json.toJsonValue(groupBy);
            throw new MongoServerError(40202, "The $bucket 'groupBy' field must be defined as a $-prefixed path or an expression, but found: " + value + ".");
        }
        return groupBy;
    }

    private List<?> getAndValidateBoundaries(Object boundaries) {
        validateValuePresent(boundaries);
        if (!(boundaries instanceof List)) {
            String type = Utils.describeType(boundaries);
            throw new MongoServerError(40200, "The $bucket 'boundaries' field must be an array, but found type: " + type + ".");
        }
        List<?> boundaryValues = (List<?>) boundaries;
        if (boundaryValues.size() < 2) {
            throw new MongoServerError(40192, "The $bucket 'boundaries' field must have at least 2 values, but found " + boundaryValues.size() + " value(s).");
        }

        for (int i = 1; i < boundaryValues.size(); i++) {
            Object value1 = boundaryValues.get(i - 1);
            Object value2 = boundaryValues.get(i);

            validateTypesAreCompatible(value1, value2);

            if (compare(value1, value2) >= 0) {
                int index1 = i - 1;
                int index2 = i;
                throw new MongoServerError(40194, "The 'boundaries' option to $bucket must be sorted, but elements " + index1 + " and " + index2 + " are not in ascending order (" + value1 + " is not less than " + value2 + ").");
            }
        }

        return boundaryValues;
    }

    private Object getAndValidateDefault(Object defaultValue) {
        Assert.notEmpty(boundaries);
        if (!(compare(defaultValue, boundaries.get(0)) < 0
            || compare(defaultValue, CollectionUtils.getLastElement(boundaries)) >= 0)) {
            throw new MongoServerError(40199, "The $bucket 'default' field must be less than the lowest boundary or greater than or equal to the highest boundary.");
        }
        return defaultValue;
    }

    private void validateTypesAreCompatible(Object value1, Object value2) {
        if (value1 instanceof Number && value2 instanceof Number) {
            return;
        }
        String type1 = Utils.describeType(value1);
        String type2 = Utils.describeType(value2);
        if (!type1.equals(type2)) {
            throw new MongoServerError(40193, "All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types " + type1 + " and " + type2 + ".");
        }
    }

    private Document getAndValidateOutput(Object output) {
        if (output == null) {
            return null;
        }
        if (!(output instanceof Document)) {
            throw new MongoServerError(40196, "The $bucket 'output' field must be an object, but found type: " + Utils.describeType(output) + ".");
        }
        return (Document) output;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        Map<Object, List<Accumulator>> accumulatorsPerBucket = new TreeMap<>(ValueComparator.asc());
        stream.forEach(document -> {
            Object key = Expression.evaluateDocument(groupByExpression, document);
            Object bucket = findBucket(key);
            List<Accumulator> accumulators = accumulatorsPerBucket.computeIfAbsent(bucket, k -> getAccumulators());

            for (Accumulator accumulator : accumulators) {
                Object expression = accumulator.getExpression();
                accumulator.aggregate(Expression.evaluateDocument(expression, document));
            }
        });
        List<Document> result = new ArrayList<>();
        for (Entry<Object, List<Accumulator>> entry : accumulatorsPerBucket.entrySet()) {
            Document groupResult = new Document();
            groupResult.put(Constants.ID_FIELD, entry.getKey());

            for (Accumulator accumulator : entry.getValue()) {
                groupResult.put(accumulator.getField(), accumulator.getResult());
            }

            result.add(groupResult);
        }

        return result.stream();
    }

    private List<Accumulator> getAccumulators() {
        if (output == null) {
            return Collections.singletonList(new SumAccumulator("count", new Document("$sum", 1)));
        }
        return Accumulator.parse(output).values().stream()
            .map(Supplier::get)
            .collect(Collectors.toList());
    }

    private Object findBucket(Object key) {
        if (compare(key, boundaries.get(0)) < 0) {
            return getDefaultValue();
        }
        for (int i = 1; i < boundaries.size(); i++) {
            Object boundary = boundaries.get(i);
            if (compare(key, boundary) < 0) {
                return boundaries.get(i - 1);
            }
        }
        return getDefaultValue();
    }

    private static int compare(Object a, Object b) {
        return ValueComparator.asc().compare(a, b);
    }

    private Object getDefaultValue() {
        if (defaultValue instanceof Missing) {
            throw new MongoServerError(40066, "$switch could not find a matching branch for an input, and no default was specified.");
        }
        return defaultValue;
    }

}
