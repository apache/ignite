package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.Accumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.AddToSetAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.AvgAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.FirstAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.LastAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.MaxAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.MinAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.PushAccumulator;
import de.bwaldvogel.mongo.backend.aggregation.accumulator.SumAccumulator;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class GroupStage implements AggregationStage {

    private final Map<String, Supplier<Accumulator>> accumulatorSuppliers;
    private final Object idExpression;

    public GroupStage(Document groupQuery) {
        if (!groupQuery.containsKey(ID_FIELD)) {
            throw new MongoServerError(15955, "a group specification must include an _id");
        }
        idExpression = groupQuery.get(ID_FIELD);
        accumulatorSuppliers = parseAccumulators(groupQuery);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        Map<Object, Collection<Accumulator>> accumulatorsPerKey = new TreeMap<>(ValueComparator.asc());
        stream.forEach(document -> {
            Object key = Expression.evaluateDocument(idExpression, document);
            if (key instanceof Missing) {
                key = null;
            }

            Collection<Accumulator> accumulators = accumulatorsPerKey.computeIfAbsent(key, k -> accumulatorSuppliers.values()
                .stream()
                .map(Supplier::get)
                .collect(Collectors.toList()));

            for (Accumulator accumulator : accumulators) {
                Object expression = accumulator.getExpression();
                accumulator.aggregate(Expression.evaluateDocument(expression, document));
            }
        });

        List<Document> result = new ArrayList<>();

        for (Entry<Object, Collection<Accumulator>> entry : accumulatorsPerKey.entrySet()) {
            Document groupResult = new Document();
            groupResult.put(ID_FIELD, entry.getKey());

            for (Accumulator accumulator : entry.getValue()) {
                groupResult.put(accumulator.getField(), accumulator.getResult());
            }

            result.add(groupResult);
        }

        return result.stream();
    }

    private static Map<String, Supplier<Accumulator>> parseAccumulators(Document groupStage) {
        Map<String, Supplier<Accumulator>> accumulators = new LinkedHashMap<>();
        for (Entry<String, ?> accumulatorEntry : groupStage.entrySet()) {
            if (accumulatorEntry.getKey().equals(ID_FIELD)) {
                continue;
            }
            String field = accumulatorEntry.getKey();
            Document entryValue = (Document) accumulatorEntry.getValue();
            if (entryValue.size() != 1) {
                throw new MongoServerError(40238, "The field '" + field + "' must specify one accumulator");
            }
            Entry<String, Object> aggregation = entryValue.entrySet().iterator().next();
            String groupOperator = aggregation.getKey();
            Object expression = aggregation.getValue();
            if (groupOperator.equals("$sum")) {
                accumulators.put(field, () -> new SumAccumulator(field, expression));
            } else if (groupOperator.equals("$min")) {
                accumulators.put(field, () -> new MinAccumulator(field, expression));
            } else if (groupOperator.equals("$max")) {
                accumulators.put(field, () -> new MaxAccumulator(field, expression));
            } else if (groupOperator.equals("$avg")) {
                accumulators.put(field, () -> new AvgAccumulator(field, expression));
            } else if (groupOperator.equals("$addToSet")) {
                accumulators.put(field, () -> new AddToSetAccumulator(field, expression));
            } else if (groupOperator.equals("$push")) {
                accumulators.put(field, () -> new PushAccumulator(field, expression));
            } else if (groupOperator.equals("$first")) {
                accumulators.put(field, () -> new FirstAccumulator(field, expression));
            } else if (groupOperator.equals("$last")) {
                accumulators.put(field, () -> new LastAccumulator(field, expression));
            } else {
                throw new MongoServerError(15952, "unknown group operator '" + groupOperator + "'");
            }
        }
        return accumulators;
    }
}
