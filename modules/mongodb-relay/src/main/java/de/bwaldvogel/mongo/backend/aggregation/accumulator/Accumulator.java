package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public abstract class Accumulator {

    private final String field;
    private final Object expression;

    Accumulator(String field, Object expression) {
        this.field = field;
        this.expression = expression;
    }

    public static Map<String, Supplier<Accumulator>> parse(Document configuration) {
        Map<String, Supplier<Accumulator>> accumulators = new LinkedHashMap<>();
        for (Map.Entry<String, ?> accumulatorEntry : configuration.entrySet()) {
            if (accumulatorEntry.getKey().equals(ID_FIELD)) {
                continue;
            }
            String field = accumulatorEntry.getKey();
            Document entryValue = (Document) accumulatorEntry.getValue();
            Map.Entry<String, Object> aggregation = CollectionUtils.getSingleElement(entryValue.entrySet(), () -> {
                throw new MongoServerError(40238, "The field '" + field + "' must specify one accumulator");
            });
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

    public String getField() {
        return field;
    }

    public Object getExpression() {
        return expression;
    }

    public abstract void aggregate(Object value);

    public abstract Object getResult();
}
