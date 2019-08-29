package de.bwaldvogel.mongo.backend.aggregation.accumulator;

public class LastAccumulator extends Accumulator {

    private Object lastValue;

    public LastAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        lastValue = value;
    }

    @Override
    public Object getResult() {
        return lastValue;
    }
}
