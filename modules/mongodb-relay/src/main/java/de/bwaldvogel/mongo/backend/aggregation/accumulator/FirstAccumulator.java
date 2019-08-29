package de.bwaldvogel.mongo.backend.aggregation.accumulator;

public class FirstAccumulator extends Accumulator {

    private Object firstValue;
    private boolean first = true;

    public FirstAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (first) {
            firstValue = value;
            first = false;
        }
    }

    @Override
    public Object getResult() {
        return firstValue;
    }
}
