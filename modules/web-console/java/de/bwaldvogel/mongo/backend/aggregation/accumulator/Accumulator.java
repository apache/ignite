package de.bwaldvogel.mongo.backend.aggregation.accumulator;

public abstract class Accumulator {

    private final String field;
    private final Object expression;

    Accumulator(String field, Object expression) {
        this.field = field;
        this.expression = expression;
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
