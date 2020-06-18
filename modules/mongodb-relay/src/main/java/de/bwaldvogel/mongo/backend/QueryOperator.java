package de.bwaldvogel.mongo.backend;

import java.util.HashMap;
import java.util.Map;

import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.MongoServerError;

public enum QueryOperator {

    IN("$in"),
    NOT_IN("$nin"),
    NOT("$not"),
    NOT_EQUALS("$ne"),
    EQUAL("$eq"),
    EXISTS("$exists"),
    GREATER_THAN_OR_EQUAL("$gte"),
    GREATER_THAN("$gt"),
    LESS_THAN_OR_EQUAL("$lte"),
    LESS_THAN("$lt"),
    MOD("$mod"),
    SIZE("$size"),
    AND("$and"),
    OR("$or"),
    ALL("$all"),
    ELEM_MATCH("$elemMatch"),
    TYPE("$type"),
    ;

    private String value;

    QueryOperator(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    private static final Map<String, QueryOperator> MAP = new HashMap<>();

    static {
        for (QueryOperator operator : QueryOperator.values()) {
            QueryOperator old = MAP.put(operator.getValue(), operator);
            Assert.isNull(old, () -> "Duplicate operator value: " + operator.getValue());
        }
    }

    static QueryOperator fromValue(String value) throws MongoServerError {
        QueryOperator op = MAP.get(value);
        if (op == null) {
            throw new BadValueException("unknown operator: " + value);
        }
        return op;
    }

}
