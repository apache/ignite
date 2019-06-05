package de.bwaldvogel.mongo.backend;

import java.util.HashMap;
import java.util.Map;

enum QueryFilter {

    AND("$and"),
    OR("$or"),
    NOR("$nor"),
    EXPR("$expr"),
    ;

    private final String value;

    QueryFilter(String value) {
        this.value = value;
    }

    private static final Map<String, QueryFilter> MAP = new HashMap<>();

    static {
        for (QueryFilter filter : QueryFilter.values()) {
            QueryFilter old = MAP.put(filter.getValue(), filter);
            Assert.isNull(old, () -> "Duplicate value: " + filter.getValue());
        }
    }

    public String getValue() {
        return value;
    }

    static boolean isQueryFilter(String value) {
        return MAP.containsKey(value);
    }

    static QueryFilter fromValue(String value) throws IllegalArgumentException {
        QueryFilter filter = MAP.get(value);
        Assert.notNull(filter, () -> "Illegal filter: " + value);
        return filter;
    }

    @Override
    public String toString() {
        return value;
    }
}
