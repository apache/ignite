package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Missing;

class TwoParameters {
    private final Object first;
    private final Object second;

    TwoParameters(Object first, Object second) {
        this.first = first;
        this.second = second;
    }

    boolean isAnyNull() {
        return Missing.isNullOrMissing(first) || Missing.isNullOrMissing(second);
    }

    Object getFirst() {
        return first;
    }

    Object getSecond() {
        return second;
    }
}
