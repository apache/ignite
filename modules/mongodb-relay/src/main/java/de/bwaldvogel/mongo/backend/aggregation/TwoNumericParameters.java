package de.bwaldvogel.mongo.backend.aggregation;

class TwoNumericParameters {

    private final Number first;
    private final Number second;

    TwoNumericParameters(Number first, Number second) {
        this.first = first;
        this.second = second;
    }

    Number getFirst() {
        return first;
    }

    Number getSecond() {
        return second;
    }

    double getFirstAsDouble() {
        return first.doubleValue();
    }

    double getSecondAsDouble() {
        return second.doubleValue();
    }

}
