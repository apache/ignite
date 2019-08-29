package de.bwaldvogel.mongo.backend.aggregation;

class TwoNumericParameters {

    private final Number first;
    private final Number second;

    TwoNumericParameters(Number first, Number second) {
        this.first = first;
        this.second = second;
    }

    double getFirst() {
        return first.doubleValue();
    }

    double getSecond() {
        return second.doubleValue();
    }
}
