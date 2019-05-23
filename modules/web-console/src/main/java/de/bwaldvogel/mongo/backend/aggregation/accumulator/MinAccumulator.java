package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import de.bwaldvogel.mongo.backend.ValueComparator;

public class MinAccumulator extends ComparingAccumulator {

    public MinAccumulator(String field, Object expression) {
        super(field, expression, ValueComparator.asc());
    }

}
