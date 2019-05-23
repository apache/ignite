package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import de.bwaldvogel.mongo.backend.ValueComparator;

public class MaxAccumulator extends ComparingAccumulator {

    public MaxAccumulator(String field, Object expression) {
        super(field, expression, ValueComparator.desc());
    }

}
