package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class JaccardIndex implements DistanceMeasure {

    @Override
    public double compute(Vector a, Vector b) throws CardinalityException {
        Set<Double> aSet = new HashSet<>();
        double intersect = 0d;

        for (int i = 0; i < a.size(); i++)
           aSet.add(a.get(i));

        for (int i = 0; i < b.size(); i++)
            if(aSet.contains(b.get(i))) ++ intersect;

        return intersect / (a.size() + b.size() - intersect);
    }
}
