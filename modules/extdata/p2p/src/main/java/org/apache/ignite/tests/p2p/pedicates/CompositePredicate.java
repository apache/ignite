package org.apache.ignite.tests.p2p.pedicates;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.binary.BinaryObject;

public class CompositePredicate<K> extends BinaryPredicate<K> implements Serializable {
    private static final long serialVersionUID = 238742479L;

    private final Collection<BinaryPredicate> predicates = new ArrayList<>();

    @Override public boolean apply(BinaryObject bo) {
        System.out.println("CompositePredicate on " + ignite.configuration().getIgniteInstanceName());

        for (BinaryPredicate predicate : predicates) {
            predicate.ignite = ignite;

            if (!predicate.apply(bo))
                return false;
        }

        return true;
    }

    public void addPredicate(BinaryPredicate predicate) {
        predicates.add(predicate);
    }
}
