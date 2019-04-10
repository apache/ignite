package org.apache.ignite.tests.p2p.pedicates;

import org.apache.ignite.binary.BinaryObject;

public class SecondConsideredPredicate extends BinaryPredicate {
    private static final long serialVersionUID = 238742456L;

    @Override public boolean apply(BinaryObject bo) {
        System.out.println("SecondConsideredPredicate on " + ignite.configuration().getIgniteInstanceName());

        return !bo.hasField("isDeleted");
    }
}
