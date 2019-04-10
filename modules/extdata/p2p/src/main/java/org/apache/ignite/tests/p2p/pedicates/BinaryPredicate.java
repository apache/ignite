package org.apache.ignite.tests.p2p.pedicates;

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

public abstract class BinaryPredicate<K> implements IgniteBiPredicate<K, BinaryObject> {
    @IgniteInstanceResource Ignite ignite;

    @Override public boolean apply(K key, BinaryObject bo) {
        return apply(bo);
    }

    public abstract boolean apply(BinaryObject bo);
}
