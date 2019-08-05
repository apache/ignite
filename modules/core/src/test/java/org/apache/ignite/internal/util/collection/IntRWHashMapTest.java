package org.apache.ignite.internal.util.collection;

/**
 * Base scenarios for read-write map.
 */
public class IntRWHashMapTest extends AbstractBaseIntMapTest {
    /** {@inheritDoc} */
    @Override protected IntMap<String> instantiateMap() {
        return new IntRWHashMap<>();
    }
}