/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.h2.mvstore.MVMap;

/**
 * A custom map returning the keys and values 1 .. 10.
 */
public class SequenceMap extends MVMap<Long, Long> {

    /**
     * The minimum value.
     */
    int min = 1;

    /**
     * The maximum value.
     */
    int max = 10;

    public SequenceMap(Map<String, Object> config) {
        super(config);
    }

    @Override
    public Set<Long> keySet() {
        return new AbstractSet<Long>() {

            @Override
            public Iterator<Long> iterator() {
                return new Iterator<Long>() {

                    long x = min;

                    @Override
                    public boolean hasNext() {
                        return x <= max;
                    }

                    @Override
                    public Long next() {
                        return Long.valueOf(x++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }

            @Override
            public int size() {
                return max - min + 1;
            }
        };
    }

    /**
     * A builder for this class.
     */
    public static class Builder extends MVMap.Builder<Long, Long> {
        @Override
        public SequenceMap create(Map<String, Object> config) {
            return new SequenceMap(config);
        }

    }
}
