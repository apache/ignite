/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.compute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs apply operations.
 */
public class IgniteApplyBenchmark extends IgniteAbstractBenchmark {
    /** Args for apply. */
    private List<Integer> applyArgs;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        assert args.jobs() > 0;

        applyArgs = new ArrayList<>(args.jobs());

        for (int i = 0; i < args.jobs(); ++i)
            applyArgs.add(null);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ignite().compute().apply(new NoopClosure(), applyArgs);

        return true;
    }

    /**
     *
     */
    public static class NoopClosure implements IgniteClosure<Integer, Object>, Externalizable {
        /** {@inheritDoc} */
        @Override public Object apply(Integer o) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            //No-op
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            //No-op
        }
    }
}