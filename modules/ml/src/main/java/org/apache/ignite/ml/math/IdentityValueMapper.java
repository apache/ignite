/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math;

import org.apache.ignite.ml.math.distributed.ValueMapper;

/**
 * Identity value mapper.
 */
public class IdentityValueMapper implements ValueMapper<Double> {
    /** */
    private static final long serialVersionUID = -8010078306142216389L;

    /** {@inheritDoc} */
    @Override public Double fromDouble(double v) {
        return v;
    }

    /** {@inheritDoc} */
    @Override public double toDouble(Double v) {
        assert v != null;

        return v;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Long.hashCode(serialVersionUID);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return true;
    }
}
