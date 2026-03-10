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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.Order;
import org.h2.value.Value;
import org.h2.value.ValueDecimal;

import static org.h2.util.StringUtils.convertBytesToHex;

/**
 * H2 Decimal.
 */
public class GridH2Decimal extends GridH2ValueMessage {
    /** */
    @Order(0)
    int scale;

    /** */
    @Order(1)
    byte[] b;

    /**
     *
     */
    public GridH2Decimal() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Decimal(Value val) {
        assert val.getType() == Value.DECIMAL : val.getType();

        BigDecimal x = val.getBigDecimal();

        scale = x.scale();
        b = x.unscaledValue().toByteArray();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return ValueDecimal.get(new BigDecimal(new BigInteger(b), scale));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return scale + "_" + convertBytesToHex(b);
    }
}
