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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * H2 Decimal.
 */
public class StatisticsDecimalMessage implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 184;

    /** */
    @Order(0)
    private int scale;

    /** */
    @Order(value = 1, method = "bytes")
    private byte[] b;

    /**
     *
     */
    public StatisticsDecimalMessage() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public StatisticsDecimalMessage(BigDecimal val) {
        if (val == null) {
            scale = 0;
            b = null;
        }
        else {
            scale = val.scale();
            b = val.unscaledValue().toByteArray();
        }
    }

    /**
     * @return Decimal value.
     */
    public BigDecimal value() {
        if (b == null && scale == 0)
            return null;

        return new BigDecimal(new BigInteger(b), scale);
    }

    /** */
    public int scale() {
        return scale;
    }

    /** */
    public void scale(int scale) {
        this.scale = scale;
    }

    /** */
    public byte[] bytes() {
        return b;
    }

    /** */
    public void bytes(byte[] b) {
        this.b = b;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return Objects.toString(value());
    }
}
