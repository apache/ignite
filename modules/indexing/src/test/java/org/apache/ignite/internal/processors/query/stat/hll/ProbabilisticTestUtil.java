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

package org.apache.ignite.internal.processors.query.stat.hll;

import org.apache.ignite.internal.processors.query.stat.hll.util.BitUtil;

/**
 * A collection of test utilities for constructing input values to HLLs and for
 * computing their serialized size.
 *
 * @author timon
 */
public class ProbabilisticTestUtil {
    /**
     * Constructs a value that when added raw to a HLL will set the register at
     * <code>registerIndex</code> to <code>registerValue</code>.
     *
     * @param  log2m the log-base-2 of the number of registers in the HLL
     * @param  registerIndex the index of the register to set
     * @param  registerValue the value to set the register to
     * @return the value
     */
    public static long constructHLLValue(final int log2m, final int registerIndex, final int registerValue) {
        final long partition = registerIndex;
        final long substreamValue = (1L << (registerValue - 1));
        return (substreamValue << log2m) | partition;
    }

    /**
     * Extracts the HLL register index from a raw value.
     */
    public static short getRegisterIndex(final long rawValue, final int log2m) {
        final long mBitsMask = (1 << log2m) - 1;
        final short j = (short)(rawValue & mBitsMask);
        return j;
    }

    /**
     * Extracts the HLL register value from a raw value.
     */
    public static byte getRegisterValue(final long rawValue, final int log2m) {
        final long substreamValue = (rawValue >>> log2m);
        final byte p_w;

        if (substreamValue == 0L) {
            // The paper does not cover p(0x0), so the special value 0 is used.
            // 0 is the original initialization value of the registers, so by
            // doing this the HLL simply ignores it. This is acceptable
            // because the probability is 1/(2^(2^registerSizeInBits)).
            p_w = 0;
        } else
            p_w = (byte)Math.min(1 + BitUtil.leastSignificantBit(substreamValue), 31);

        return p_w;
    }

    /**
     * @return the number of bytes required to pack <code>registerCount</code>
     *         registers of width <code>shortWordLength</code>.
     */
    public static int getRequiredBytes(final int shortWordLength, final int registerCount) {
        return (int)Math.ceil((registerCount * shortWordLength) / (float)8);
    }
}
