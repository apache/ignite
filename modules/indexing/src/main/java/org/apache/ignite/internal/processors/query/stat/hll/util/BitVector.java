/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.hll.util;

import org.apache.ignite.internal.processors.query.stat.hll.serialization.IWordSerializer;

/**
 * A vector (array) of bits that is accessed in units ("registers") of <code>width</code>
 * bits which are stored as 64bit "words" (<code>long</code>s).  In this context
 * a register is at most 64bits.
 *
 * @author rgrzywinski
 */
public class BitVector implements Cloneable {
    /** NOTE:  in this context, a word is 64bits
     * rather than doing division to determine how a bit index fits into 64bit
     * words (i.e. longs), bit shifting is used
     */
    private static final int LOG2_BITS_PER_WORD = 6/*=>64bits*/;

    /** Bits per word. */
    private static final int BITS_PER_WORD = 1 << LOG2_BITS_PER_WORD;

    /** Bits per word mask. */
    private static final int BITS_PER_WORD_MASK = BITS_PER_WORD - 1;

    /** ditto from above but for bytes (for output)*/
    private static final int LOG2_BITS_PER_BYTE = 3/*=>8bits*/;

    /** Bits per byte. */
    public static final int BITS_PER_BYTE = 1 << LOG2_BITS_PER_BYTE;

    // ========================================================================
    /** Bytes per word. */
    public static final int BYTES_PER_WORD = 8/*8 bytes in a long*/;

    // ************************************************************************
    /** 64bit words. */
    private final long[] words;

    /** */
    public final long[] words() { return words; }

    /** */
    public final int wordCount() { return words.length; }

    /** */
    public final int byteCount() { return wordCount() * BYTES_PER_WORD; }

    /** the width of a register in bits (this cannot be more than 64 (the word size)) */
    private final int registerWidth;

    /** */
    public final int registerWidth() { return registerWidth; }

    /** Count. */
    private final long count;

    // ------------------------------------------------------------------------
    /** Register mask. */
    private final long registerMask;

    // ========================================================================
    /**
     * @param  width the width of each register.  This cannot be negative or
     *         zero or greater than 63 (the signed word size).
     * @param  count the number of registers.  This cannot be negative or zero
     */
    public BitVector(final int width, final long count) {
        // ceil((width * count)/BITS_PER_WORD)
        this.words = new long[(int)(((width * count) + BITS_PER_WORD_MASK) >>> LOG2_BITS_PER_WORD)];
        this.registerWidth = width;
        this.count = count;

        this.registerMask = (1L << width) - 1;
    }

    // ========================================================================
    /**
     * @param  registerIndex the index of the register whose value is to be
     *         retrieved.  This cannot be negative.
     * @return the value at the specified register index
     * @see #setRegister(long, long)
     * @see #setMaxRegister(long, long)
     */
    // NOTE:  if this changes then setMaxRegister() must change
    public long getRegister(final long registerIndex) {
        final long bitIndex = registerIndex * registerWidth;
        final int firstWordIndex = (int)(bitIndex >>> LOG2_BITS_PER_WORD)/*aka (bitIndex / BITS_PER_WORD)*/;
        final int secondWordIndex = (int)((bitIndex + registerWidth - 1) >>> LOG2_BITS_PER_WORD)/*see above*/;
        final int bitRemainder = (int)(bitIndex & BITS_PER_WORD_MASK)/*aka (bitIndex % BITS_PER_WORD)*/;

        if (firstWordIndex == secondWordIndex)
            return ((words[firstWordIndex] >>> bitRemainder) & registerMask);
        /* else -- register spans words */

        return (words[firstWordIndex] >>> bitRemainder)/*no need to mask since at top of word*/
            | (words[secondWordIndex] << (BITS_PER_WORD - bitRemainder)) & registerMask;
    }

    /**
     * @param registerIndex the index of the register whose value is to be set.
     *        This cannot be negative
     * @param value the value to set in the register
     * @see #getRegister(long)
     * @see #setMaxRegister(long, long)
     */
    // NOTE:  if this changes then setMaxRegister() must change
    public void setRegister(final long registerIndex, final long value) {
        final long bitIndex = registerIndex * registerWidth;
        final int firstWordIndex = (int)(bitIndex >>> LOG2_BITS_PER_WORD)/*aka (bitIndex / BITS_PER_WORD)*/;
        final int secondWordIndex = (int)((bitIndex + registerWidth - 1) >>> LOG2_BITS_PER_WORD)/*see above*/;
        final int bitRemainder = (int)(bitIndex & BITS_PER_WORD_MASK)/*aka (bitIndex % BITS_PER_WORD)*/;

        final long words[] = this.words/*for convenience/performance*/;

        if (firstWordIndex == secondWordIndex) {
            // clear then set
            words[firstWordIndex] &= ~(registerMask << bitRemainder);
            words[firstWordIndex] |= (value << bitRemainder);
        }
        else {
            /*register spans words*/
            // clear then set each partial word
            words[firstWordIndex] &= (1L << bitRemainder) - 1;
            words[firstWordIndex] |= (value << bitRemainder);

            words[secondWordIndex] &= ~(registerMask >>> (BITS_PER_WORD - bitRemainder));
            words[secondWordIndex] |= (value >>> (BITS_PER_WORD - bitRemainder));
        }
    }

    // ------------------------------------------------------------------------
    /**
     * @return a <code>LongIterator</code> for iterating starting at the register
     *         with index zero. This will never be <code>null</code>.
     */
    public org.apache.ignite.internal.processors.query.stat.hll.util.LongIterator registerIterator() {
        LongIterator longIterator = new LongIterator() {
            final int registerWidth = BitVector.this.registerWidth;
            final long[] words = BitVector.this.words;
            final long registerMask = BitVector.this.registerMask;

            // register setup
            long registerIndex = 0;
            int wordIdx = 0;
            int remainingWordBits = BITS_PER_WORD;
            long word = words[wordIdx];

            @Override public long next() {
                long register;

                if (remainingWordBits >= registerWidth) {
                    register = word & registerMask;

                    // shift to the next register
                    word >>>= registerWidth;
                    remainingWordBits -= registerWidth;
                }
                else { /*insufficient bits remaining in current word*/
                    wordIdx++/*move to the next word*/;

                    register = (word | (words[wordIdx] << remainingWordBits)) & registerMask;

                    // shift to the next partial register (word)
                    word = words[wordIdx] >>> (registerWidth - remainingWordBits);
                    remainingWordBits += BITS_PER_WORD - registerWidth;
                }

                registerIndex++;
                return register;
            }

            @Override public boolean hasNext() {
                return registerIndex < count;
            }
        };

        return longIterator;
    }

    // ------------------------------------------------------------------------
    // composite accessors
    /**
     * Sets the value of the specified index register if and only if the specified
     * value is greater than the current value in the register.  This is equivalent
     * to but much more performant than:<p/>
     *
     * <pre>vector.setRegister(index, Math.max(vector.getRegister(index), value));</pre>
     *
     * @param  registerIndex the index of the register whose value is to be set.
     *         This cannot be negative
     * @param  value the value to set in the register if and only if this value
     *         is greater than the current value in the register
     * @return <code>true</code> if and only if the specified value is greater
     *         than or equal to the current register value.  <code>false</code>
     *         otherwise.
     * @see #getRegister(long)
     * @see #setRegister(long, long)
     * @see java.lang.Math#max(long, long)
     */
    // NOTE:  if this changes then setRegister() must change
    public boolean setMaxRegister(final long registerIndex, final long value) {
        final long bitIndex = registerIndex * registerWidth;
        final int firstWordIndex = (int)(bitIndex >>> LOG2_BITS_PER_WORD)/*aka (bitIndex / BITS_PER_WORD)*/;
        final int secondWordIndex = (int)((bitIndex + registerWidth - 1) >>> LOG2_BITS_PER_WORD)/*see above*/;
        final int bitRemainder = (int)(bitIndex & BITS_PER_WORD_MASK)/*aka (bitIndex % BITS_PER_WORD)*/;

        // NOTE:  matches getRegister()
        final long registerValue;
        final long words[] = this.words/*for convenience/performance*/;
        if (firstWordIndex == secondWordIndex)
            registerValue = ((words[firstWordIndex] >>> bitRemainder) & registerMask);
        else /*register spans words*/
            registerValue = (words[firstWordIndex] >>> bitRemainder)/*no need to mask since at top of word*/
                | (words[secondWordIndex] << (BITS_PER_WORD - bitRemainder)) & registerMask;

        // determine which is the larger and update as necessary
        if (value > registerValue) {
            // NOTE:  matches setRegister()
            if (firstWordIndex == secondWordIndex) {
                // clear then set
                words[firstWordIndex] &= ~(registerMask << bitRemainder);
                words[firstWordIndex] |= (value << bitRemainder);
            }
            else {
                /*register spans words*/
                // clear then set each partial word
                words[firstWordIndex] &= (1L << bitRemainder) - 1;
                words[firstWordIndex] |= (value << bitRemainder);

                words[secondWordIndex] &= ~(registerMask >>> (BITS_PER_WORD - bitRemainder));
                words[secondWordIndex] |= (value >>> (BITS_PER_WORD - bitRemainder));
            }
        } /* else -- the register value is greater (or equal) so nothing needs to be done */

        return (value >= registerValue);
    }

    // ========================================================================
    /**
     * Fills this bit vector with the specified bit value.  This can be used to
     * clear the vector by specifying <code>0</code>.
     *
     * @param  val the value to set all bits to (only the lowest bit is used)
     */
    public void fill(final long val) {
        for (long i = 0; i < count; i++)
            setRegister(i, val);
    }

    // ------------------------------------------------------------------------
    /**
     * Serializes the registers of the vector using the specified serializer.
     *
     * @param serializer the serializer to use. This cannot be <code>null</code>.
     */
    public void getRegisterContents(final IWordSerializer serializer) {
        for (final LongIterator iter = registerIterator(); iter.hasNext();)
            serializer.writeWord(iter.next());
    }

    /**
     * Creates a deep copy of this vector.
     *
     * @see java.lang.Object#clone()
     */
    @Override public BitVector clone() {
        final BitVector copy = new BitVector(registerWidth, count);

        System.arraycopy(words, 0, copy.words, 0, words.length);

        return copy;
    }
}
