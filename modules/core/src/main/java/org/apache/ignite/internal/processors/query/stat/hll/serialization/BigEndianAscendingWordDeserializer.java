package org.apache.ignite.internal.processors.query.stat.hll.serialization;

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

/**
 * A corresponding deserializer for {@link BigEndianAscendingWordSerializer}.
 *
 * @author timon
 */
public class BigEndianAscendingWordDeserializer implements IWordDeserializer {
    /** The number of bits per byte. */
    private static final int BITS_PER_BYTE = 8;

    /** long mask for the maximum value stored in a byte. */
    private static final long BYTE_MASK = (1L << BITS_PER_BYTE) - 1L;

    // ************************************************************************
    /** The length in bits of the words to be read. */
    private final int wordLength;

    /** The byte array to which the words are serialized. */
    private final byte[] bytes;

    /** The number of leading padding bytes in 'bytes' to be ignored. */
    private final int bytePadding;

    /** The number of words that the byte array contains. */
    private final int wordCount;

    /** The current read state. */
    private int currentWordIndex;

    // ========================================================================
    /**
     * @param wordLength the length in bits of the words to be deserialized. Must
     *        be less than or equal to 64 and greater than or equal to 1.
     * @param bytePadding the number of leading bytes that pad the serialized words.
     *        Must be greater than or equal to zero.
     * @param bytes the byte array containing the serialized words. Cannot be
     *        <code>null</code>.
     */
    public BigEndianAscendingWordDeserializer(final int wordLength, final int bytePadding, final byte[] bytes) {
        if ((wordLength < 1) || (wordLength > 64))
            throw new IllegalArgumentException("Word length must be >= 1 and <= 64. (was: " + wordLength + ")");

        if (bytePadding < 0)
            throw new IllegalArgumentException("Byte padding must be >= zero. (was: " + bytePadding + ")");

        this.wordLength = wordLength;
        this.bytes = bytes;
        this.bytePadding = bytePadding;

        final int dataBytes = (bytes.length - bytePadding);
        final long dataBits = (dataBytes * BITS_PER_BYTE);

        this.wordCount = (int)(dataBits / wordLength);

        currentWordIndex = 0;
    }

    // ========================================================================
    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IWordDeserializer#readWord()
     */
    @Override public long readWord() {
        final long word = readWord(currentWordIndex);
        currentWordIndex++;

        return word;
    }

    // ------------------------------------------------------------------------
    /**
     * Reads the word at the specified sequence position (zero-indexed).
     *
     * @param  position the zero-indexed position of the word to be read. This
     *         must be greater than or equal to zero.
     * @return the value of the serialized word at the specified position.
     */
    private long readWord(final int position) {
        if (position < 0)
            throw new ArrayIndexOutOfBoundsException(position);

        // First bit of the word
        final long firstBitIndex = (position * wordLength);
        final int firstByteIndex = (bytePadding + (int)(firstBitIndex / BITS_PER_BYTE));
        final int firstByteSkipBits = (int)(firstBitIndex % BITS_PER_BYTE);

        // Last bit of the word
        final long lastBitIndex = (firstBitIndex + wordLength - 1);
        final int lastByteIndex = (bytePadding + (int)(lastBitIndex / BITS_PER_BYTE));
        final int lastByteBitsToConsume;

        final int bitsAfterByteBoundary = (int)((lastBitIndex + 1) % BITS_PER_BYTE);
        // If the word terminates at the end of the last byte, consume the whole
        // last byte.
        if (bitsAfterByteBoundary == 0)
            lastByteBitsToConsume = BITS_PER_BYTE;
        else
            // Otherwise, only consume what is necessary.
            lastByteBitsToConsume = bitsAfterByteBoundary;

        if (lastByteIndex >= bytes.length)
            throw new ArrayIndexOutOfBoundsException("Word out of bounds of backing array.");

        // Accumulator
        long value = 0;

        // --------------------------------------------------------------------
        // First byte
        final int bitsRemainingInFirstByte = (BITS_PER_BYTE - firstByteSkipBits);
        final int bitsToConsumeInFirstByte = Math.min(bitsRemainingInFirstByte, wordLength);
        long firstByte = (long)bytes[firstByteIndex];

        // Mask off the bits to skip in the first byte.
        final long firstByteMask = ((1L << bitsRemainingInFirstByte) - 1L);
        firstByte &= firstByteMask;
        // Right-align relevant bits of first byte.
        firstByte >>>= (bitsRemainingInFirstByte - bitsToConsumeInFirstByte);

        value |= firstByte;

        // If the first byte contains the whole word, short-circuit.
        if (firstByteIndex == lastByteIndex)
            return value;

        // --------------------------------------------------------------------
        // Middle bytes
        final int middleByteCount = (lastByteIndex - firstByteIndex - 1);

        for (int i = 0; i < middleByteCount; i++) {
            final long middleByte = (bytes[firstByteIndex + i + 1] & BYTE_MASK);
            // Push middle byte onto accumulator.
            value <<= BITS_PER_BYTE;
            value |= middleByte;
        }

        // --------------------------------------------------------------------
        // Last byte
        long lastByte = (bytes[lastByteIndex] & BYTE_MASK);
        lastByte >>= (BITS_PER_BYTE - lastByteBitsToConsume);
        value <<= lastByteBitsToConsume;
        value |= lastByte;

        return value;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IWordDeserializer#totalWordCount()
     */
    @Override public int totalWordCount() {
        return wordCount;
    }
}
