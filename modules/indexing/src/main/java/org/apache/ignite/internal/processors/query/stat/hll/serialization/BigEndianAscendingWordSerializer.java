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
package org.apache.ignite.internal.processors.query.stat.hll.serialization;

/**
 * A serializer that writes a sequence of fixed bit-width 'words' to a byte array.
 * Bitwise OR is used to write words into bytes, so a low bit in a word is also
 * a low bit in a byte. However, a high byte in a word is written at a lower index
 * in the array than a low byte in a word. The first word is written at the lowest
 * array index. Each serializer is one time use and returns its backing byte
 * array.<p/>
 *
 * This encoding was chosen so that when reading bytes as octets in the typical
 * first-octet-is-the-high-nibble fashion, an octet-to-binary conversion
 * would yield a high-to-low, left-to-right view of the "short words".<p/>
 *
 * Example:<p/>
 *
 * Say short words are 5 bits wide. Our word sequence is the values
 * <code>[31, 1, 5]</code>. In big-endian binary format, the values are
 * <code>[0b11111, 0b00001, 0b00101]</code>. We use 15 of 16 bits in two bytes
 * and pad the last (lowest) bit of the last byte with a zero:
 *
 * <code>
 *  [0b11111000, 0b01001010] = [0xF8, 0x4A]
 * </code>.
 *
 * @author timon
 */
public class BigEndianAscendingWordSerializer implements IWordSerializer {
    // The number of bits per byte.
    private static final int BITS_PER_BYTE = 8;

    // ************************************************************************
    // The length in bits of the words to be written.
    private final int wordLength;
    // The number of words to be written.
    private final int wordCount;

    // The byte array to which the words are serialized.
    private final byte[] bytes;

    // ------------------------------------------------------------------------
    // Write state
    // Number of bits that remain writable in the current byte.
    private int bitsLeftInByte;
    // Index of byte currently being written to.
    private int byteIndex;
    // Number of words written.
    private int wordsWritten;

    // ========================================================================
    /**
     * @param wordLength the length in bits of the words to be serialized. Must
     *        be greater than or equal to 1 and less than or equal to 64.
     * @param wordCount the number of words to be serialized. Must be greater than
     *        or equal to zero.
     * @param bytePadding the number of leading bytes that should pad the
     *        serialized words. Must be greater than or equal to zero.
     */
    public BigEndianAscendingWordSerializer(final int wordLength, final int wordCount, final int bytePadding) {
        if((wordLength < 1) || (wordLength > 64)) {
            throw new IllegalArgumentException("Word length must be >= 1 and <= 64. (was: " + wordLength + ")");
        }
        if(wordCount < 0) {
            throw new IllegalArgumentException("Word count must be >= 0. (was: " + wordCount + ")");
        }
        if(bytePadding < 0) {
            throw new IllegalArgumentException("Byte padding must be must be >= 0. (was: " + bytePadding + ")");
        }

        this.wordLength = wordLength;
        this.wordCount = wordCount;

        final long bitsRequired = (wordLength * wordCount);
        final boolean leftoverBits = ((bitsRequired % BITS_PER_BYTE) != 0);
        final int bytesRequired = (int)(bitsRequired / BITS_PER_BYTE) + (leftoverBits ? 1 : 0) + bytePadding;
        bytes = new byte[bytesRequired];

        bitsLeftInByte = BITS_PER_BYTE;
        byteIndex = bytePadding;
        wordsWritten = 0;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IWordSerializer#writeWord(long)
     * @throws RuntimeException if the number of words written is greater than the
     *         <code>wordCount</code> parameter in the constructor.
     */
    @Override
    public void writeWord(final long word) {
        if(wordsWritten == wordCount) {
            throw new RuntimeException("Cannot write more words, backing array full!");
        }

        int bitsLeftInWord = wordLength;

        while(bitsLeftInWord > 0) {
            // Move to the next byte if the current one is fully packed.
            if(bitsLeftInByte == 0) {
                byteIndex++;
                bitsLeftInByte = BITS_PER_BYTE;
            }

            final long consumedMask;
            if(bitsLeftInWord == 64) {
                consumedMask = ~0L;
            } else {
                consumedMask = ((1L << bitsLeftInWord) - 1L);
            }

            // Fix how many bits will be written in this cycle. Choose the
            // smaller of the remaining bits in the word or byte.
            final int numberOfBitsToWrite = Math.min(bitsLeftInByte, bitsLeftInWord);
            final int bitsInByteRemainingAfterWrite = (bitsLeftInByte - numberOfBitsToWrite);

            // In general, we write the highest bits of the word first, so we
            // strip the highest bits that were consumed in previous cycles.
            final long remainingBitsOfWordToWrite = (word & consumedMask);

            final long bitsThatTheByteCanAccept;
            // If there is more left in the word than can be written to this
            // byte, shift off the bits that can't be written off the bottom.
            if(bitsLeftInWord > numberOfBitsToWrite) {
                bitsThatTheByteCanAccept = (remainingBitsOfWordToWrite >>> (bitsLeftInWord - bitsLeftInByte));
            } else {
                // If the byte can accept all remaining bits, there is no need
                // to shift off the bits that won't be written in this cycle.
                bitsThatTheByteCanAccept = remainingBitsOfWordToWrite;
            }

            // Align the word bits to write up against the byte bits that have
            // already been written. This shift may do nothing if the remainder
            // of the byte is being consumed in this cycle.
            final long alignedBits = (bitsThatTheByteCanAccept << bitsInByteRemainingAfterWrite);

            // Update the byte with the alignedBits.
            bytes[byteIndex] |= (byte)alignedBits;

            // Update state with bit count written.
            bitsLeftInWord -= numberOfBitsToWrite;
            bitsLeftInByte = bitsInByteRemainingAfterWrite;
        }

        wordsWritten ++;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IWordSerializer#getBytes()
     * @throws RuntimeException if the number of words written is fewer than the
     *         <code>wordCount</code> parameter in the constructor.
     */
    @Override
    public byte[] getBytes() {
        if(wordsWritten < wordCount) {
            throw new RuntimeException("Not all words have been written! (" + wordsWritten + "/" + wordCount + ")");
        }

        return bytes;
    }
}
