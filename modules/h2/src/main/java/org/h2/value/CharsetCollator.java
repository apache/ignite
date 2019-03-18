/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.nio.charset.Charset;
import java.text.CollationKey;
import java.text.Collator;
import java.util.Comparator;

/**
 * The charset collator sorts strings according to the order in the given charset.
 */
public class CharsetCollator extends Collator {

    /**
     * The comparator used to compare byte arrays.
     */
    static final Comparator<byte[]> COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] b1, byte[] b2) {
            int minLength = Math.min(b1.length, b2.length);
            for (int index = 0; index < minLength; index++) {
                int result = b1[index] - b2[index];
                if (result != 0) {
                    return result;
                }
            }
            return b1.length - b2.length;
        }
    };
    private final Charset charset;

    public CharsetCollator(Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return charset;
    }

    @Override
    public int compare(String source, String target) {
        return COMPARATOR.compare(toBytes(source), toBytes(target));
    }

    /**
     * Convert the source to bytes, using the character set.
     *
     * @param source the source
     * @return the bytes
     */
    byte[] toBytes(String source) {
        return source.getBytes(charset);
    }

    @Override
    public CollationKey getCollationKey(final String source) {
        return new CharsetCollationKey(source);
    }

    @Override
    public int hashCode() {
        return 255;
    }

    private class CharsetCollationKey extends CollationKey {

        CharsetCollationKey(String source) {
            super(source);
        }

        @Override
        public int compareTo(CollationKey target) {
            return COMPARATOR.compare(toByteArray(), toBytes(target.getSourceString()));
        }

        @Override
        public byte[] toByteArray() {
            return toBytes(getSourceString());
        }

    }
}
