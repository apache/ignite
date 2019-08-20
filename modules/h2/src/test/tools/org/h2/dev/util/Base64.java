/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This class converts binary to base64 and vice versa.
 */
public class Base64 {

    private static final byte[] CODE = new byte[64];
    private static final byte[] REV = new byte[256];

    private Base64() {
        // utility class
    }

    static {
        for (int i = 'A'; i <= 'Z'; i++) {
            CODE[i - 'A'] = (byte) i;
            CODE[i - 'A' + 26] = (byte) (i + 'a' - 'A');
        }
        for (int i = 0; i < 10; i++) {
            CODE[i + 2 * 26] = (byte) ('0' + i);
        }
        CODE[62] = (byte) '+';
        CODE[63] = (byte) '/';
        for (int i = 0; i < 255; i++) {
            REV[i] = -1;
        }
        for (int i = 0; i < 64; i++) {
            REV[CODE[i]] = (byte) i;
        }
    }

    private static void check(String a, String b) {
        if (!a.equals(b)) {
            throw new RuntimeException("mismatch: " + a + " <> " + b);
        }
    }

    /**
     * Run the tests.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        check(new String(encode(new byte[] {})), "");
        check(new String(encode("A".getBytes())), "QQ==");
        check(new String(encode("AB".getBytes())), "QUI=");
        check(new String(encode("ABC".getBytes())), "QUJD");
        check(new String(encode("ABCD".getBytes())), "QUJDRA==");
        check(new String(decode(new byte[] {})), "");
        check(new String(decode("QQ==".getBytes())), "A");
        check(new String(decode("QUI=".getBytes())), "AB");
        check(new String(decode("QUJD".getBytes())), "ABC");
        check(new String(decode("QUJDRA==".getBytes())), "ABCD");
        int len = 10000;
        test(false, len);
        test(true, len);
        test(false, len);
        test(true, len);
    }

    private static void test(boolean fast, int len) {
        Random random = new Random(10);
        long time = System.nanoTime();
        byte[] bin = new byte[len];
        random.nextBytes(bin);
        for (int i = 0; i < len; i++) {
            byte[] dec;
            if (fast) {
                byte[] enc = encodeFast(bin);
                dec = decodeFast(enc);
            } else {
                byte[] enc = encode(bin);
                dec = decode(enc);
            }
            test(bin, dec);
        }
        time = System.nanoTime() - time;
        System.out.println("fast=" + fast + " time=" + TimeUnit.NANOSECONDS.toMillis(time));
    }

    private static void test(byte[] in, byte[] out) {
        if (in.length != out.length) {
            throw new RuntimeException("Length error");
        }
        for (int i = 0; i < in.length; i++) {
            if (in[i] != out[i]) {
                throw new RuntimeException("Error at " + i);
            }
        }
    }

    private static byte[] encode(byte[] bin) {
        byte[] code = CODE;
        int size = bin.length;
        int len = ((size + 2) / 3) * 4;
        byte[] enc = new byte[len];
        int fast = size / 3 * 3, i = 0, j = 0;
        for (; i < fast; i += 3, j += 4) {
            int a = ((bin[i] & 255) << 16) + ((bin[i + 1] & 255) << 8) + (bin[i + 2] & 255);
            enc[j] = code[a >> 18];
            enc[j + 1] = code[(a >> 12) & 63];
            enc[j + 2] = code[(a >> 6) & 63];
            enc[j + 3] = code[a & 63];
        }
        if (i < size) {
            int a = (bin[i++] & 255) << 16;
            enc[j] = code[a >> 18];
            if (i < size) {
                a += (bin[i] & 255) << 8;
                enc[j + 2] = code[(a >> 6) & 63];
            } else {
                enc[j + 2] = (byte) '=';
            }
            enc[j + 1] = code[(a >> 12) & 63];
            enc[j + 3] = (byte) '=';
        }
        return enc;
    }

    private static byte[] encodeFast(byte[] bin) {
        byte[] code = CODE;
        int size = bin.length;
        int len = ((size * 4) + 2) / 3;
        byte[] enc = new byte[len];
        int fast = size / 3 * 3, i = 0, j = 0;
        for (; i < fast; i += 3, j += 4) {
            int a = ((bin[i] & 255) << 16) + ((bin[i + 1] & 255) << 8) + (bin[i + 2] & 255);
            enc[j] = code[a >> 18];
            enc[j + 1] = code[(a >> 12) & 63];
            enc[j + 2] = code[(a >> 6) & 63];
            enc[j + 3] = code[a & 63];
        }
        if (i < size) {
            int a = (bin[i++] & 255) << 16;
            enc[j] = code[a >> 18];
            if (i < size) {
                a += (bin[i] & 255) << 8;
                enc[j + 2] = code[(a >> 6) & 63];
            }
            enc[j + 1] = code[(a >> 12) & 63];
        }
        return enc;
    }

    private static byte[] trim(byte[] enc) {
        byte[] rev = REV;
        int j = 0, size = enc.length;
        if (size > 1 && enc[size - 2] == '=') {
            size--;
        }
        if (size > 0 && enc[size - 1] == '=') {
            size--;
        }
        for (int i = 0; i < size; i++) {
            if (rev[enc[i] & 255] < 0) {
                j++;
            }
        }
        if (j == 0) {
            return enc;
        }
        byte[] buff = new byte[size - j];
        for (int i = 0, k = 0; i < size; i++) {
            int x = enc[i] & 255;
            if (rev[x] >= 0) {
                buff[k++] = (byte) x;
            }
        }
        return buff;
    }

    private static byte[] decode(byte[] enc) {
        enc = trim(enc);
        byte[] rev = REV;
        int len = enc.length, size = (len * 3) / 4;
        if (len > 0 && enc[len - 1] == '=') {
            size--;
            if (len > 1 && enc[len - 2] == '=') {
                size--;
            }
        }
        byte[] bin = new byte[size];
        int fast = size / 3 * 3, i = 0, j = 0;
        for (; i < fast; i += 3, j += 4) {
            int a = (rev[enc[j] & 255] << 18) + (rev[enc[j + 1] & 255] << 12) +
                    (rev[enc[j + 2] & 255] << 6) + rev[enc[j + 3] & 255];
            bin[i] = (byte) (a >> 16);
            bin[i + 1] = (byte) (a >> 8);
            bin[i + 2] = (byte) a;
        }
        if (i < size) {
            int a = (rev[enc[j] & 255] << 10) + (rev[enc[j + 1] & 255] << 4);
            bin[i++] = (byte) (a >> 8);
            if (i < size) {
                a += rev[enc[j + 2] & 255] >> 2;
                bin[i] = (byte) a;
            }
        }
        return bin;
    }

    private static byte[] decodeFast(byte[] enc) {
        byte[] rev = REV;
        int len = enc.length, size = (len * 3) / 4;
        byte[] bin = new byte[size];
        int fast = size / 3 * 3, i = 0, j = 0;
        for (; i < fast; i += 3, j += 4) {
            int a = (rev[enc[j] & 255] << 18) + (rev[enc[j + 1] & 255] << 12) +
                    (rev[enc[j + 2] & 255] << 6) + rev[enc[j + 3] & 255];
            bin[i] = (byte) (a >> 16);
            bin[i + 1] = (byte) (a >> 8);
            bin[i + 2] = (byte) a;
        }
        if (i < size) {
            int a = (rev[enc[j] & 255] << 10) + (rev[enc[j + 1] & 255] << 4);
            bin[i++] = (byte) (a >> 8);
            if (i < size) {
                a += rev[enc[j + 2] & 255] >> 2;
                bin[i] = (byte) a;
            }
        }
        return bin;
    }

}
