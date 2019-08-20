/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import org.h2.util.Bits;

/**
 * An implementation of the AES block cipher algorithm,
 * also known as Rijndael. Only AES-128 is supported by this class.
 */
public class AES implements BlockCipher {

    private static final int[] RCON = new int[10];
    private static final int[] FS = new int[256];
    private static final int[] FT0 = new int[256];
    private static final int[] FT1 = new int[256];
    private static final int[] FT2 = new int[256];
    private static final int[] FT3 = new int[256];
    private static final int[] RS = new int[256];
    private static final int[] RT0 = new int[256];
    private static final int[] RT1 = new int[256];
    private static final int[] RT2 = new int[256];
    private static final int[] RT3 = new int[256];
    private final int[] encKey = new int[44];
    private final int[] decKey = new int[44];

    private static int rot8(int x) {
        return (x >>> 8) | (x << 24);
    }

    private static int xtime(int x) {
        return ((x << 1) ^ (((x & 0x80) != 0) ? 0x1b : 0)) & 255;
    }

    private static int mul(int[] pow, int[] log, int x, int y) {
        return (x != 0 && y != 0) ? pow[(log[x] + log[y]) % 255] : 0;
    }

    static {
        int[] pow = new int[256];
        int[] log = new int[256];
        for (int i = 0, x = 1; i < 256; i++, x ^= xtime(x)) {
            pow[i] = x;
            log[x] = i;
        }
        for (int i = 0, x = 1; i < 10; i++, x = xtime(x)) {
            RCON[i] = x << 24;
        }
        FS[0x00] = 0x63;
        RS[0x63] = 0x00;
        for (int i = 1; i < 256; i++) {
            int x = pow[255 - log[i]], y = x;
            y = ((y << 1) | (y >> 7)) & 255;
            x ^= y;
            y = ((y << 1) | (y >> 7)) & 255;
            x ^= y;
            y = ((y << 1) | (y >> 7)) & 255;
            x ^= y;
            y = ((y << 1) | (y >> 7)) & 255;
            x ^= y ^ 0x63;
            FS[i] = x & 255;
            RS[x] = i & 255;
        }
        for (int i = 0; i < 256; i++) {
            int x = FS[i], y = xtime(x);
            FT0[i] = (x ^ y) ^ (x << 8) ^ (x << 16) ^ (y << 24);
            FT1[i] = rot8(FT0[i]);
            FT2[i] = rot8(FT1[i]);
            FT3[i] = rot8(FT2[i]);
            y = RS[i];
            RT0[i] = mul(pow, log, 0x0b, y) ^ (mul(pow, log, 0x0d, y) << 8)
                ^ (mul(pow, log, 0x09, y) << 16) ^ (mul(pow, log, 0x0e, y) << 24);
            RT1[i] = rot8(RT0[i]);
            RT2[i] = rot8(RT1[i]);
            RT3[i] = rot8(RT2[i]);
        }
    }

    private static int getDec(int t) {
        return RT0[FS[(t >> 24) & 255]] ^ RT1[FS[(t >> 16) & 255]]
                ^ RT2[FS[(t >> 8) & 255]] ^ RT3[FS[t & 255]];
    }

    @Override
    public void setKey(byte[] key) {
        for (int i = 0, j = 0; i < 4; i++) {
            encKey[i] = decKey[i] = ((key[j++] & 255) << 24)
                    | ((key[j++] & 255) << 16) | ((key[j++] & 255) << 8)
                    | (key[j++] & 255);
        }
        int e = 0;
        for (int i = 0; i < 10; i++, e += 4) {
            encKey[e + 4] = encKey[e] ^ RCON[i]
                    ^ (FS[(encKey[e + 3] >> 16) & 255] << 24)
                    ^ (FS[(encKey[e + 3] >> 8) & 255] << 16)
                    ^ (FS[(encKey[e + 3]) & 255] << 8)
                    ^ FS[(encKey[e + 3] >> 24) & 255];
            encKey[e + 5] = encKey[e + 1] ^ encKey[e + 4];
            encKey[e + 6] = encKey[e + 2] ^ encKey[e + 5];
            encKey[e + 7] = encKey[e + 3] ^ encKey[e + 6];
        }
        int d = 0;
        decKey[d++] = encKey[e++];
        decKey[d++] = encKey[e++];
        decKey[d++] = encKey[e++];
        decKey[d++] = encKey[e++];
        for (int i = 1; i < 10; i++) {
            e -= 8;
            decKey[d++] = getDec(encKey[e++]);
            decKey[d++] = getDec(encKey[e++]);
            decKey[d++] = getDec(encKey[e++]);
            decKey[d++] = getDec(encKey[e++]);
        }
        e -= 8;
        decKey[d++] = encKey[e++];
        decKey[d++] = encKey[e++];
        decKey[d++] = encKey[e++];
        decKey[d] = encKey[e];
    }

    @Override
    public void encrypt(byte[] bytes, int off, int len) {
        for (int i = off; i < off + len; i += 16) {
            encryptBlock(bytes, bytes, i);
        }
    }

    @Override
    public void decrypt(byte[] bytes, int off, int len) {
        for (int i = off; i < off + len; i += 16) {
            decryptBlock(bytes, bytes, i);
        }
    }

    private void encryptBlock(byte[] in, byte[] out, int off) {
        int[] k = encKey;
        int x0 = Bits.readInt(in, off) ^ k[0];
        int x1 = Bits.readInt(in, off + 4) ^ k[1];
        int x2 = Bits.readInt(in, off + 8) ^ k[2];
        int x3 = Bits.readInt(in, off + 12) ^ k[3];
        int y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255]
                ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[4];
        int y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255]
                ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[5];
        int y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255]
                ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[6];
        int y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255]
                ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[7];
        x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255]
                ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[8];
        x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255]
                ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[9];
        x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255]
                ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[10];
        x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255]
                ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[11];
        y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255]
                ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[12];
        y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255]
                ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[13];
        y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255]
                ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[14];
        y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255]
                ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[15];
        x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255]
                ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[16];
        x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255]
                ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[17];
        x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255]
                ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[18];
        x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255]
                ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[19];
        y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255]
                ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[20];
        y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255]
                ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[21];
        y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255]
                ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[22];
        y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255]
                ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[23];
        x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255]
                ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[24];
        x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255]
                ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[25];
        x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255]
                ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[26];
        x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255]
                ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[27];
        y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255]
                ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[28];
        y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255]
                ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[29];
        y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255]
                ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[30];
        y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255]
                ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[31];
        x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255]
                ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[32];
        x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255]
                ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[33];
        x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255]
                ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[34];
        x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255]
                ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[35];
        y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255]
                ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[36];
        y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255]
                ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[37];
        y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255]
                ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[38];
        y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255]
                ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[39];
        x0 = ((FS[(y0 >> 24) & 255] << 24) | (FS[(y1 >> 16) & 255] << 16)
                | (FS[(y2 >> 8) & 255] << 8) | FS[y3 & 255]) ^ k[40];
        x1 = ((FS[(y1 >> 24) & 255] << 24) | (FS[(y2 >> 16) & 255] << 16)
                | (FS[(y3 >> 8) & 255] << 8) | FS[y0 & 255]) ^ k[41];
        x2 = ((FS[(y2 >> 24) & 255] << 24) | (FS[(y3 >> 16) & 255] << 16)
                | (FS[(y0 >> 8) & 255] << 8) | FS[y1 & 255]) ^ k[42];
        x3 = ((FS[(y3 >> 24) & 255] << 24) | (FS[(y0 >> 16) & 255] << 16)
                | (FS[(y1 >> 8) & 255] << 8) | FS[y2 & 255]) ^ k[43];
        Bits.writeInt(out, off, x0);
        Bits.writeInt(out, off + 4, x1);
        Bits.writeInt(out, off + 8, x2);
        Bits.writeInt(out, off + 12, x3);
    }

    private void decryptBlock(byte[] in, byte[] out, int off) {
        int[] k = decKey;
        int x0 = Bits.readInt(in, off) ^ k[0];
        int x1 = Bits.readInt(in, off + 4) ^ k[1];
        int x2 = Bits.readInt(in, off + 8) ^ k[2];
        int x3 = Bits.readInt(in, off + 12) ^ k[3];
        int y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255]
                ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[4];
        int y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255]
                ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[5];
        int y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255]
                ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[6];
        int y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255]
                ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[7];
        x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255]
                ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[8];
        x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255]
                ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[9];
        x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255]
                ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[10];
        x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255]
                ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[11];
        y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255]
                ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[12];
        y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255]
                ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[13];
        y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255]
                ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[14];
        y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255]
                ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[15];
        x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255]
                ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[16];
        x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255]
                ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[17];
        x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255]
                ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[18];
        x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255]
                ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[19];
        y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255]
                ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[20];
        y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255]
                ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[21];
        y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255]
                ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[22];
        y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255]
                ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[23];
        x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255]
                ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[24];
        x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255]
                ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[25];
        x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255]
                ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[26];
        x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255]
                ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[27];
        y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255]
                ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[28];
        y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255]
                ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[29];
        y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255]
                ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[30];
        y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255]
                ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[31];
        x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255]
                ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[32];
        x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255]
                ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[33];
        x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255]
                ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[34];
        x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255]
                ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[35];
        y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255]
                ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[36];
        y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255]
                ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[37];
        y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255]
                ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[38];
        y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255]
                ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[39];
        x0 = ((RS[(y0 >> 24) & 255] << 24) | (RS[(y3 >> 16) & 255] << 16)
                | (RS[(y2 >> 8) & 255] << 8) | RS[y1 & 255]) ^ k[40];
        x1 = ((RS[(y1 >> 24) & 255] << 24) | (RS[(y0 >> 16) & 255] << 16)
                | (RS[(y3 >> 8) & 255] << 8) | RS[y2 & 255]) ^ k[41];
        x2 = ((RS[(y2 >> 24) & 255] << 24) | (RS[(y1 >> 16) & 255] << 16)
                | (RS[(y0 >> 8) & 255] << 8) | RS[y3 & 255]) ^ k[42];
        x3 = ((RS[(y3 >> 24) & 255] << 24) | (RS[(y2 >> 16) & 255] << 16)
                | (RS[(y1 >> 8) & 255] << 8) | RS[y0 & 255]) ^ k[43];
        Bits.writeInt(out, off, x0);
        Bits.writeInt(out, off + 4, x1);
        Bits.writeInt(out, off + 8, x2);
        Bits.writeInt(out, off + 12, x3);
    }

    @Override
    public int getKeyLength() {
        return 16;
    }

}
