/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.Bits;

/**
 * An implementation of the XTEA block cipher algorithm.
 * <p>
 * This implementation uses 32 rounds.
 * The best attack reported as of 2009 is 36 rounds (Wikipedia).
 */
public class XTEA implements BlockCipher {

    private static final int DELTA = 0x9E3779B9;
    private int k0, k1, k2, k3, k4, k5, k6, k7;
    private int k8, k9, k10, k11, k12, k13, k14, k15;
    private int k16, k17, k18, k19, k20, k21, k22, k23;
    private int k24, k25, k26, k27, k28, k29, k30, k31;

    @Override
    public void setKey(byte[] b) {
        int[] key = new int[4];
        for (int i = 0; i < 16; i += 4) {
            key[i / 4] = Bits.readInt(b, i);
        }
        int[] r = new int[32];
        for (int i = 0, sum = 0; i < 32;) {
            r[i++] = sum + key[sum & 3];
            sum += DELTA;
            r[i++] = sum + key[ (sum >>> 11) & 3];
        }
        k0 = r[0]; k1 = r[1]; k2 = r[2]; k3 = r[3];
        k4 = r[4]; k5 = r[5]; k6 = r[6]; k7 = r[7];
        k8 = r[8]; k9 = r[9]; k10 = r[10]; k11 = r[11];
        k12 = r[12]; k13 = r[13]; k14 = r[14]; k15 = r[15];
        k16 = r[16]; k17 = r[17]; k18 = r[18]; k19 = r[19];
        k20 = r[20]; k21 = r[21]; k22 = r[22]; k23 = r[23];
        k24 = r[24]; k25 = r[25]; k26 = r[26]; k27 = r[27];
        k28 = r[28]; k29 = r[29]; k30 = r[30]; k31 = r[31];
    }

    @Override
    public void encrypt(byte[] bytes, int off, int len) {
        if (SysProperties.CHECK) {
            if (len % ALIGN != 0) {
                DbException.throwInternalError("unaligned len " + len);
            }
        }
        for (int i = off; i < off + len; i += 8) {
            encryptBlock(bytes, bytes, i);
        }
    }

    @Override
    public void decrypt(byte[] bytes, int off, int len) {
        if (SysProperties.CHECK) {
            if (len % ALIGN != 0) {
                DbException.throwInternalError("unaligned len " + len);
            }
        }
        for (int i = off; i < off + len; i += 8) {
            decryptBlock(bytes, bytes, i);
        }
    }

    private void encryptBlock(byte[] in, byte[] out, int off) {
        int y = Bits.readInt(in, off);
        int z = Bits.readInt(in, off + 4);
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k0;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k1;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k2;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k3;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k4;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k5;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k6;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k7;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k8;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k9;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k10;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k11;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k12;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k13;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k14;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k15;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k16;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k17;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k18;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k19;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k20;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k21;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k22;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k23;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k24;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k25;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k26;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k27;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k28;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k29;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k30;
        z += (((y >>> 5) ^ (y << 4)) + y) ^ k31;
        Bits.writeInt(out, off, y);
        Bits.writeInt(out, off + 4, z);
    }

    private void decryptBlock(byte[] in, byte[] out, int off) {
        int y = Bits.readInt(in, off);
        int z = Bits.readInt(in, off + 4);
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k31;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k30;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k29;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k28;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k27;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k26;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k25;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k24;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k23;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k22;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k21;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k20;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k19;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k18;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k17;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k16;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k15;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k14;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k13;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k12;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k11;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k10;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k9;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k8;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k7;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k6;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k5;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k4;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k3;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k2;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k1;
        y -= (((z << 4) ^ (z >>> 5)) + z) ^ k0;
        Bits.writeInt(out, off, y);
        Bits.writeInt(out, off + 4, z);
    }

    @Override
    public int getKeyLength() {
        return 16;
    }

}
