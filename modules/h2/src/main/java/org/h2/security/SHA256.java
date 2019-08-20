/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.h2.util.Bits;

/**
 * This class implements the cryptographic hash function SHA-256.
 */
public class SHA256 {

    private SHA256() {
    }

    /**
     * Calculate the hash code by using the given salt. The salt is appended
     * after the data before the hash code is calculated. After generating the
     * hash code, the data and all internal buffers are filled with zeros to
     * avoid keeping insecure data in memory longer than required (and possibly
     * swapped to disk).
     *
     * @param data the data to hash
     * @param salt the salt to use
     * @return the hash code
     */
    public static byte[] getHashWithSalt(byte[] data, byte[] salt) {
        byte[] buff = new byte[data.length + salt.length];
        System.arraycopy(data, 0, buff, 0, data.length);
        System.arraycopy(salt, 0, buff, data.length, salt.length);
        return getHash(buff, true);
    }

    /**
     * Calculate the hash of a password by prepending the user name and a '@'
     * character. Both the user name and the password are encoded to a byte
     * array using UTF-16. After generating the hash code, the password array
     * and all internal buffers are filled with zeros to avoid keeping the plain
     * text password in memory longer than required (and possibly swapped to
     * disk).
     *
     * @param userName the user name
     * @param password the password
     * @return the hash code
     */
    public static byte[] getKeyPasswordHash(String userName, char[] password) {
        String user = userName + "@";
        byte[] buff = new byte[2 * (user.length() + password.length)];
        int n = 0;
        for (int i = 0, length = user.length(); i < length; i++) {
            char c = user.charAt(i);
            buff[n++] = (byte) (c >> 8);
            buff[n++] = (byte) c;
        }
        for (char c : password) {
            buff[n++] = (byte) (c >> 8);
            buff[n++] = (byte) c;
        }
        Arrays.fill(password, (char) 0);
        return getHash(buff, true);
    }

    /**
     * Calculate the hash-based message authentication code.
     *
     * @param key the key
     * @param message the message
     * @return the hash
     */
    public static byte[] getHMAC(byte[] key, byte[] message) {
        return initMac(key).doFinal(message);
    }

    private static Mac initMac(byte[] key) {
        // Java forbids empty keys
        if (key.length == 0) {
            key = new byte[1];
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac;
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculate the hash using the password-based key derivation function 2.
     *
     * @param password the password
     * @param salt the salt
     * @param iterations the number of iterations
     * @param resultLen the number of bytes in the result
     * @return the result
     */
    public static byte[] getPBKDF2(byte[] password, byte[] salt,
            int iterations, int resultLen) {
        byte[] result = new byte[resultLen];
        Mac mac = initMac(password);
        int len = 64 + Math.max(32, salt.length + 4);
        byte[] message = new byte[len];
        byte[] macRes = null;
        for (int k = 1, offset = 0; offset < resultLen; k++, offset += 32) {
            for (int i = 0; i < iterations; i++) {
                if (i == 0) {
                    System.arraycopy(salt, 0, message, 0, salt.length);
                    Bits.writeInt(message, salt.length, k);
                    len = salt.length + 4;
                } else {
                    System.arraycopy(macRes, 0, message, 0, 32);
                    len = 32;
                }
                mac.update(message, 0, len);
                macRes = mac.doFinal();
                for (int j = 0; j < 32 && j + offset < resultLen; j++) {
                    result[j + offset] ^= macRes[j];
                }
            }
        }
        Arrays.fill(password, (byte) 0);
        return result;
    }

    /**
     * Calculate the hash code for the given data.
     *
     * @param data the data to hash
     * @param nullData if the data should be filled with zeros after calculating
     *            the hash code
     * @return the hash code
     */
    public static byte[] getHash(byte[] data, boolean nullData) {
        byte[] result;
        try {
            result = MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        if (nullData) {
            Arrays.fill(data, (byte) 0);
        }
        return result;
    }

}
