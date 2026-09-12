package org.apache.ignite.console.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
public class StringToUUID {

    /**
     * 将字符串转换为 MD5 格式的 UUID
     */
    public static UUID uuidFromString(String input) {
        try {
            // 1. 计算 MD5
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes = md.digest(input.getBytes());

            // 2. 将 MD5 字节数组转换为 UUID
            return bytesToUUID(md5Bytes);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    /**
     * 将字节数组转换为 UUID（16字节）
     */
    private static UUID bytesToUUID(byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("MD5 hash must be 16 bytes");
        }

        // 将前8字节转换为最高有效位
        long mostSigBits = 0;
        for (int i = 0; i < 8; i++) {
            mostSigBits = (mostSigBits << 8) | (bytes[i] & 0xFF);
        }

        // 将后8字节转换为最低有效位
        long leastSigBits = 0;
        for (int i = 8; i < 16; i++) {
            leastSigBits = (leastSigBits << 8) | (bytes[i] & 0xFF);
        }

        return new UUID(mostSigBits, leastSigBits);
    }
}
