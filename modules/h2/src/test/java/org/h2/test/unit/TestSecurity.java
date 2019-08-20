/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.h2.security.BlockCipher;
import org.h2.security.CipherFactory;
import org.h2.security.SHA256;
import org.h2.test.TestBase;
import org.h2.util.StringUtils;

/**
 * Tests various security primitives.
 */
public class TestSecurity extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        testConnectWithHash();
        testSHA();
        testAES();
        testBlockCiphers();
        testRemoveAnonFromLegacyAlgorithms();
        // testResetLegacyAlgorithms();
    }

    private static void testConnectWithHash() throws SQLException {
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test", "sa", "sa");
        String pwd = StringUtils.convertBytesToHex(
                SHA256.getKeyPasswordHash("SA", "sa".toCharArray()));
        Connection conn2 = DriverManager.getConnection(
                "jdbc:h2:mem:test;PASSWORD_HASH=TRUE", "sa", pwd);
        conn.close();
        conn2.close();
    }

    private void testSHA() {
        testPBKDF2();
        testHMAC();
        testOneSHA();
    }

    private void testPBKDF2() {
        // test vectors from StackOverflow (PBKDF2-HMAC-SHA2)
        assertEquals(
                "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b",
                StringUtils.convertBytesToHex(
                SHA256.getPBKDF2(
                "password".getBytes(),
                "salt".getBytes(), 1, 32)));
        assertEquals(
                "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43",
                StringUtils.convertBytesToHex(
                SHA256.getPBKDF2(
                "password".getBytes(),
                "salt".getBytes(), 2, 32)));
        assertEquals(
                "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a",
                StringUtils.convertBytesToHex(
                SHA256.getPBKDF2(
                "password".getBytes(),
                "salt".getBytes(), 4096, 32)));
        // take a very long time to calculate
        // assertEquals(
        //         "cf81c66fe8cfc04d1f31ecb65dab4089f7f179e" +
        //         "89b3b0bcb17ad10e3ac6eba46",
        //         StringUtils.convertBytesToHex(
        //         SHA256.getPBKDF2(
        //         "password".getBytes(),
        //         "salt".getBytes(), 16777216, 32)));
        assertEquals(
                "348c89dbcbd32b2f32d814b8116e84cf2b17347e" +
                "bc1800181c4e2a1fb8dd53e1c635518c7dac47e9",
                StringUtils.convertBytesToHex(
                SHA256.getPBKDF2(
                ("password" + "PASSWORD" + "password").getBytes(),
                ("salt"+ "SALT"+ "salt"+ "SALT"+ "salt"+
                "SALT"+ "salt"+ "SALT"+ "salt").getBytes(), 4096, 40)));
        assertEquals(
                "89b69d0516f829893c696226650a8687",
                StringUtils.convertBytesToHex(
                SHA256.getPBKDF2(
                "pass\0word".getBytes(),
                "sa\0lt".getBytes(), 4096, 16)));

        // the password is filled with zeroes
        byte[] password = "Test".getBytes();
        SHA256.getPBKDF2(password, "".getBytes(), 1, 16);
        assertEquals(new byte[4], password);
    }

    private void testHMAC() {
        // from Wikipedia
        assertEquals(
                "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad",
                StringUtils.convertBytesToHex(
                        SHA256.getHMAC(new byte[0], new byte[0])));
        assertEquals(
                "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
                StringUtils.convertBytesToHex(
                SHA256.getHMAC(
                "key".getBytes(),
                "The quick brown fox jumps over the lazy dog".getBytes())));
    }

    private String getHashString(byte[] data) {
        byte[] result = SHA256.getHash(data, true);
        if (data.length > 0) {
            assertEquals(0, data[0]);
        }
        return StringUtils.convertBytesToHex(result);
    }

    private void testOneSHA() {
        assertEquals(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                getHashString(new byte[] {}));
        assertEquals(
                "68aa2e2ee5dff96e3355e6c7ee373e3d6a4e17f75f9518d843709c0c9bc3e3d4",
                getHashString(new byte[] { 0x19 }));
        assertEquals(
                "175ee69b02ba9b58e2b0a5fd13819cea573f3940a94f825128cf4209beabb4e8",
                getHashString(
                        new byte[] { (byte) 0xe3, (byte) 0xd7, 0x25,
                        0x70, (byte) 0xdc, (byte) 0xdd, 0x78, 0x7c,
                        (byte) 0xe3, (byte) 0x88, 0x7a, (byte) 0xb2,
                        (byte) 0xcd, 0x68, 0x46, 0x52 }));
        checkSHA256(
                "",
                "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855");
        checkSHA256(
                "a",
                "CA978112CA1BBDCAFAC231B39A23DC4DA786EFF8147C4E72B9807785AFEE48BB");
        checkSHA256(
                "abc",
                "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD");
        checkSHA256(
                "message digest",
                "F7846F55CF23E14EEBEAB5B4E1550CAD5B509E3348FBC4EFA3A1413D393CB650");
        checkSHA256(
                "abcdefghijklmnopqrstuvwxyz",
                "71C480DF93D6AE2F1EFAD1447C66C9525E316218CF51FC8D9ED832F2DAF18B73");
        checkSHA256(
                "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
                "248D6A61D20638B8E5C026930C3E6039A33CE45964FF2167F6ECEDD419DB06C1");
        checkSHA256(
                "123456789012345678901234567890" +
                        "12345678901234567890" +
                        "123456789012345678901234567890",
                "F371BC4A311F2B009EEF952DD83CA80E2B60026C8E935592D0F9C308453C813E");
        StringBuilder buff = new StringBuilder(1000000);
        buff.append('a');
        checkSHA256(buff.toString(),
                "CA978112CA1BBDCAFAC231B39A23DC4DA786EFF8147C4E72B9807785AFEE48BB");
    }

    private void checkSHA256(String message, String expected) {
        String hash = StringUtils.convertBytesToHex(
                SHA256.getHash(message.getBytes(), true)).toUpperCase();
        assertEquals(expected, hash);
    }

    private void testBlockCiphers() {
        for (String algorithm : new String[] { "AES", "FOG" }) {
            byte[] test = new byte[4096];
            BlockCipher cipher = CipherFactory.getBlockCipher(algorithm);
            cipher.setKey("abcdefghijklmnop".getBytes());
            for (int i = 0; i < 10; i++) {
                cipher.encrypt(test, 0, test.length);
            }
            assertFalse(isCompressible(test));
            for (int i = 0; i < 10; i++) {
                cipher.decrypt(test, 0, test.length);
            }
            assertEquals(new byte[test.length], test);
            assertTrue(isCompressible(test));
        }
    }

    private void testAES() {
        BlockCipher test = CipherFactory.getBlockCipher("AES");

        String r;
        byte[] data;

        // test vector from
        // http://csrc.nist.gov/groups/STM/cavp/documents/aes/KAT_AES.zip
        // ECBVarTxt128e.txt
        // COUNT = 0
        test.setKey(StringUtils.convertHexToBytes("00000000000000000000000000000000"));
        data = StringUtils.convertHexToBytes("80000000000000000000000000000000");
        test.encrypt(data, 0, data.length);
        r = StringUtils.convertBytesToHex(data);
        assertEquals("3ad78e726c1ec02b7ebfe92b23d9ec34", r);

        // COUNT = 127
        test.setKey(StringUtils.convertHexToBytes("00000000000000000000000000000000"));
        data = StringUtils.convertHexToBytes("ffffffffffffffffffffffffffffffff");
        test.encrypt(data, 0, data.length);
        r = StringUtils.convertBytesToHex(data);
        assertEquals("3f5b8cc9ea855a0afa7347d23e8d664e", r);

        // test vector
        test.setKey(StringUtils.convertHexToBytes("2b7e151628aed2a6abf7158809cf4f3c"));
        data = StringUtils.convertHexToBytes("6bc1bee22e409f96e93d7e117393172a");
        test.encrypt(data, 0, data.length);
        r = StringUtils.convertBytesToHex(data);
        assertEquals("3ad77bb40d7a3660a89ecaf32466ef97", r);

        test.setKey(StringUtils.convertHexToBytes("000102030405060708090A0B0C0D0E0F"));
        byte[] in = new byte[128];
        byte[] enc = new byte[128];
        test.encrypt(enc, 0, 128);
        test.decrypt(enc, 0, 128);
        if (!Arrays.equals(in, enc)) {
            throw new AssertionError();
        }

        for (int i = 0; i < 10; i++) {
            test.encrypt(in, 0, 128);
            test.decrypt(enc, 0, 128);
        }
    }

    private static boolean isCompressible(byte[] data) {
        int len = data.length;
        int[] sum = new int[16];
        for (int i = 0; i < len; i++) {
            int x = (data[i] & 255) >> 4;
            sum[x]++;
        }
        int r = 0;
        for (int x : sum) {
            long v = ((long) x << 32) / len;
            r += 63 - Long.numberOfLeadingZeros(v + 1);
        }
        return len * r < len * 120;
    }

    private void testRemoveAnonFromLegacyAlgorithms() {
        String legacyAlgorithms = "K_NULL, C_NULL, M_NULL, DHE_DSS_EXPORT" +
                ", DHE_RSA_EXPORT, DH_anon_EXPORT, DH_DSS_EXPORT, DH_RSA_EXPORT, RSA_EXPORT" +
                ", DH_anon, ECDH_anon, RC4_128, RC4_40, DES_CBC, DES40_CBC";
        String expectedLegacyWithoutDhAnon = "K_NULL, C_NULL, M_NULL, DHE_DSS_EXPORT" +
                ", DHE_RSA_EXPORT, DH_anon_EXPORT, DH_DSS_EXPORT, DH_RSA_EXPORT, RSA_EXPORT" +
                ", RC4_128, RC4_40, DES_CBC, DES40_CBC";
        assertEquals(expectedLegacyWithoutDhAnon,
                CipherFactory.removeDhAnonFromCommaSeparatedList(legacyAlgorithms));

        legacyAlgorithms = "ECDH_anon, DH_anon_EXPORT, DH_anon";
        expectedLegacyWithoutDhAnon = "DH_anon_EXPORT";
        assertEquals(expectedLegacyWithoutDhAnon,
                CipherFactory.removeDhAnonFromCommaSeparatedList(legacyAlgorithms));

        legacyAlgorithms = null;
        assertNull(CipherFactory.removeDhAnonFromCommaSeparatedList(legacyAlgorithms));
    }

    /**
     * This test is meaningful when run in isolation. However, tests of server
     * sockets or ssl connections may modify the global state given by the
     * jdk.tls.legacyAlgorithms security property (for a good reason).
     * It is best to avoid running it in test suites, as it could itself lead
     * to a modification of the global state with hard-to-track consequences.
     */
    @SuppressWarnings("unused")
    private void testResetLegacyAlgorithms() {
        String legacyAlgorithmsBefore = CipherFactory.getLegacyAlgorithmsSilently();
        assertEquals("Failed assumption: jdk.tls.legacyAlgorithms" +
                " has been modified from its initial setting",
                CipherFactory.DEFAULT_LEGACY_ALGORITHMS, legacyAlgorithmsBefore);
        CipherFactory.removeAnonFromLegacyAlgorithms();
        CipherFactory.resetDefaultLegacyAlgorithms();
        String legacyAlgorithmsAfter = CipherFactory.getLegacyAlgorithmsSilently();
        assertEquals(CipherFactory.DEFAULT_LEGACY_ALGORITHMS, legacyAlgorithmsAfter);
    }


}
