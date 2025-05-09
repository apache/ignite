package org.apache.ignite.internal.processors.rest.igfs.util;


import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Base64.Decoder;

public class EncryptUtil {
	private final static String DES = "DES";
    private final static String ENCODE = "UTF-8";
    private final static String defaultKey = "LocalS3X";

    
    public static String encryptByMD5(String str) {
        //生成md5加密算法       
        return encryptByMD5(str.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public static String encryptByMD5(byte[] bytea){
        //生成md5加密算法
        MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
			md5.update(bytea);
	        byte b[] = md5.digest();
	        int i;
	        StringBuffer buf = new StringBuffer("");
	        for (int j = 0; j < b.length; j++) {
	            i = b[j];
	            if (i < 0)
	                i += 256;
	            if (i < 16)
	                buf.append("0");
	            buf.append(Integer.toHexString(i));
	        }
	        String md5_32 = buf.toString();
	        return md5_32;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}        
    }

    public static String encryptByDES(String data) throws Exception {
        byte[] bt = encrypt(data.getBytes(ENCODE), defaultKey.getBytes(ENCODE));
        String strs = Base64.getEncoder().encodeToString(bt);
        return strs;
    }
    
    public static String encryptByDES(byte[] data) throws Exception {
        byte[] bt = encrypt(data, defaultKey.getBytes(ENCODE));
        String strs = Base64.getEncoder().encodeToString(bt);
        return strs;
    }

    public static String decryptByDES(String data) throws IOException, Exception {
        if (data == null)
            return null;
        Decoder decoder = Base64.getDecoder();
        byte[] buf = decoder.decode(data);
        byte[] bt = decrypt(buf, defaultKey.getBytes(ENCODE));
        return new String(bt, ENCODE);
    }

    private static byte[] encrypt(byte[] data, byte[] key) throws Exception {
        SecureRandom sr = new SecureRandom();
        DESKeySpec dks = new DESKeySpec(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
        SecretKey securekey = keyFactory.generateSecret(dks);
        Cipher cipher = Cipher.getInstance(DES);
        cipher.init(Cipher.ENCRYPT_MODE, securekey, sr);
        return cipher.doFinal(data);
    }

    private static byte[] decrypt(byte[] data, byte[] key) throws Exception {
        SecureRandom sr = new SecureRandom();
        DESKeySpec dks = new DESKeySpec(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
        SecretKey securekey = keyFactory.generateSecret(dks);
        Cipher cipher = Cipher.getInstance(DES);
        cipher.init(Cipher.DECRYPT_MODE, securekey, sr);
        return cipher.doFinal(data);
    }
}
