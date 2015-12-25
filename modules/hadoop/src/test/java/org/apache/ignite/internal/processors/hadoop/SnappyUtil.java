package org.apache.ignite.internal.processors.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to check Snappy compression.
 */
public class SnappyUtil {
    /** */
    static final int BYTE_SIZE = 1024 * 50;

    /**
     *
     */
    public static void printDiagnosticAndTestSnappy(boolean doCheck, @Nullable Class<?> clazz,
            @Nullable Configuration conf) {
        if (doCheck) {
            snappyDiagnostics(clazz);

            try {
                testSnappyCodec(conf);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     *
     *
     * @param clazz
     */
    public static void snappyDiagnostics(Class<?> clazz) {
        System.out.println("### snappy supported:     " + NativeCodeLoader.buildSupportsSnappy());
        System.out.println("### native loaded:        " + NativeCodeLoader.isNativeCodeLoaded());
        System.out.println("### lib name:             " + NativeCodeLoader.getLibraryName());
        System.out.println("### snappy native loaded: " + SnappyCompressor.isNativeCodeLoaded());
        System.out.println("### snappy library:       " + SnappyCompressor.getLibraryName());

        if (clazz != null)
            System.out.println("### classloader:          " + clazz.getClassLoader());

        System.out.println("### call stack:");
        new Throwable().printStackTrace(System.out);

        SnappyCodec.checkNativeCodeLoaded();
    }

    /**
     *
     * @param conf
     * @throws Exception
     */
    public static void testSnappyCodec(@Nullable Configuration conf) throws Exception {
        final SnappyCodec codec = new SnappyCodec();

        if (conf == null)
            conf = new Configuration();

        codec.setConf(conf);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] bytes = BytesGenerator.get(BYTE_SIZE);

        byte[] bytes2 = new byte[bytes.length];

        try (CompressionOutputStream cos = codec.createOutputStream(baos)) {
            cos.write(bytes);
            cos.flush();
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        int read;

        try (CompressionInputStream cis = codec.createInputStream(bais)) {
            read = cis.read(bytes2, 0, bytes2.length);
        }

        A.ensure(read == bytes2.length, "Number of bytes.");
        A.ensure(Arrays.equals(bytes, bytes2), "Data contants.");
    }

    /**
     * Class copied from Hadoop.
     */
    static final class BytesGenerator {
        /**
         * Constructor block.
         */
        private BytesGenerator() {
        }

        private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
            0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };

        private static final Random rnd = new Random(12345l);

        public static byte[] get(int size) {
            byte[] array = (byte[]) Array.newInstance(byte.class, size);
            for (int i = 0; i < size; i++)
                array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
            return array;
        }
    }
}