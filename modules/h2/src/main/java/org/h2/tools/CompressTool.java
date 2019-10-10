/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.h2.api.ErrorCode;
import org.h2.compress.CompressDeflate;
import org.h2.compress.CompressLZF;
import org.h2.compress.CompressNo;
import org.h2.compress.Compressor;
import org.h2.compress.LZFInputStream;
import org.h2.compress.LZFOutputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A tool to losslessly compress data, and expand the compressed data again.
 */
public class CompressTool {

    private static final int MAX_BUFFER_SIZE =
            3 * Constants.IO_BUFFER_SIZE_COMPRESS;
    private byte[] cachedBuffer;

    private CompressTool() {
        // don't allow construction
    }

    private byte[] getBuffer(int min) {
        if (min > MAX_BUFFER_SIZE) {
            return Utils.newBytes(min);
        }
        if (cachedBuffer == null || cachedBuffer.length < min) {
            cachedBuffer = Utils.newBytes(min);
        }
        return cachedBuffer;
    }

    /**
     * Get a new instance. Each instance uses a separate buffer, so multiple
     * instances can be used concurrently. However each instance alone is not
     * multithreading safe.
     *
     * @return a new instance
     */
    public static CompressTool getInstance() {
        return new CompressTool();
    }

    /**
     * Compressed the data using the specified algorithm. If no algorithm is
     * supplied, LZF is used
     *
     * @param in the byte array with the original data
     * @param algorithm the algorithm (LZF, DEFLATE)
     * @return the compressed data
     */
    public byte[] compress(byte[] in, String algorithm) {
        int len = in.length;
        if (in.length < 5) {
            algorithm = "NO";
        }
        Compressor compress = getCompressor(algorithm);
        byte[] buff = getBuffer((len < 100 ? len + 100 : len) * 2);
        int newLen = compress(in, in.length, compress, buff);
        return Utils.copyBytes(buff, newLen);
    }

    private static int compress(byte[] in, int len, Compressor compress,
            byte[] out) {
        int newLen = 0;
        out[0] = (byte) compress.getAlgorithm();
        int start = 1 + writeVariableInt(out, 1, len);
        newLen = compress.compress(in, len, out, start);
        if (newLen > len + start || newLen <= 0) {
            out[0] = Compressor.NO;
            System.arraycopy(in, 0, out, start, len);
            newLen = len + start;
        }
        return newLen;
    }

    /**
     * Expands the compressed  data.
     *
     * @param in the byte array with the compressed data
     * @return the uncompressed data
     */
    public byte[] expand(byte[] in) {
        int algorithm = in[0];
        Compressor compress = getCompressor(algorithm);
        try {
            int len = readVariableInt(in, 1);
            int start = 1 + getVariableIntLength(len);
            byte[] buff = Utils.newBytes(len);
            compress.expand(in, start, in.length - start, buff, 0, len);
            return buff;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.COMPRESSION_ERROR, e);
        }
    }

    /**
     * INTERNAL
     */
    public static void expand(byte[] in, byte[] out, int outPos) {
        int algorithm = in[0];
        Compressor compress = getCompressor(algorithm);
        try {
            int len = readVariableInt(in, 1);
            int start = 1 + getVariableIntLength(len);
            compress.expand(in, start, in.length - start, out, outPos, len);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.COMPRESSION_ERROR, e);
        }
    }

    /**
     * Read a variable size integer using Rice coding.
     *
     * @param buff the buffer
     * @param pos the position
     * @return the integer
     */
    public static int readVariableInt(byte[] buff, int pos) {
        int x = buff[pos++] & 0xff;
        if (x < 0x80) {
            return x;
        }
        if (x < 0xc0) {
            return ((x & 0x3f) << 8) + (buff[pos] & 0xff);
        }
        if (x < 0xe0) {
            return ((x & 0x1f) << 16) +
                    ((buff[pos++] & 0xff) << 8) +
                    (buff[pos] & 0xff);
        }
        if (x < 0xf0) {
            return ((x & 0xf) << 24) +
                    ((buff[pos++] & 0xff) << 16) +
                    ((buff[pos++] & 0xff) << 8) +
                    (buff[pos] & 0xff);
        }
        return Bits.readInt(buff, pos);
    }

    /**
     * Write a variable size integer using Rice coding.
     * Negative values need 5 bytes.
     *
     * @param buff the buffer
     * @param pos the position
     * @param x the value
     * @return the number of bytes written (0-5)
     */
    public static int writeVariableInt(byte[] buff, int pos, int x) {
        if (x < 0) {
            buff[pos++] = (byte) 0xf0;
            Bits.writeInt(buff, pos, x);
            return 5;
        } else if (x < 0x80) {
            buff[pos] = (byte) x;
            return 1;
        } else if (x < 0x4000) {
            buff[pos++] = (byte) (0x80 | (x >> 8));
            buff[pos] = (byte) x;
            return 2;
        } else if (x < 0x20_0000) {
            buff[pos++] = (byte) (0xc0 | (x >> 16));
            buff[pos++] = (byte) (x >> 8);
            buff[pos] = (byte) x;
            return 3;
        } else if (x < 0x1000_0000) {
            Bits.writeInt(buff, pos, x | 0xe000_0000);
            return 4;
        } else {
            buff[pos++] = (byte) 0xf0;
            Bits.writeInt(buff, pos, x);
            return 5;
        }
    }

    /**
     * Get a variable size integer length using Rice coding.
     * Negative values need 5 bytes.
     *
     * @param x the value
     * @return the number of bytes needed (0-5)
     */
    public static int getVariableIntLength(int x) {
        if (x < 0) {
            return 5;
        } else if (x < 0x80) {
            return 1;
        } else if (x < 0x4000) {
            return 2;
        } else if (x < 0x20_0000) {
            return 3;
        } else if (x < 0x1000_0000) {
            return 4;
        } else {
            return 5;
        }
    }

    private static Compressor getCompressor(String algorithm) {
        if (algorithm == null) {
            algorithm = "LZF";
        }
        int idx = algorithm.indexOf(' ');
        String options = null;
        if (idx > 0) {
            options = algorithm.substring(idx + 1);
            algorithm = algorithm.substring(0, idx);
        }
        int a = getCompressAlgorithm(algorithm);
        Compressor compress = getCompressor(a);
        compress.setOptions(options);
        return compress;
    }

    /**
     * INTERNAL
     */
    public static int getCompressAlgorithm(String algorithm) {
        algorithm = StringUtils.toUpperEnglish(algorithm);
        if ("NO".equals(algorithm)) {
            return Compressor.NO;
        } else if ("LZF".equals(algorithm)) {
            return Compressor.LZF;
        } else if ("DEFLATE".equals(algorithm)) {
            return Compressor.DEFLATE;
        } else {
            throw DbException.get(
                    ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1,
                    algorithm);
        }
    }

    private static Compressor getCompressor(int algorithm) {
        switch (algorithm) {
        case Compressor.NO:
            return new CompressNo();
        case Compressor.LZF:
            return new CompressLZF();
        case Compressor.DEFLATE:
            return new CompressDeflate();
        default:
            throw DbException.get(
                    ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1,
                    "" + algorithm);
        }
    }

    /**
     * INTERNAL
     */
    public static OutputStream wrapOutputStream(OutputStream out,
            String compressionAlgorithm, String entryName) {
        try {
            if ("GZIP".equals(compressionAlgorithm)) {
                out = new GZIPOutputStream(out);
            } else if ("ZIP".equals(compressionAlgorithm)) {
                ZipOutputStream z = new ZipOutputStream(out);
                z.putNextEntry(new ZipEntry(entryName));
                out = z;
            } else if ("DEFLATE".equals(compressionAlgorithm)) {
                out = new DeflaterOutputStream(out);
            } else if ("LZF".equals(compressionAlgorithm)) {
                out = new LZFOutputStream(out);
            } else if (compressionAlgorithm != null) {
                throw DbException.get(
                        ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1,
                        compressionAlgorithm);
            }
            return out;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * INTERNAL
     */
    public static InputStream wrapInputStream(InputStream in,
            String compressionAlgorithm, String entryName) {
        try {
            if ("GZIP".equals(compressionAlgorithm)) {
                in = new GZIPInputStream(in);
            } else if ("ZIP".equals(compressionAlgorithm)) {
                ZipInputStream z = new ZipInputStream(in);
                while (true) {
                    ZipEntry entry = z.getNextEntry();
                    if (entry == null) {
                        return null;
                    }
                    if (entryName.equals(entry.getName())) {
                        break;
                    }
                }
                in = z;
            } else if ("DEFLATE".equals(compressionAlgorithm)) {
                in = new InflaterInputStream(in);
            } else if ("LZF".equals(compressionAlgorithm)) {
                in = new LZFInputStream(in);
            } else if (compressionAlgorithm != null) {
                throw DbException.get(
                        ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1,
                        compressionAlgorithm);
            }
            return in;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

}

