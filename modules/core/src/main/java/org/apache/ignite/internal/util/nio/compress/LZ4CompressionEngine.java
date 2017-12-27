package org.apache.ignite.internal.util.nio.compress;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.OK;

public class LZ4CompressionEngine implements CompressionEngine {
    /* For debug stats. */
    private long bytesBefore = 0;
    private long bytesAfter = 0;

    private static boolean compressSmall = false;
    private static byte smallMsgSize = 127;
    static {
        assert smallMsgSize > 0;
    }

    private final ExtendedByteArrayOutputStream deflateBaos = new ExtendedByteArrayOutputStream(1024);
    private byte[] inputWrapArray = new byte[1024];

    private final ExtendedByteArrayOutputStream inflateBaos = new ExtendedByteArrayOutputStream(1024);
    private final byte[] inflateArray = new byte[1024];
    private byte[] inputUnwapArray = new byte[1024];
    private final byte[] lenBytes = new byte[4];

    /** */
    public CompressionEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        int len = src.remaining();

        bytesBefore += len;

        if (compressSmall && len < smallMsgSize) {
            bytesAfter += len + 1;

            buf.put((byte)len);
            buf.put(src);

            return OK;
        }

        while (inputWrapArray.length < len)
            inputWrapArray = new byte[inputWrapArray.length * 2];

        src.get(inputWrapArray, 0 , len);

        deflateBaos.reset();

        try (LZ4BlockOutputStream out = new LZ4BlockOutputStream(deflateBaos) ) {
            out.write(inputWrapArray, 0, len);
        }

        if (deflateBaos.size() + 4 > buf.remaining()) {
            src.rewind();

            return BUFFER_OVERFLOW;
        }

        if (compressSmall)
            buf.put((byte)-1);
        buf.put(toArray(deflateBaos.size()));
        buf.put(deflateBaos.getByteArray(), 0, deflateBaos.size());

        bytesAfter += deflateBaos.size();

        return OK;
    }

    /** */
    public void closeInbound() throws IOException{
        //No-op
        System.out.println("MY LZ4 bytesBefore:"+bytesBefore+" bytesAfter;"+bytesAfter+ " cr="+bytesBefore*1.0/bytesAfter);
    }

    /** */
    public CompressionEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        int initPos = src.position();

        if (compressSmall && src.remaining() == 0) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        byte flag = compressSmall ? src.get() : -1;
        assert flag >= 0 || flag == -1;

        if (compressSmall && flag != -1){
            if (src.remaining() < flag) {
                src.position(initPos);

                return BUFFER_UNDERFLOW;
            }

            for (int i = 0; i < flag; i++)
                buf.put(src.get());

            return (src.remaining() == 0) ? BUFFER_UNDERFLOW : OK;
        }

        if (src.remaining() < 5) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        src.get(lenBytes);
        int len = toInt(lenBytes);

        if (src.remaining() < len) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        while (inputUnwapArray.length < src.remaining())
            inputUnwapArray = new byte[inputUnwapArray.length * 2];

        src.get(inputUnwapArray, 0, len);

        inflateBaos.reset();

        try (InputStream in = new LZ4BlockInputStream(new ByteArrayInputStream(inputUnwapArray, 0, len))
        ) {
            int length;

            while ((length = in.read(inflateArray)) != -1)
                inflateBaos.write(inflateArray, 0, length);

            inflateBaos.flush();

            if (inflateBaos.size() > buf.remaining()) {
                src.position(initPos);

                return BUFFER_OVERFLOW;
            }

            buf.put(inflateBaos.getByteArray(), 0, inflateBaos.size());
        }

        if (src.remaining() == 0)
            return BUFFER_UNDERFLOW;

        return OK;
    }

    /** */
    private int toInt(byte[] bytes){
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
            | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    /** */
    private byte[] toArray(int val){
        return  new byte[] {
            (byte)(val >>> 24),
            (byte)(val >>> 16),
            (byte)(val >>> 8),
            (byte)val
        };
    }

}
