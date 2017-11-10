package org.apache.ignite.internal.util.nio.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.nio.compress.GZipCompressor.afterCompress;
import static org.apache.ignite.internal.util.nio.compress.GZipCompressor.beforeCompress;

public class DeflatorCompressor implements NioCompressor {
    @Override public ByteBuffer compress(@NotNull ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[buf.rewind().remaining()];

        buf.get(bytes);

        byte b = beforeCompress(bytes.length);

        Deflater compressor = new Deflater();

        compressor.setLevel(Deflater.BEST_SPEED);
        compressor.setInput(bytes);
        compressor.finish();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] buf0 = new byte[32];

        while (!compressor.finished()) {
            int count = compressor.deflate(buf0);

            baos.write(buf0, 0, count);
        }

        compressor.end();
        baos.close();

        bytes = baos.toByteArray();

        afterCompress(b, bytes.length);

        if (bytes.length > buf.capacity()) {
            ByteOrder order = buf.order();

            if (buf.isDirect())
                buf = ByteBuffer.allocateDirect(bytes.length);
            else
                buf = ByteBuffer.allocate(bytes.length);

            buf.order(order);
        }
        else
            buf.clear();

        buf.put(bytes);

        buf.flip();

        return buf;
    }

    @Override public ByteBuffer decompress(@NotNull ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[buf.rewind().remaining()];

        buf.get(bytes);

        Inflater inflater = new Inflater();
        inflater.setInput(bytes);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);

        byte[] buf0 = new byte[32];

        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buf0);

                baos.write(buf0, 0, count);

            } catch (DataFormatException e) {
                throw new IOException("DataFormatException: ", e);
            }
        }

        inflater.end();

        baos.close();

        bytes = baos.toByteArray();

        if (bytes.length > buf.capacity()) {
            ByteOrder order = buf.order();

            if (buf.isDirect())
                buf = ByteBuffer.allocateDirect(bytes.length);
            else
                buf = ByteBuffer.allocate(bytes.length);

            buf.order(order);
        }
        else
            buf.clear();

        buf.put(bytes);

        buf.flip();

        return buf;
    }
}
