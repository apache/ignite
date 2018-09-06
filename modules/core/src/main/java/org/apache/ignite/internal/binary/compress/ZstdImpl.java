package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteSystemProperties;

public class ZstdImpl {

    private static final boolean ENABLED = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_ENABLE_COMPRESSION);
    private static final int DICTIONARY_SAMPLES = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_COMPRESSION_SAMPLES, 16384);
    private static final int DICTIONARY_BUFFER = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_COMPRESSION_BUFFER, 4 * 1024 * 1024);
    private static final int DICTIONARY_SIZE = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_COMPRESSION_DICTIONARY_LENGTH, 1024);
    private static final int COMPRESSION_LEVEL = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_COMPRESSION_LEVEL, 2);
    private static final boolean CHECK = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_COMPRESSION_CHECK);

    public static final int MIN_DELTA_BYTES = 8;

    private final AtomicInteger dictSize = new AtomicInteger();
    private final List<byte[]> dictionary = new ArrayList<>();
    private final AtomicInteger bufferCollected = new AtomicInteger();

    private volatile ZstdDictCompress compressor;
    private volatile ZstdDictDecompress decompressor;
    private final AtomicLong unc = new AtomicLong();
    private final AtomicLong comp = new AtomicLong();
    private final AtomicLong acc = new AtomicLong();
    private final AtomicLong rej = new AtomicLong();
    private final Lock dictLock = new ReentrantLock();

    /** Compress a string to a list of output symbols. */
    public void dictionarize(byte[] uncompressed) {
        if (uncompressed.length == 0 || !dictLock.tryLock()
            || bufferCollected.addAndGet(uncompressed.length) > DICTIONARY_BUFFER)
            return;

        dictionary.add(Arrays.copyOf(uncompressed, uncompressed.length));

        dictLock.unlock();
    }

    public byte[] handle(byte[] message) {
        ZstdDictCompress cmpr = this.compressor;

        if (cmpr != null) {
            if (!ENABLED)
                return null;

            byte[] o = compress(message, cmpr, false);

            if (CHECK) {
                byte[] decompressed = decompress(o);
                if (ByteArrayComparator.INSTANCE.compare(message, decompressed) != 0)
                    throw new IllegalStateException("Recompressed did not match: "
                        + Arrays.toString(message) + " vs " + Arrays.toString(decompress(o)));
            }

            //System.err.println("Before: " + message.length + ", after: " + o.length);

            boolean accept = (message.length - o.length) > MIN_DELTA_BYTES;

            unc.addAndGet(message.length);
            comp.addAndGet(accept ? o.length : message.length);

            if (dictSize.incrementAndGet() % DICTIONARY_SAMPLES == DICTIONARY_SAMPLES - 1)
                System.out.println("Ratio: " + (float)comp.get() / (float)unc.get() +
                    ", acceptance: " + (acc.get() * 100L) / (rej.get() + acc.get()) + "%");

            if (accept) {
                acc.incrementAndGet();
                return o;
            } else {
                rej.incrementAndGet();
                return null;
            }
        }

        int iv = dictSize.incrementAndGet();
        if (iv < DICTIONARY_SAMPLES) {
            dictionarize(message);
        }
        else if (iv == DICTIONARY_SAMPLES) {
            dictLock.lock();
            int totalLen = 0;
            for (byte[] sample : dictionary)
                totalLen += sample.length;

            ZstdDictTrainer trainer = new ZstdDictTrainer(totalLen, DICTIONARY_SIZE);

            for (byte[] sample : dictionary)
                trainer.addSample(sample);

            byte[] dictionary = trainer.trainSamples();

            this.decompressor = new ZstdDictDecompress(dictionary);
            this.compressor = new ZstdDictCompress(dictionary, COMPRESSION_LEVEL);

            dictLock.unlock();
        }

        return null;
    }

    private static byte[] compress(byte[] s, ZstdDictCompress trie, boolean b) {
        return Zstd.compress(s, trie);
    }

    public byte[] decompress(byte[] bytes) {
        return Zstd.decompress(bytes, decompressor, /* XXX */bytes.length * 128);
    }

}
