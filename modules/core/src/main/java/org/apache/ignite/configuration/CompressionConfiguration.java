package org.apache.ignite.configuration;

import java.io.Serializable;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.Compressor;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;

public class CompressionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Compression enabled. */
    private boolean enabled = true;

    /** Use dictionary. */
    private boolean useDict = true;

    /** Number of samples to train dictionary. */
    private int dictTrainSamples = 16384;

    /** Length of buffer to store training samples. */
    private long dictTrainBufLen = 4L * 1024 * 1024;

    /** Size of dictionary. */
    private int dictSz = 1024;

    /** Compression level. */
    private int compressionLevel = 2;

    /** Compress keys. */
    private boolean compressKeys = false;

    /** Compressor factory. */
    private Factory<? extends Compressor> compressorFactory = new CompressorFactory();

    /**
     * Gets compression enabled flag.
     * By default {@code true} (use {@code null} CompressionConfiguration as disabled default).
     *
     * @return compression enabled flag.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets compression enabled flag.
     *
     * @param enabled Compression enabled flag.
     */
    public CompressionConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * Gets use dictionary flag.
     *
     * @return use dictionary flag.
     */
    public boolean isUseDictionary() {
        return useDict;
    }

    /**
     * Sets use dictionary flag.
     *
     * If {@code true}, dictionary is trained basing on sample objects from cache. Enabled by default.
     * Disabling decreases space savings twofold typically.
     *
     * @param useDict use dictionary flag.
     */
    public CompressionConfiguration setUseDictionary(boolean useDict) {
        this.useDict = useDict;

        return this;
    }

    /**
     * Gets number of cache objects collected to train dictionary.
     *
     * @return number of samples.
     */
    public int getDictionaryTrainSamples() {
        return dictTrainSamples;
    }

    /**
     * Sets number of cache objects collected to train dictionary.
     * It is recommended to keep default value or do extensive benchmarking after changing this setting.
     *
     * Total samples length is limited by {@link #setDictionaryTrainBufferLength(long)}.
     *
     * @param dictTrainSamples number of samples.
     */
    public CompressionConfiguration setDictionaryTrainSamples(int dictTrainSamples) {
        this.dictTrainSamples = dictTrainSamples;

        return this;
    }

    /**
     * Gets maximum length of buffer, in bytes, used to store training samples.
     *
     * @return length of buffer.
     */
    public long getDictionaryTrainBufferLength() {
        return dictTrainBufLen;
    }

    /**
     * Sets length of buffer, in bytes, used to store training samples.
     * It is recommended to keep default value or do extensive benchmarking after changing this setting.
     *
     * Note that {@code zstd-jni} library might have problems with larger buffers, causing JVM crash!
     *
     * @param dictTrainBufLen length of buffer.
     */
    public CompressionConfiguration setDictionaryTrainBufferLength(long dictTrainBufLen) {
        this.dictTrainBufLen = dictTrainBufLen;

        return this;
    }

    /**
     * Gets size of dictionary, in bytes.
     *
     * @return dictionary size.
     */
    public int getDictionarySize() {
        return dictSz;
    }

    /**
     * Sets size of dictionary, in bytes.
     *
     * Generally, using larger dictionary slightly improves space savings and massively decreases compression speed.
     * It is recommended to keep default value or do extensive benchmarking after changing this setting.
     *
     * @param dictSz dictionary size.
     */
    public CompressionConfiguration setDictionarySize(int dictSz) {
        this.dictSz = dictSz;

        return this;
    }

    /**
     * Gets compression level, as interpreted by {@link Compressor} implementation.
     *
     * @return compression level.
     */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    /**
     * Sets compression level, as interpreted by {@link Compressor} implementation.
     *
     * It is recommended to keep default value or do extensive benchmarking after changing this setting.
     *
     * Note that default is chosen for {@code zstd-jni} library.
     *
     * @param compressionLevel compression level.
     */
    public CompressionConfiguration setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;

        return this;
    }

    /**
     * Gets compress cache keys flag. By default is {@code false} so only cache values are compressed.
     *
     * This is because keys are smaller, less repetitive and marshalled-demarshalled often
     * during some of {@link BPlusTree} operations.
     *
     * @return compress keys flag.
     */
    public boolean isCompressKeys() {
        return compressKeys;
    }

    /**
     * Sets compress cache keys flag.
     *
     * @param compressKeys compress keys flag.
     */
    public CompressionConfiguration setCompressKeys(boolean compressKeys) {
        this.compressKeys = compressKeys;

        return this;
    }

    /**
     * Gets compression-decompression implementation to use.
     *
     * By default, {@code ignite-compress} is used which employs {@code zstd-jni} library.
     *
     * @return compressor factory.
     */
    public Factory<? extends Compressor> getCompressorFactory() {
        return compressorFactory;
    }

    /**
     * Sets compression-decompression implementation to use.
     *
     * Note that custom {@link Compressor} implementation may use a subset of configuration settings
     * and-or use their own settings provided by {@link #compressorFactory}.
     *
     * @param compressorFactory compressor factory.
     */
    public CompressionConfiguration setCompressorFactory(Factory<? extends Compressor> compressorFactory) {
        this.compressorFactory = compressorFactory;

        return this;
    }

    /** */
    private class CompressorFactory implements Factory<Compressor>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @Override public Compressor create() {
            Compressor res;

            try {
                Class<? extends Compressor> cls = (Class<? extends Compressor>)Class.forName(
                    isUseDictionary() ?
                    "org.apache.ignite.internal.binary.compress.ZstdDictionaryCompressor" :
                    "org.apache.ignite.internal.binary.compress.ZstdStatelessCompressor");

                res = cls.newInstance();
            }
            catch (Throwable t) {
                throw new IgniteException("Failed to instantiate Compressor " +
                    "(please ensure that \"ignite-compress\" module is added to classpath).", t);
            }

            return res;
        }
    }
}
