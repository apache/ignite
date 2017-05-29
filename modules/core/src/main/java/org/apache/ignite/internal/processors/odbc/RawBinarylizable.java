package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * Interface that allows to implement custom serialization
 * logic to raw binary streams.
 */
public interface RawBinarylizable {
    /**
     * Writes fields to provided writer.
     *
     * @param writer Binary object writer.
     * @param objWriter Object writer.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException;

    /**
     * Reads fields from provided reader.
     *
     * @param reader Binary object reader.
     * @param objReader Object reqder.
     * @throws BinaryObjectException In case of error.
     */
    public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException;
}