package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * Interface that allows to implement custom serialization
 * logic to raw binary streams.
 */
public interface JdbcRawBinarylizable {
    /**
     * Writes fields to provided writer.
     *
     * @param writer Binary object writer.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException;

    /**
     * Reads fields from provided reader.
     *
     * @param reader Binary object reader.
     * @throws BinaryObjectException In case of error.
     */
    public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException;
}