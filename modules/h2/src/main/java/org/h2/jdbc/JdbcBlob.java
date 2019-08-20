/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.IOUtils;
import org.h2.util.Task;
import org.h2.value.Value;

/**
 * Represents a BLOB value.
 */
public class JdbcBlob extends TraceObject implements Blob {

    Value value;
    private final JdbcConnection conn;

    /**
     * INTERNAL
     */
    public JdbcBlob(JdbcConnection conn, Value value, int id) {
        setTrace(conn.getSession().getTrace(), TraceObject.BLOB, id);
        this.conn = conn;
        this.value = value;
    }

    /**
     * Returns the length.
     *
     * @return the length
     */
    @Override
    public long length() throws SQLException {
        try {
            debugCodeCall("length");
            checkClosed();
            if (value.getType() == Value.BLOB) {
                long precision = value.getPrecision();
                if (precision > 0) {
                    return precision;
                }
            }
            return IOUtils.copyAndCloseInput(value.getInputStream(), null);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Truncates the object.
     *
     * @param len the new length
     */
    @Override
    public void truncate(long len) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns some bytes of the object.
     *
     * @param pos the index, the first byte is at position 1
     * @param length the number of bytes
     * @return the bytes, at most length bytes
     */
    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBytes("+pos+", "+length+");");
            }
            checkClosed();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (InputStream in = value.getInputStream()) {
                IOUtils.skipFully(in, pos - 1);
                IOUtils.copy(in, out, length);
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Fills the Blob. This is only supported for new, empty Blob objects that
     * were created with Connection.createBlob(). The position
     * must be 1, meaning the whole Blob data is set.
     *
     * @param pos where to start writing (the first byte is at position 1)
     * @param bytes the bytes to set
     * @return the length of the added data
     */
    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBytes("+pos+", "+quoteBytes(bytes)+");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            value = conn.createBlob(new ByteArrayInputStream(bytes), -1);
            return bytes.length;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets some bytes of the object.
     *
     * @param pos the write position
     * @param bytes the bytes to set
     * @param offset the bytes offset
     * @param len the number of bytes to write
     * @return how many bytes have been written
     */
    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBytes(" + pos + ", " + quoteBytes(bytes) + ", " + offset + ", " + len + ");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            value = conn.createBlob(new ByteArrayInputStream(bytes, offset, len), -1);
            return (int) value.getPrecision();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the input stream.
     *
     * @return the input stream
     */
    @Override
    public InputStream getBinaryStream() throws SQLException {
        try {
            debugCodeCall("getBinaryStream");
            checkClosed();
            return value.getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get a writer to update the Blob. This is only supported for new, empty
     * Blob objects that were created with Connection.createBlob(). The Blob is
     * created in a separate thread, and the object is only updated when
     * OutputStream.close() is called. The position must be 1, meaning the whole
     * Blob data is set.
     *
     * @param pos where to start writing (the first byte is at position 1)
     * @return an output stream
     */
    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBinaryStream("+pos+");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            if (value.getPrecision() != 0) {
                throw DbException.getInvalidValueException("length", value.getPrecision());
            }
            final JdbcConnection c = conn; // local variable avoids generating synthetic accessor method
            final PipedInputStream in = new PipedInputStream();
            final Task task = new Task() {
                @Override
                public void call() {
                    value = c.createBlob(in, -1);
                }
            };
            PipedOutputStream out = new PipedOutputStream(in) {
                @Override
                public void close() throws IOException {
                    super.close();
                    try {
                        task.get();
                    } catch (Exception e) {
                        throw DbException.convertToIOException(e);
                    }
                }
            };
            task.execute();
            return new BufferedOutputStream(out);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     *
     * @param pattern the pattern to search
     * @param start the index, the first byte is at position 1
     * @return the position (first byte is at position 1), or -1 for not found
     */
    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("position("+quoteBytes(pattern)+", "+start+");");
        }
        if (Constants.BLOB_SEARCH) {
            try {
                checkClosed();
                if (pattern == null) {
                    return -1;
                }
                if (pattern.length == 0) {
                    return 1;
                }
                // TODO performance: blob pattern search is slow
                BufferedInputStream in = new BufferedInputStream(value.getInputStream());
                IOUtils.skipFully(in, start - 1);
                int pos = 0;
                int patternPos = 0;
                while (true) {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    if (x == (pattern[patternPos] & 0xff)) {
                        if (patternPos == 0) {
                            in.mark(pattern.length);
                        }
                        if (patternPos == pattern.length) {
                            return pos - patternPos;
                        }
                        patternPos++;
                    } else {
                        if (patternPos > 0) {
                            in.reset();
                            pos -= patternPos;
                        }
                    }
                    pos++;
                }
                return -1;
            } catch (Exception e) {
                throw logAndConvert(e);
            }
        }
        throw unsupported("LOB search");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     *
     * @param blobPattern the pattern to search
     * @param start the index, the first byte is at position 1
     * @return the position (first byte is at position 1), or -1 for not found
     */
    @Override
    public long position(Blob blobPattern, long start) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("position(blobPattern, "+start+");");
        }
        if (Constants.BLOB_SEARCH) {
            try {
                checkClosed();
                if (blobPattern == null) {
                    return -1;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                InputStream in = blobPattern.getBinaryStream();
                while (true) {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    out.write(x);
                }
                return position(out.toByteArray(), start);
            } catch (Exception e) {
                throw logAndConvert(e);
            }
        }
        throw unsupported("LOB subset");
    }

    /**
     * Release all resources of this object.
     */
    @Override
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    /**
     * Returns the input stream, starting from an offset.
     *
     * @param pos where to start reading
     * @param length the number of bytes that will be read
     * @return the input stream to read
     */
    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        try {
            debugCodeCall("getBinaryStream(pos, length)");
            checkClosed();
            return value.getInputStream(pos, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void checkClosed() {
        conn.checkClosed();
        if (value == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " +
                (value == null ? "null" : value.getTraceSQL());
    }

}
