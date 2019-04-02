/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.store.RangeReader;
import org.h2.util.IOUtils;
import org.h2.value.Value;

/**
 * Represents a CLOB value.
 */
public class JdbcClob extends JdbcLob implements NClob {

    /**
     * INTERNAL
     */
    public JdbcClob(JdbcConnection conn, Value value, State state, int id) {
        super(conn, value, state, TraceObject.CLOB, id);
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
            checkReadable();
            if (value.getValueType() == Value.CLOB) {
                long precision = value.getType().getPrecision();
                if (precision > 0) {
                    return precision;
                }
            }
            return IOUtils.copyAndCloseInput(value.getReader(), null, Long.MAX_VALUE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Truncates the object.
     */
    @Override
    public void truncate(long len) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns the input stream.
     *
     * @return the input stream
     */
    @Override
    public InputStream getAsciiStream() throws SQLException {
        try {
            debugCodeCall("getAsciiStream");
            checkReadable();
            String s = value.getString();
            return IOUtils.getInputStreamFromString(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns an output  stream.
     */
    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return super.getCharacterStream();
    }

    /**
     * Get a writer to update the Clob. This is only supported for new, empty
     * Clob objects that were created with Connection.createClob() or
     * createNClob(). The Clob is created in a separate thread, and the object
     * is only updated when Writer.close() is called. The position must be 1,
     * meaning the whole Clob data is set.
     *
     * @param pos where to start writing (the first character is at position 1)
     * @return a writer
     */
    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setCharacterStream(" + pos + ");");
            }
            checkEditable();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            state = State.SET_CALLED;
            return setCharacterStreamImpl();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a substring.
     *
     * @param pos the position (the first character is at position 1)
     * @param length the number of characters
     * @return the string
     */
    @Override
    public String getSubString(long pos, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getSubString(" + pos + ", " + length + ");");
            }
            checkReadable();
            if (pos < 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            if (length < 0) {
                throw DbException.getInvalidValueException("length", length);
            }
            StringWriter writer = new StringWriter(
                    Math.min(Constants.IO_BUFFER_SIZE, length));
            try (Reader reader = value.getReader()) {
                IOUtils.skipFully(reader, pos - 1);
                IOUtils.copyAndCloseInput(reader, writer, length);
            }
            return writer.toString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Fills the Clob. This is only supported for new, empty Clob objects that
     * were created with Connection.createClob() or createNClob(). The position
     * must be 1, meaning the whole Clob data is set.
     *
     * @param pos where to start writing (the first character is at position 1)
     * @param str the string to add
     * @return the length of the added text
     */
    @Override
    public int setString(long pos, String str) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setString(" + pos + ", " + quote(str) + ");");
            }
            checkEditable();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            } else if (str == null) {
                throw DbException.getInvalidValueException("str", str);
            }
            completeWrite(conn.createClob(new StringReader(str), -1));
            return str.length();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Fills the Clob. This is only supported for new, empty Clob objects that
     * were created with Connection.createClob() or createNClob(). The position
     * must be 1, meaning the whole Clob data is set.
     *
     * @param pos where to start writing (the first character is at position 1)
     * @param str the string to add
     * @param offset the string offset
     * @param len the number of characters to read
     * @return the length of the added text
     */
    @Override
    public int setString(long pos, String str, int offset, int len)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setString(" + pos + ", " + quote(str) + ", " + offset + ", " + len + ");");
            }
            checkEditable();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            } else if (str == null) {
                throw DbException.getInvalidValueException("str", str);
            }
            completeWrite(conn.createClob(new RangeReader(new StringReader(str), offset, len), -1));
            return (int) value.getType().getPrecision();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     */
    @Override
    public long position(String pattern, long start) throws SQLException {
        throw unsupported("LOB search");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     */
    @Override
    public long position(Clob clobPattern, long start) throws SQLException {
        throw unsupported("LOB search");
    }

    /**
     * Returns the reader, starting from an offset.
     *
     * @param pos 1-based offset
     * @param length length of requested area
     * @return the reader
     */
    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getCharacterStream(" + pos + ", " + length + ");");
            }
            checkReadable();
            if (state == State.NEW) {
                if (pos != 1) {
                    throw DbException.getInvalidValueException("pos", pos);
                }
                if (length != 0) {
                    throw DbException.getInvalidValueException("length", pos);
                }
            }
            return value.getReader(pos, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

}
