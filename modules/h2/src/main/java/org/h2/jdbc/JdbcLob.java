/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.IOUtils;
import org.h2.util.Task;
import org.h2.value.Value;

/**
 * Represents a large object value.
 */
public abstract class JdbcLob extends TraceObject {

    final class LobPipedOutputStream extends PipedOutputStream {
        private final Task task;

        LobPipedOutputStream(PipedInputStream snk, Task task) throws IOException {
            super(snk);
            this.task = task;
        }

        @Override
        public void close() throws IOException {
            super.close();
            try {
                task.get();
            } catch (Exception e) {
                throw DbException.convertToIOException(e);
            }
        }
    }

    /**
     * State of the object.
     */
    public enum State {
        /**
         * New object without a value.
         */
        NEW,

        /**
         * One of setter methods is invoked, but stream is not closed yet.
         */
        SET_CALLED,

        /**
         * A value is set.
         */
        WITH_VALUE,

        /**
         * Object is closed.
         */
        CLOSED;
    }

    /**
     * JDBC connection.
     */
    final JdbcConnection conn;

    /**
     * Value.
     */
    Value value;

    /**
     * State.
     */
    State state;

    JdbcLob(JdbcConnection conn, Value value, State state, int type, int id) {
        setTrace(conn.getSession().getTrace(), type, id);
        this.conn = conn;
        this.value = value;
        this.state = state;
    }

    /**
     * Check that connection and LOB is not closed, otherwise throws exception with
     * error code {@link org.h2.api.ErrorCode#OBJECT_CLOSED}.
     */
    void checkClosed() {
        conn.checkClosed();
        if (state == State.CLOSED) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    /**
     * Check the state of the LOB and throws the exception when check failed
     * (set is supported only for a new LOB).
     */
    void checkEditable() {
        checkClosed();
        if (state != State.NEW) {
            throw DbException.getUnsupportedException("Allocate a new object to set its value.");
        }
    }

    /**
     * Check the state of the LOB and throws the exception when check failed
     * (the LOB must be set completely before read).
     */
    void checkReadable() throws SQLException, IOException {
        checkClosed();
        if (state == State.SET_CALLED) {
            throw DbException.getUnsupportedException("Stream setter is not yet closed.");
        }
    }

    /**
     * Change the state LOB state (LOB value is set completely and available to read).
     * @param blob LOB value.
     */
    void completeWrite(Value blob) {
        checkClosed();
        state = State.WITH_VALUE;
        value = blob;
    }

    /**
     * Release all resources of this object.
     */
    public void free() {
        debugCodeCall("free");
        state = State.CLOSED;
        value = null;
    }

    /**
     * Returns the input stream.
     *
     * @return the input stream
     */
    InputStream getBinaryStream() throws SQLException {
        try {
            debugCodeCall("getBinaryStream");
            checkReadable();
            return value.getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the reader.
     *
     * @return the reader
     */
    Reader getCharacterStream() throws SQLException {
        try {
            debugCodeCall("getCharacterStream");
            checkReadable();
            return value.getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the writer.
     *
     * @return Writer.
     * @throws IOException If an I/O error occurs.
     */
    Writer setCharacterStreamImpl() throws IOException {
        return IOUtils.getBufferedWriter(setClobOutputStreamImpl());
    }

    /**
     * Returns the writer stream.
     *
     * @return Output stream..
     * @throws IOException If an I/O error occurs.
     */
    LobPipedOutputStream setClobOutputStreamImpl() throws IOException {
        // PipedReader / PipedWriter are a lot slower
        // than PipedInputStream / PipedOutputStream
        // (Sun/Oracle Java 1.6.0_20)
        final PipedInputStream in = new PipedInputStream();
        final Task task = new Task() {
            @Override
            public void call() {
                completeWrite(conn.createClob(IOUtils.getReader(in), -1));
            }
        };
        LobPipedOutputStream out = new LobPipedOutputStream(in, task);
        task.execute();
        return out;
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append(getTraceObjectName()).append(": ");
        if (state == State.SET_CALLED) {
            builder.append("<setter_in_progress>");
        } else if (state == State.CLOSED) {
            builder.append("<closed>");
        } else {
            builder.append(value.getTraceSQL());
        }
        return builder.toString();
    }

}
