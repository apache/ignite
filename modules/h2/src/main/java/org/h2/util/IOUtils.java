/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;

/**
 * This utility class contains input/output functions.
 */
public class IOUtils {

    private IOUtils() {
        // utility class
    }

    /**
     * Close a Closeable without throwing an exception.
     *
     * @param out the Closeable or null
     */
    public static void closeSilently(Closeable out) {
        if (out != null) {
            try {
                trace("closeSilently", null, out);
                out.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Close an AutoCloseable without throwing an exception.
     *
     * @param out the AutoCloseable or null
     */
    public static void closeSilently(AutoCloseable out) {
        if (out != null) {
            try {
                trace("closeSilently", null, out);
                out.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Skip a number of bytes in an input stream.
     *
     * @param in the input stream
     * @param skip the number of bytes to skip
     * @throws EOFException if the end of file has been reached before all bytes
     *             could be skipped
     * @throws IOException if an IO exception occurred while skipping
     */
    public static void skipFully(InputStream in, long skip) throws IOException {
        try {
            while (skip > 0) {
                long skipped = in.skip(skip);
                if (skipped <= 0) {
                    throw new EOFException();
                }
                skip -= skipped;
            }
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }

    /**
     * Skip a number of characters in a reader.
     *
     * @param reader the reader
     * @param skip the number of characters to skip
     * @throws EOFException if the end of file has been reached before all
     *             characters could be skipped
     * @throws IOException if an IO exception occurred while skipping
     */
    public static void skipFully(Reader reader, long skip) throws IOException {
        try {
            while (skip > 0) {
                long skipped = reader.skip(skip);
                if (skipped <= 0) {
                    throw new EOFException();
                }
                skip -= skipped;
            }
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }

    /**
     * Copy all data from the input stream to the output stream and close both
     * streams. Exceptions while closing are ignored.
     *
     * @param in the input stream
     * @param out the output stream
     * @return the number of bytes copied
     */
    public static long copyAndClose(InputStream in, OutputStream out)
            throws IOException {
        try {
            long len = copyAndCloseInput(in, out);
            out.close();
            return len;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        } finally {
            closeSilently(out);
        }
    }

    /**
     * Copy all data from the input stream to the output stream and close the
     * input stream. Exceptions while closing are ignored.
     *
     * @param in the input stream
     * @param out the output stream (null if writing is not required)
     * @return the number of bytes copied
     */
    public static long copyAndCloseInput(InputStream in, OutputStream out)
            throws IOException {
        try {
            return copy(in, out);
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        } finally {
            closeSilently(in);
        }
    }

    /**
     * Copy all data from the input stream to the output stream. Both streams
     * are kept open.
     *
     * @param in the input stream
     * @param out the output stream (null if writing is not required)
     * @return the number of bytes copied
     */
    public static long copy(InputStream in, OutputStream out)
            throws IOException {
        return copy(in, out, Long.MAX_VALUE);
    }

    /**
     * Copy all data from the input stream to the output stream. Both streams
     * are kept open.
     *
     * @param in the input stream
     * @param out the output stream (null if writing is not required)
     * @param length the maximum number of bytes to copy
     * @return the number of bytes copied
     */
    public static long copy(InputStream in, OutputStream out, long length)
            throws IOException {
        try {
            long copied = 0;
            int len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            byte[] buffer = new byte[len];
            while (length > 0) {
                len = in.read(buffer, 0, len);
                if (len < 0) {
                    break;
                }
                if (out != null) {
                    out.write(buffer, 0, len);
                }
                copied += len;
                length -= len;
                len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            }
            return copied;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }

    /**
     * Copy all data from the reader to the writer and close the reader.
     * Exceptions while closing are ignored.
     *
     * @param in the reader
     * @param out the writer (null if writing is not required)
     * @param length the maximum number of bytes to copy
     * @return the number of characters copied
     */
    public static long copyAndCloseInput(Reader in, Writer out, long length)
            throws IOException {
        try {
            long copied = 0;
            int len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            char[] buffer = new char[len];
            while (length > 0) {
                len = in.read(buffer, 0, len);
                if (len < 0) {
                    break;
                }
                if (out != null) {
                    out.write(buffer, 0, len);
                }
                length -= len;
                len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
                copied += len;
            }
            return copied;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        } finally {
            in.close();
        }
    }

    /**
     * Close an input stream without throwing an exception.
     *
     * @param in the input stream or null
     */
    public static void closeSilently(InputStream in) {
        if (in != null) {
            try {
                trace("closeSilently", null, in);
                in.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Close a reader without throwing an exception.
     *
     * @param reader the reader or null
     */
    public static void closeSilently(Reader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Close a writer without throwing an exception.
     *
     * @param writer the writer or null
     */
    public static void closeSilently(Writer writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Read a number of bytes from an input stream and close the stream.
     *
     * @param in the input stream
     * @param length the maximum number of bytes to read, or -1 to read until
     *            the end of file
     * @return the bytes read
     */
    public static byte[] readBytesAndClose(InputStream in, int length)
            throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(Constants.IO_BUFFER_SIZE, length);
            ByteArrayOutputStream out = new ByteArrayOutputStream(block);
            copy(in, out, length);
            return out.toByteArray();
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        } finally {
            in.close();
        }
    }

    /**
     * Read a number of characters from a reader and close it.
     *
     * @param in the reader
     * @param length the maximum number of characters to read, or -1 to read
     *            until the end of file
     * @return the string read
     */
    public static String readStringAndClose(Reader in, int length)
            throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(Constants.IO_BUFFER_SIZE, length);
            StringWriter out = new StringWriter(block);
            copyAndCloseInput(in, out, length);
            return out.toString();
        } finally {
            in.close();
        }
    }

    /**
     * Try to read the given number of bytes to the buffer. This method reads
     * until the maximum number of bytes have been read or until the end of
     * file.
     *
     * @param in the input stream
     * @param buffer the output buffer
     * @param max the number of bytes to read at most
     * @return the number of bytes read, 0 meaning EOF
     */
    public static int readFully(InputStream in, byte[] buffer, int max)
            throws IOException {
        try {
            int result = 0, len = Math.min(max, buffer.length);
            while (len > 0) {
                int l = in.read(buffer, result, len);
                if (l < 0) {
                    break;
                }
                result += l;
                len -= l;
            }
            return result;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }

    /**
     * Try to read the given number of characters to the buffer. This method
     * reads until the maximum number of characters have been read or until the
     * end of file.
     *
     * @param in the reader
     * @param buffer the output buffer
     * @param max the number of characters to read at most
     * @return the number of characters read, 0 meaning EOF
     */
    public static int readFully(Reader in, char[] buffer, int max)
            throws IOException {
        try {
            int result = 0, len = Math.min(max, buffer.length);
            while (len > 0) {
                int l = in.read(buffer, result, len);
                if (l < 0) {
                    break;
                }
                result += l;
                len -= l;
            }
            return result;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }

    /**
     * Create a buffered reader to read from an input stream using the UTF-8
     * format. If the input stream is null, this method returns null. The
     * InputStreamReader that is used here is not exact, that means it may read
     * some additional bytes when buffering.
     *
     * @param in the input stream or null
     * @return the reader
     */
    public static Reader getBufferedReader(InputStream in) {
        return in == null ? null : new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    /**
     * Create a reader to read from an input stream using the UTF-8 format. If
     * the input stream is null, this method returns null. The InputStreamReader
     * that is used here is not exact, that means it may read some additional
     * bytes when buffering.
     *
     * @param in the input stream or null
     * @return the reader
     */
    public static Reader getReader(InputStream in) {
        // InputStreamReader may read some more bytes
        return in == null ? null : new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    /**
     * Create a buffered writer to write to an output stream using the UTF-8
     * format. If the output stream is null, this method returns null.
     *
     * @param out the output stream or null
     * @return the writer
     */
    public static Writer getBufferedWriter(OutputStream out) {
        return out == null ? null : new BufferedWriter(
                new OutputStreamWriter(out, StandardCharsets.UTF_8));
    }

    /**
     * Wrap an input stream in a reader. The bytes are converted to characters
     * using the US-ASCII character set.
     *
     * @param in the input stream
     * @return the reader
     */
    public static Reader getAsciiReader(InputStream in) {
        return in == null ? null : new InputStreamReader(in, StandardCharsets.US_ASCII);
    }

    /**
     * Trace input or output operations if enabled.
     *
     * @param method the method from where this method was called
     * @param fileName the file name
     * @param o the object to append to the message
     */
    public static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("IOUtils." + method + " " + fileName + " " + o);
        }
    }

    /**
     * Create an input stream to read from a string. The string is converted to
     * a byte array using UTF-8 encoding.
     * If the string is null, this method returns null.
     *
     * @param s the string
     * @return the input stream
     */
    public static InputStream getInputStreamFromString(String s) {
        if (s == null) {
            return null;
        }
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Copy a file from one directory to another, or to another file.
     *
     * @param original the original file name
     * @param copy the file name of the copy
     */
    public static void copyFiles(String original, String copy) throws IOException {
        InputStream in = FileUtils.newInputStream(original);
        OutputStream out = FileUtils.newOutputStream(copy, false);
        copyAndClose(in, out);
    }

}
