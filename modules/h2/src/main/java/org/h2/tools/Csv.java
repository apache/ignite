/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A facility to read from and write to CSV (comma separated values) files. When
 * reading, the BOM (the byte-order-mark) character 0xfeff at the beginning of
 * the file is ignored.
 *
 * @author Thomas Mueller, Sylvain Cuaz
 */
public class Csv implements SimpleRowSource {

    private String[] columnNames;

    private String characterSet = SysProperties.FILE_ENCODING;
    private char escapeCharacter = '\"';
    private char fieldDelimiter = '\"';
    private char fieldSeparatorRead = ',';
    private String fieldSeparatorWrite = ",";
    private boolean caseSensitiveColumnNames;
    private boolean preserveWhitespace;
    private boolean writeColumnHeader = true;
    private char lineComment;
    private String lineSeparator = SysProperties.LINE_SEPARATOR;
    private String nullString = "";

    private String fileName;
    private Reader input;
    private char[] inputBuffer;
    private int inputBufferPos;
    private int inputBufferStart = -1;
    private int inputBufferEnd;
    private Writer output;
    private boolean endOfLine, endOfFile;

    private int writeResultSet(ResultSet rs) throws SQLException {
        try {
            int rows = 0;
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            String[] row = new String[columnCount];
            int[] sqlTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = meta.getColumnLabel(i + 1);
                sqlTypes[i] = meta.getColumnType(i + 1);
            }
            if (writeColumnHeader) {
                writeRow(row);
            }
            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    Object o;
                    switch (sqlTypes[i]) {
                    case Types.DATE:
                        o = rs.getDate(i + 1);
                        break;
                    case Types.TIME:
                        o = rs.getTime(i + 1);
                        break;
                    case Types.TIMESTAMP:
                        o = rs.getTimestamp(i + 1);
                        break;
                    default:
                        o = rs.getString(i + 1);
                    }
                    row[i] = o == null ? null : o.toString();
                }
                writeRow(row);
                rows++;
            }
            output.close();
            return rows;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            close();
            JdbcUtils.closeSilently(rs);
        }
    }

    /**
     * Writes the result set to a file in the CSV format.
     *
     * @param writer the writer
     * @param rs the result set
     * @return the number of rows written
     */
    public int write(Writer writer, ResultSet rs) throws SQLException {
        this.output = writer;
        return writeResultSet(rs);
    }

    /**
     * Writes the result set to a file in the CSV format. The result set is read
     * using the following loop:
     *
     * <pre>
     * while (rs.next()) {
     *     writeRow(row);
     * }
     * </pre>
     *
     * @param outputFileName the name of the csv file
     * @param rs the result set - the result set must be positioned before the
     *          first row.
     * @param charset the charset or null to use the system default charset
     *          (see system property file.encoding)
     * @return the number of rows written
     */
    public int write(String outputFileName, ResultSet rs, String charset)
            throws SQLException {
        init(outputFileName, charset);
        try {
            initWrite();
            return writeResultSet(rs);
        } catch (IOException e) {
            throw convertException("IOException writing " + outputFileName, e);
        }
    }

    /**
     * Writes the result set of a query to a file in the CSV format.
     *
     * @param conn the connection
     * @param outputFileName the file name
     * @param sql the query
     * @param charset the charset or null to use the system default charset
     *          (see system property file.encoding)
     * @return the number of rows written
     */
    public int write(Connection conn, String outputFileName, String sql,
            String charset) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        int rows = write(outputFileName, rs, charset);
        stat.close();
        return rows;
    }

    /**
     * Reads from the CSV file and returns a result set. The rows in the result
     * set are created on demand, that means the file is kept open until all
     * rows are read or the result set is closed.
     * <br />
     * If the columns are read from the CSV file, then the following rules are
     * used: columns names that start with a letter or '_', and only
     * contain letters, '_', and digits, are considered case insensitive
     * and are converted to uppercase. Other column names are considered
     * case sensitive (that means they need to be quoted when accessed).
     *
     * @param inputFileName the file name
     * @param colNames or null if the column names should be read from the CSV
     *          file
     * @param charset the charset or null to use the system default charset
     *          (see system property file.encoding)
     * @return the result set
     */
    public ResultSet read(String inputFileName, String[] colNames,
            String charset) throws SQLException {
        init(inputFileName, charset);
        try {
            return readResultSet(colNames);
        } catch (IOException e) {
            throw convertException("IOException reading " + inputFileName, e);
        }
    }

    /**
     * Reads CSV data from a reader and returns a result set. The rows in the
     * result set are created on demand, that means the reader is kept open
     * until all rows are read or the result set is closed.
     *
     * @param reader the reader
     * @param colNames or null if the column names should be read from the CSV
     *            file
     * @return the result set
     */
    public ResultSet read(Reader reader, String[] colNames) throws IOException {
        init(null, null);
        this.input = reader;
        return readResultSet(colNames);
    }

    private ResultSet readResultSet(String[] colNames) throws IOException {
        this.columnNames = colNames;
        initRead();
        SimpleResultSet result = new SimpleResultSet(this);
        makeColumnNamesUnique();
        for (String columnName : columnNames) {
            result.addColumn(columnName, Types.VARCHAR, Integer.MAX_VALUE, 0);
        }
        return result;
    }

    private void makeColumnNamesUnique() {
        for (int i = 0; i < columnNames.length; i++) {
            StringBuilder buff = new StringBuilder();
            String n = columnNames[i];
            if (n == null || n.length() == 0) {
                buff.append('C').append(i + 1);
            } else {
                buff.append(n);
            }
            for (int j = 0; j < i; j++) {
                String y = columnNames[j];
                if (buff.toString().equals(y)) {
                    buff.append('1');
                    j = -1;
                }
            }
            columnNames[i] = buff.toString();
        }
    }

    private void init(String newFileName, String charset) {
        this.fileName = newFileName;
        if (charset != null) {
            this.characterSet = charset;
        }
    }

    private void initWrite() throws IOException {
        if (output == null) {
            try {
                OutputStream out = FileUtils.newOutputStream(fileName, false);
                out = new BufferedOutputStream(out, Constants.IO_BUFFER_SIZE);
                output = new BufferedWriter(new OutputStreamWriter(out, characterSet));
            } catch (Exception e) {
                close();
                throw DbException.convertToIOException(e);
            }
        }
    }

    private void writeRow(String[] values) throws IOException {
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                if (fieldSeparatorWrite != null) {
                    output.write(fieldSeparatorWrite);
                }
            }
            String s = values[i];
            if (s != null) {
                if (escapeCharacter != 0) {
                    if (fieldDelimiter != 0) {
                        output.write(fieldDelimiter);
                    }
                    output.write(escape(s));
                    if (fieldDelimiter != 0) {
                        output.write(fieldDelimiter);
                    }
                } else {
                    output.write(s);
                }
            } else if (nullString != null && nullString.length() > 0) {
                output.write(nullString);
            }
        }
        output.write(lineSeparator);
    }

    private String escape(String data) {
        if (data.indexOf(fieldDelimiter) < 0) {
            if (escapeCharacter == fieldDelimiter || data.indexOf(escapeCharacter) < 0) {
                return data;
            }
        }
        int length = data.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char ch = data.charAt(i);
            if (ch == fieldDelimiter || ch == escapeCharacter) {
                buff.append(escapeCharacter);
            }
            buff.append(ch);
        }
        return buff.toString();
    }

    private void initRead() throws IOException {
        if (input == null) {
            try {
                InputStream in = FileUtils.newInputStream(fileName);
                in = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE);
                input = new InputStreamReader(in, characterSet);
            } catch (IOException e) {
                close();
                throw e;
            }
        }
        if (!input.markSupported()) {
            input = new BufferedReader(input);
        }
        input.mark(1);
        int bom = input.read();
        if (bom != 0xfeff) {
            // Microsoft Excel compatibility
            // ignore pseudo-BOM
            input.reset();
        }
        inputBuffer = new char[Constants.IO_BUFFER_SIZE * 2];
        if (columnNames == null) {
            readHeader();
        }
    }

    private void readHeader() throws IOException {
        ArrayList<String> list = New.arrayList();
        while (true) {
            String v = readValue();
            if (v == null) {
                if (endOfLine) {
                    if (endOfFile || !list.isEmpty()) {
                        break;
                    }
                } else {
                    v = "COLUMN" + list.size();
                    list.add(v);
                }
            } else {
                if (v.length() == 0) {
                    v = "COLUMN" + list.size();
                } else if (!caseSensitiveColumnNames && isSimpleColumnName(v)) {
                    v = StringUtils.toUpperEnglish(v);
                }
                list.add(v);
                if (endOfLine) {
                    break;
                }
            }
        }
        columnNames = list.toArray(new String[0]);
    }

    private static boolean isSimpleColumnName(String columnName) {
        for (int i = 0, length = columnName.length(); i < length; i++) {
            char ch = columnName.charAt(i);
            if (i == 0) {
                if (ch != '_' && !Character.isLetter(ch)) {
                    return false;
                }
            } else {
                if (ch != '_' && !Character.isLetterOrDigit(ch)) {
                    return false;
                }
            }
        }
        return columnName.length() != 0;
    }

    private void pushBack() {
        inputBufferPos--;
    }

    private int readChar() throws IOException {
        if (inputBufferPos >= inputBufferEnd) {
            return readBuffer();
        }
        return inputBuffer[inputBufferPos++];
    }

    private int readBuffer() throws IOException {
        if (endOfFile) {
            return -1;
        }
        int keep;
        if (inputBufferStart >= 0) {
            keep = inputBufferPos - inputBufferStart;
            if (keep > 0) {
                char[] src = inputBuffer;
                if (keep + Constants.IO_BUFFER_SIZE > src.length) {
                    inputBuffer = new char[src.length * 2];
                }
                System.arraycopy(src, inputBufferStart, inputBuffer, 0, keep);
            }
            inputBufferStart = 0;
        } else {
            keep = 0;
        }
        inputBufferPos = keep;
        int len = input.read(inputBuffer, keep, Constants.IO_BUFFER_SIZE);
        if (len == -1) {
            // ensure bufferPos > bufferEnd
            // even after pushBack
            inputBufferEnd = -1024;
            endOfFile = true;
            // ensure the right number of characters are read
            // in case the input buffer is still used
            inputBufferPos++;
            return -1;
        }
        inputBufferEnd = keep + len;
        return inputBuffer[inputBufferPos++];
    }

    private String readValue() throws IOException {
        endOfLine = false;
        inputBufferStart = inputBufferPos;
        while (true) {
            int ch = readChar();
            if (ch == fieldDelimiter) {
                // delimited value
                boolean containsEscape = false;
                inputBufferStart = inputBufferPos;
                int sep;
                while (true) {
                    ch = readChar();
                    if (ch == fieldDelimiter) {
                        ch = readChar();
                        if (ch != fieldDelimiter) {
                            sep = 2;
                            break;
                        }
                        containsEscape = true;
                    } else if (ch == escapeCharacter) {
                        ch = readChar();
                        if (ch < 0) {
                            sep = 1;
                            break;
                        }
                        containsEscape = true;
                    } else if (ch < 0) {
                        sep = 1;
                        break;
                    }
                }
                String s = new String(inputBuffer,
                        inputBufferStart, inputBufferPos - inputBufferStart - sep);
                if (containsEscape) {
                    s = unEscape(s);
                }
                inputBufferStart = -1;
                while (true) {
                    if (ch == fieldSeparatorRead) {
                        break;
                    } else if (ch == '\n' || ch < 0 || ch == '\r') {
                        endOfLine = true;
                        break;
                    } else if (ch == ' ' || ch == '\t') {
                        // ignore
                    } else {
                        pushBack();
                        break;
                    }
                    ch = readChar();
                }
                return s;
            } else if (ch == '\n' || ch < 0 || ch == '\r') {
                endOfLine = true;
                return null;
            } else if (ch == fieldSeparatorRead) {
                // null
                return null;
            } else if (ch <= ' ') {
                // ignore spaces
            } else if (lineComment != 0 && ch == lineComment) {
                // comment until end of line
                inputBufferStart = -1;
                do {
                    ch = readChar();
                } while (ch != '\n' && ch >= 0 && ch != '\r');
                endOfLine = true;
                return null;
            } else {
                // un-delimited value
                while (true) {
                    ch = readChar();
                    if (ch == fieldSeparatorRead) {
                        break;
                    } else if (ch == '\n' || ch < 0 || ch == '\r') {
                        endOfLine = true;
                        break;
                    }
                }
                String s = new String(inputBuffer,
                        inputBufferStart, inputBufferPos - inputBufferStart - 1);
                if (!preserveWhitespace) {
                    s = s.trim();
                }
                inputBufferStart = -1;
                // check un-delimited value for nullString
                return readNull(s);
            }
        }
    }

    private String readNull(String s) {
        return s.equals(nullString) ? null : s;
    }

    private String unEscape(String s) {
        StringBuilder buff = new StringBuilder(s.length());
        int start = 0;
        char[] chars = null;
        while (true) {
            int idx = s.indexOf(escapeCharacter, start);
            if (idx < 0) {
                idx = s.indexOf(fieldDelimiter, start);
                if (idx < 0) {
                    break;
                }
            }
            if (chars == null) {
                chars = s.toCharArray();
            }
            buff.append(chars, start, idx - start);
            if (idx == s.length() - 1) {
                start = s.length();
                break;
            }
            buff.append(chars[idx + 1]);
            start = idx + 2;
        }
        buff.append(s, start, s.length());
        return buff.toString();
    }

    /**
     * INTERNAL
     */
    @Override
    public Object[] readRow() throws SQLException {
        if (input == null) {
            return null;
        }
        String[] row = new String[columnNames.length];
        try {
            int i = 0;
            while (true) {
                String v = readValue();
                if (v == null) {
                    if (endOfLine) {
                        if (i == 0) {
                            if (endOfFile) {
                                return null;
                            }
                            // empty line
                            continue;
                        }
                        break;
                    }
                }
                if (i < row.length) {
                    row[i++] = v;
                }
                if (endOfLine) {
                    break;
                }
            }
        } catch (IOException e) {
            throw convertException("IOException reading from " + fileName, e);
        }
        return row;
    }

    private static SQLException convertException(String message, Exception e) {
        return DbException.get(ErrorCode.IO_EXCEPTION_1, e, message).getSQLException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void close() {
        IOUtils.closeSilently(input);
        input = null;
        IOUtils.closeSilently(output);
        output = null;
    }

    /**
     * INTERNAL
     */
    @Override
    public void reset() throws SQLException {
        throw new SQLException("Method is not supported", "CSV");
    }

    /**
     * Override the field separator for writing. The default is ",".
     *
     * @param fieldSeparatorWrite the field separator
     */
    public void setFieldSeparatorWrite(String fieldSeparatorWrite) {
        this.fieldSeparatorWrite = fieldSeparatorWrite;
    }

    /**
     * Get the current field separator for writing.
     *
     * @return the field separator
     */
    public String getFieldSeparatorWrite() {
        return fieldSeparatorWrite;
    }

    /**
     * Override the case sensitive column names setting. The default is false.
     * If enabled, the case of all column names is always preserved.
     *
     * @param caseSensitiveColumnNames whether column names are case sensitive
     */
    public void setCaseSensitiveColumnNames(boolean caseSensitiveColumnNames) {
        this.caseSensitiveColumnNames = caseSensitiveColumnNames;
    }

    /**
     * Get the current case sensitive column names setting.
     *
     * @return whether column names are case sensitive
     */
    public boolean getCaseSensitiveColumnNames() {
        return caseSensitiveColumnNames;
    }

    /**
     * Override the field separator for reading. The default is ','.
     *
     * @param fieldSeparatorRead the field separator
     */
    public void setFieldSeparatorRead(char fieldSeparatorRead) {
        this.fieldSeparatorRead = fieldSeparatorRead;
    }

    /**
     * Get the current field separator for reading.
     *
     * @return the field separator
     */
    public char getFieldSeparatorRead() {
        return fieldSeparatorRead;
    }

    /**
     * Set the line comment character. The default is character code 0 (line
     * comments are disabled).
     *
     * @param lineCommentCharacter the line comment character
     */
    public void setLineCommentCharacter(char lineCommentCharacter) {
        this.lineComment = lineCommentCharacter;
    }

    /**
     * Get the line comment character.
     *
     * @return the line comment character, or 0 if disabled
     */
    public char getLineCommentCharacter() {
        return lineComment;
    }

    /**
     * Set the field delimiter. The default is " (a double quote).
     * The value 0 means no field delimiter is used.
     *
     * @param fieldDelimiter the field delimiter
     */
    public void setFieldDelimiter(char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    /**
     * Get the current field delimiter.
     *
     * @return the field delimiter
     */
    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    /**
     * Set the escape character. The escape character is used to escape the
     * field delimiter. This is needed if the data contains the field delimiter.
     * The default escape character is " (a double quote), which is the same as
     * the field delimiter. If the field delimiter and the escape character are
     * both " (double quote), and the data contains a double quote, then an
     * additional double quote is added. Example:
     * <pre>
     * Data: He said "Hello".
     * Escape character: "
     * Field delimiter: "
     * CSV file: "He said ""Hello""."
     * </pre>
     * If the field delimiter is a double quote and the escape character is a
     * backslash, then escaping is done similar to Java (however, only the field
     * delimiter is escaped). Example:
     * <pre>
     * Data: He said "Hello".
     * Escape character: \
     * Field delimiter: "
     * CSV file: "He said \"Hello\"."
     * </pre>
     * The value 0 means no escape character is used.
     *
     * @param escapeCharacter the escape character
     */
    public void setEscapeCharacter(char escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    /**
     * Get the current escape character.
     *
     * @return the escape character
     */
    public char getEscapeCharacter() {
        return escapeCharacter;
    }

    /**
     * Set the line separator used for writing. This is usually a line feed (\n
     * or \r\n depending on the system settings). The line separator is written
     * after each row (including the last row), so this option can include an
     * end-of-row marker if needed.
     *
     * @param lineSeparator the line separator
     */
    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    /**
     * Get the line separator used for writing.
     *
     * @return the line separator
     */
    public String getLineSeparator() {
        return lineSeparator;
    }

    /**
     * Set the value that represents NULL. It is only used for non-delimited
     * values.
     *
     * @param nullString the null
     */
    public void setNullString(String nullString) {
        this.nullString = nullString;
    }

    /**
     * Get the current null string.
     *
     * @return the null string.
     */
    public String getNullString() {
        return nullString;
    }

    /**
     * Enable or disable preserving whitespace in unquoted text.
     *
     * @param value the new value for the setting
     */
    public void setPreserveWhitespace(boolean value) {
        this.preserveWhitespace = value;
    }

    /**
     * Whether whitespace in unquoted text is preserved.
     *
     * @return the current value for the setting
     */
    public boolean getPreserveWhitespace() {
        return preserveWhitespace;
    }

    /**
     * Enable or disable writing the column header.
     *
     * @param value the new value for the setting
     */
    public void setWriteColumnHeader(boolean value) {
        this.writeColumnHeader = value;
    }

    /**
     * Whether the column header is written.
     *
     * @return the current value for the setting
     */
    public boolean getWriteColumnHeader() {
        return writeColumnHeader;
    }

    /**
     * INTERNAL.
     * Parse and set the CSV options.
     *
     * @param options the the options
     * @return the character set
     */
    public String setOptions(String options) {
        String charset = null;
        String[] keyValuePairs = StringUtils.arraySplit(options, ' ', false);
        for (String pair : keyValuePairs) {
            if (pair.length() == 0) {
                continue;
            }
            int index = pair.indexOf('=');
            String key = StringUtils.trim(pair.substring(0, index), true, true, " ");
            String value = pair.substring(index + 1);
            char ch = value.length() == 0 ? 0 : value.charAt(0);
            if (isParam(key, "escape", "esc", "escapeCharacter")) {
                setEscapeCharacter(ch);
            } else if (isParam(key, "fieldDelimiter", "fieldDelim")) {
                setFieldDelimiter(ch);
            } else if (isParam(key, "fieldSeparator", "fieldSep")) {
                setFieldSeparatorRead(ch);
                setFieldSeparatorWrite(value);
            } else if (isParam(key, "lineComment", "lineCommentCharacter")) {
                setLineCommentCharacter(ch);
            } else if (isParam(key, "lineSeparator", "lineSep")) {
                setLineSeparator(value);
            } else if (isParam(key, "null", "nullString")) {
                setNullString(value);
            } else if (isParam(key, "charset", "characterSet")) {
                charset = value;
            } else if (isParam(key, "preserveWhitespace")) {
                setPreserveWhitespace(Utils.parseBoolean(value, false, false));
            } else if (isParam(key, "writeColumnHeader")) {
                setWriteColumnHeader(Utils.parseBoolean(value, true, false));
            } else if (isParam(key, "caseSensitiveColumnNames")) {
                setCaseSensitiveColumnNames(Utils.parseBoolean(value, false, false));
            } else {
                throw DbException.getUnsupportedException(key);
            }
        }
        return charset;
    }

    private static boolean isParam(String key, String... values) {
        for (String v : values) {
            if (key.equalsIgnoreCase(v)) {
                return true;
            }
        }
        return false;
    }

}
