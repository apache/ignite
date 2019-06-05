/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.compress.CompressLZF;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.MetaRecord;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.h2.mvstore.StreamStore;
import org.h2.mvstore.db.TransactionStore;
import org.h2.mvstore.db.TransactionStore.TransactionMap;
import org.h2.mvstore.db.ValueDataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SimpleRow;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.store.DataReader;
import org.h2.store.FileLister;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.LobStorageBackend;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageMap;
import org.h2.store.Page;
import org.h2.store.PageFreeList;
import org.h2.store.PageLog;
import org.h2.store.PageStore;
import org.h2.store.fs.FileUtils;
import org.h2.util.BitField;
import org.h2.util.IOUtils;
import org.h2.util.IntArray;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.util.Tool;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueLong;

/**
 * Helps recovering a corrupted database.
 * @h2.resource
 */
public class Recover extends Tool implements DataHandler {

    private String databaseName;
    private int storageId;
    private String storageName;
    private int recordLength;
    private int valueId;
    private boolean trace;
    private boolean transactionLog;
    private ArrayList<MetaRecord> schema;
    private HashSet<Integer> objectIdSet;
    private HashMap<Integer, String> tableMap;
    private HashMap<String, String> columnTypeMap;
    private boolean remove;

    private int pageSize;
    private FileStore store;
    private int[] parents;

    private Stats stat;
    private boolean lobMaps;

    /**
     * Statistic data
     */
    static class Stats {

        /**
         * The empty space in bytes in a data leaf pages.
         */
        long pageDataEmpty;

        /**
         * The number of bytes used for data.
         */
        long pageDataRows;

        /**
         * The number of bytes used for the page headers.
         */
        long pageDataHead;

        /**
         * The count per page type.
         */
        final int[] pageTypeCount = new int[Page.TYPE_STREAM_DATA + 2];

        /**
         * The number of free pages.
         */
        int free;
    }

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>The database name (all databases if not set)</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Print additional trace information</td></tr>
     * <tr><td>[-transactionLog]</td>
     * <td>Print the transaction log</td></tr>
     * </table>
     * Encrypted databases need to be decrypted first.
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Recover().runTool(args);
    }

    /**
     * Dumps the contents of a database file to a human readable text file. This
     * text file can be used to recover most of the data. This tool does not
     * open the database and can be used even if the database files are
     * corrupted. A database can get corrupted if there is a bug in the database
     * engine or file system software, or if an application writes into the
     * database file that doesn't understand the the file format, or if there is
     * a hardware problem.
     *
     * @param args the command line arguments
     */
    @Override
    public void runTool(String... args) throws SQLException {
        String dir = ".";
        String db = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if ("-dir".equals(arg)) {
                dir = args[++i];
            } else if ("-db".equals(arg)) {
                db = args[++i];
            } else if ("-removePassword".equals(arg)) {
                remove = true;
            } else if ("-trace".equals(arg)) {
                trace = true;
            } else if ("-transactionLog".equals(arg)) {
                transactionLog = true;
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        process(dir, db);
    }

    /**
     * INTERNAL
     */
    public static Reader readClob(String fileName) throws IOException {
        return new BufferedReader(new InputStreamReader(readBlob(fileName),
                StandardCharsets.UTF_8));
    }

    /**
     * INTERNAL
     */
    public static InputStream readBlob(String fileName) throws IOException {
        return new BufferedInputStream(FileUtils.newInputStream(fileName));
    }

    /**
     * INTERNAL
     */
    public static Value.ValueBlob readBlobDb(Connection conn, long lobId,
            long precision) {
        DataHandler h = ((JdbcConnection) conn).getSession().getDataHandler();
        verifyPageStore(h);
        ValueLobDb lob = ValueLobDb.create(Value.BLOB, h, LobStorageFrontend.TABLE_TEMP,
                lobId, null, precision);
        lob.setRecoveryReference(true);
        return lob;
    }

    private static void verifyPageStore(DataHandler h) {
        if (h.getLobStorage() instanceof LobStorageMap) {
            throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                    "Restore page store recovery SQL script " +
                    "can only be restored to a PageStore file");
        }
    }

    /**
     * INTERNAL
     */
    public static Value.ValueClob readClobDb(Connection conn, long lobId,
            long precision) {
        DataHandler h = ((JdbcConnection) conn).getSession().getDataHandler();
        verifyPageStore(h);
        ValueLobDb lob =  ValueLobDb.create(Value.CLOB, h, LobStorageFrontend.TABLE_TEMP,
                lobId, null, precision);
        lob.setRecoveryReference(true);
        return lob;
    }

    /**
     * INTERNAL
     */
    public static InputStream readBlobMap(Connection conn, long lobId,
            long precision) throws SQLException {
        final PreparedStatement prep = conn.prepareStatement(
                "SELECT DATA FROM INFORMATION_SCHEMA.LOB_BLOCKS " +
                "WHERE LOB_ID = ? AND SEQ = ? AND ? > 0");
        prep.setLong(1, lobId);
        // precision is currently not really used,
        // it is just to improve readability of the script
        prep.setLong(3, precision);
        return new SequenceInputStream(
            new Enumeration<InputStream>() {

                private int seq;
                private byte[] data = fetch();

                private byte[] fetch() {
                    try {
                        prep.setInt(2, seq++);
                        ResultSet rs = prep.executeQuery();
                        if (rs.next()) {
                            return rs.getBytes(1);
                        }
                        return null;
                    } catch (SQLException e) {
                        throw DbException.convert(e);
                    }
                }

                @Override
                public boolean hasMoreElements() {
                    return data != null;
                }

                @Override
                public InputStream nextElement() {
                    ByteArrayInputStream in = new ByteArrayInputStream(data);
                    data = fetch();
                    return in;
                }
            }
        );
    }

    /**
     * INTERNAL
     */
    public static Reader readClobMap(Connection conn, long lobId, long precision)
            throws Exception {
        InputStream in = readBlobMap(conn, lobId, precision);
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    private void trace(String message) {
        if (trace) {
            out.println(message);
        }
    }

    private void traceError(String message, Throwable t) {
        out.println(message + ": " + t.toString());
        if (trace) {
            t.printStackTrace(out);
        }
    }

    /**
     * Dumps the contents of a database to a SQL script file.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     */
    public static void execute(String dir, String db) throws SQLException {
        try {
            new Recover().process(dir, db);
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    private void process(String dir, String db) {
        ArrayList<String> list = FileLister.getDatabaseFiles(dir, db, true);
        if (list.isEmpty()) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (String fileName : list) {
            if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
                dumpPageStore(fileName);
            } else if (fileName.endsWith(Constants.SUFFIX_LOB_FILE)) {
                dumpLob(fileName, false);
            } else if (fileName.endsWith(Constants.SUFFIX_MV_FILE)) {
                String f = fileName.substring(0, fileName.length() -
                        Constants.SUFFIX_PAGE_FILE.length());
                PrintWriter writer;
                writer = getWriter(fileName, ".txt");
                MVStoreTool.dump(fileName, writer, true);
                MVStoreTool.info(fileName, writer);
                writer.close();
                writer = getWriter(f + ".h2.db", ".sql");
                dumpMVStoreFile(writer, fileName);
                writer.close();
            }
        }
    }

    private PrintWriter getWriter(String fileName, String suffix) {
        fileName = fileName.substring(0, fileName.length() - 3);
        String outputFile = fileName + suffix;
        trace("Created file: " + outputFile);
        try {
            return new PrintWriter(IOUtils.getBufferedWriter(
                    FileUtils.newOutputStream(outputFile, false)));
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private void writeDataError(PrintWriter writer, String error, byte[] data) {
        writer.println("-- ERROR: " + error + " storageId: "
                + storageId + " recordLength: " + recordLength + " valueId: " + valueId);
        StringBuilder sb = new StringBuilder();
        for (byte aData1 : data) {
            int x = aData1 & 0xff;
            if (x >= ' ' && x < 128) {
                sb.append((char) x);
            } else {
                sb.append('?');
            }
        }
        writer.println("-- dump: " + sb.toString());
        sb = new StringBuilder();
        for (byte aData : data) {
            int x = aData & 0xff;
            sb.append(' ');
            if (x < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(x));
        }
        writer.println("-- dump: " + sb.toString());
    }

    private void dumpLob(String fileName, boolean lobCompression) {
        OutputStream fileOut = null;
        FileStore fileStore = null;
        long size = 0;
        String n = fileName + (lobCompression ? ".comp" : "") + ".txt";
        InputStream in = null;
        try {
            fileOut = FileUtils.newOutputStream(n, false);
            fileStore = FileStore.open(null, fileName, "r");
            fileStore.init();
            in = new FileStoreInputStream(fileStore, this, lobCompression, false);
            size = IOUtils.copy(in, fileOut);
        } catch (Throwable e) {
            // this is usually not a problem, because we try both compressed and
            // uncompressed
        } finally {
            IOUtils.closeSilently(fileOut);
            IOUtils.closeSilently(in);
            closeSilently(fileStore);
        }
        if (size == 0) {
            try {
                FileUtils.delete(n);
            } catch (Exception e) {
                traceError(n, e);
            }
        }
    }

    private String getSQL(String column, Value v) {
        if (v instanceof ValueLob) {
            ValueLob lob = (ValueLob) v;
            byte[] small = lob.getSmall();
            if (small == null) {
                String file = lob.getFileName();
                String type = lob.getType() == Value.BLOB ? "BLOB" : "CLOB";
                if (lob.isCompressed()) {
                    dumpLob(file, true);
                    file += ".comp";
                }
                return "READ_" + type + "('" + file + ".txt')";
            }
        } else if (v instanceof ValueLobDb) {
            ValueLobDb lob = (ValueLobDb) v;
            byte[] small = lob.getSmall();
            if (small == null) {
                int type = lob.getType();
                long id = lob.getLobId();
                long precision = lob.getPrecision();
                String m;
                String columnType;
                if (type == Value.BLOB) {
                    columnType = "BLOB";
                    m = "READ_BLOB";
                } else {
                    columnType = "CLOB";
                    m = "READ_CLOB";
                }
                if (lobMaps) {
                    m += "_MAP";
                } else {
                    m += "_DB";
                }
                columnTypeMap.put(column, columnType);
                return m + "(" + id + ", " + precision + ")";
            }
        }
        return v.getSQL();
    }

    private void setDatabaseName(String name) {
        databaseName = name;
    }

    private void dumpPageStore(String fileName) {
        setDatabaseName(fileName.substring(0, fileName.length() -
                Constants.SUFFIX_PAGE_FILE.length()));
        PrintWriter writer = null;
        stat = new Stats();
        try {
            writer = getWriter(fileName, ".sql");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB FOR \"" +
                    this.getClass().getName() + ".readBlob\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB FOR \"" +
                    this.getClass().getName() + ".readClob\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB_DB FOR \"" +
                    this.getClass().getName() + ".readBlobDb\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB_DB FOR \"" +
                    this.getClass().getName() + ".readClobDb\";");
            resetSchema();
            store = FileStore.open(null, fileName, remove ? "rw" : "r");
            long length = store.length();
            try {
                store.init();
            } catch (Exception e) {
                writeError(writer, e);
            }
            Data s = Data.create(this, 128);
            seek(0);
            store.readFully(s.getBytes(), 0, 128);
            s.setPos(48);
            pageSize = s.readInt();
            int writeVersion = s.readByte();
            int readVersion = s.readByte();
            writer.println("-- pageSize: " + pageSize +
                    " writeVersion: " + writeVersion +
                    " readVersion: " + readVersion);
            if (pageSize < PageStore.PAGE_SIZE_MIN ||
                    pageSize > PageStore.PAGE_SIZE_MAX) {
                pageSize = Constants.DEFAULT_PAGE_SIZE;
                writer.println("-- ERROR: page size; using " + pageSize);
            }
            long pageCount = length / pageSize;
            parents = new int[(int) pageCount];
            s = Data.create(this, pageSize);
            for (long i = 3; i < pageCount; i++) {
                s.reset();
                seek(i);
                store.readFully(s.getBytes(), 0, 32);
                s.readByte();
                s.readShortInt();
                parents[(int) i] = s.readInt();
            }
            int logKey = 0, logFirstTrunkPage = 0, logFirstDataPage = 0;
            s = Data.create(this, pageSize);
            for (long i = 1;; i++) {
                if (i == 3) {
                    break;
                }
                s.reset();
                seek(i);
                store.readFully(s.getBytes(), 0, pageSize);
                CRC32 crc = new CRC32();
                crc.update(s.getBytes(), 4, pageSize - 4);
                int expected = (int) crc.getValue();
                int got = s.readInt();
                long writeCounter = s.readLong();
                int key = s.readInt();
                int firstTrunkPage = s.readInt();
                int firstDataPage = s.readInt();
                if (expected == got) {
                    logKey = key;
                    logFirstTrunkPage = firstTrunkPage;
                    logFirstDataPage = firstDataPage;
                }
                writer.println("-- head " + i +
                        ": writeCounter: " + writeCounter +
                        " log " + key + ":" + firstTrunkPage + "/" + firstDataPage +
                        " crc " + got + " (" + (expected == got ?
                                "ok" : ("expected: " + expected)) + ")");
            }
            writer.println("-- log " + logKey + ":" + logFirstTrunkPage +
                    "/" + logFirstDataPage);

            PrintWriter devNull = new PrintWriter(new OutputStream() {
                @Override
                public void write(int b) {
                    // ignore
                }
            });
            dumpPageStore(devNull, pageCount);
            stat = new Stats();
            schema.clear();
            objectIdSet = new HashSet<>();
            dumpPageStore(writer, pageCount);
            writeSchema(writer);
            try {
                dumpPageLogStream(writer, logKey, logFirstTrunkPage,
                        logFirstDataPage, pageCount);
            } catch (IOException e) {
                // ignore
            }
            writer.println("---- Statistics ----");
            writer.println("-- page count: " + pageCount + ", free: " + stat.free);
            long total = Math.max(1, stat.pageDataRows +
                    stat.pageDataEmpty + stat.pageDataHead);
            writer.println("-- page data bytes: head " + stat.pageDataHead +
                    ", empty " + stat.pageDataEmpty +
                    ", rows " + stat.pageDataRows +
                    " (" + (100 - 100L * stat.pageDataEmpty / total) + "% full)");
            for (int i = 0; i < stat.pageTypeCount.length; i++) {
                int count = stat.pageTypeCount[i];
                if (count > 0) {
                    writer.println("-- " + getPageType(i) + " " +
                            (100 * count / pageCount) + "%, " + count + " page(s)");
                }
            }
            writer.close();
        } catch (Throwable e) {
            writeError(writer, e);
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }

    private void dumpMVStoreFile(PrintWriter writer, String fileName) {
        writer.println("-- MVStore");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB FOR \"" +
                this.getClass().getName() + ".readBlob\";");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB FOR \"" +
                this.getClass().getName() + ".readClob\";");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB_DB FOR \"" +
                this.getClass().getName() + ".readBlobDb\";");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB_DB FOR \"" +
                this.getClass().getName() + ".readClobDb\";");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB_MAP FOR \"" +
                this.getClass().getName() + ".readBlobMap\";");
        writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB_MAP FOR \"" +
                this.getClass().getName() + ".readClobMap\";");
        resetSchema();
        setDatabaseName(fileName.substring(0, fileName.length() -
                Constants.SUFFIX_MV_FILE.length()));
        MVStore mv = new MVStore.Builder().
                fileName(fileName).readOnly().open();
        dumpLobMaps(writer, mv);
        writer.println("-- Meta");
        dumpMeta(writer, mv);
        writer.println("-- Tables");
        TransactionStore store = new TransactionStore(mv);
        try {
            store.init();
        } catch (Throwable e) {
            writeError(writer, e);
        }
        try {
            for (String mapName : mv.getMapNames()) {
                if (!mapName.startsWith("table.")) {
                    continue;
                }
                String tableId = mapName.substring("table.".length());
                ValueDataType keyType = new ValueDataType(
                        null, this, null);
                ValueDataType valueType = new ValueDataType(
                        null, this, null);
                TransactionMap<Value, Value> dataMap = store.begin().openMap(
                        mapName, keyType, valueType);
                Iterator<Value> dataIt = dataMap.keyIterator(null);
                boolean init = false;
                while (dataIt.hasNext()) {
                    Value rowId = dataIt.next();
                    Value[] values = ((ValueArray) dataMap.get(rowId)).getList();
                    recordLength = values.length;
                    if (!init) {
                        setStorage(Integer.parseInt(tableId));
                        // init the column types
                        for (valueId = 0; valueId < recordLength; valueId++) {
                            String columnName = storageName + "." + valueId;
                            getSQL(columnName, values[valueId]);
                        }
                        createTemporaryTable(writer);
                        init = true;
                    }
                    StringBuilder buff = new StringBuilder();
                    buff.append("INSERT INTO O_").append(tableId)
                            .append(" VALUES(");
                    for (valueId = 0; valueId < recordLength; valueId++) {
                        if (valueId > 0) {
                            buff.append(", ");
                        }
                        String columnName = storageName + "." + valueId;
                        buff.append(getSQL(columnName, values[valueId]));
                    }
                    buff.append(");");
                    writer.println(buff.toString());
                    if (storageId == 0) {
                        try {
                            SimpleRow r = new SimpleRow(values);
                            MetaRecord meta = new MetaRecord(r);
                            schema.add(meta);
                            if (meta.getObjectType() == DbObject.TABLE_OR_VIEW) {
                                String sql = values[3].getString();
                                String name = extractTableOrViewName(sql);
                                tableMap.put(meta.getId(), name);
                            }
                        } catch (Throwable t) {
                            writeError(writer, t);
                        }
                    }
                }
            }
            writeSchema(writer);
            writer.println("DROP ALIAS READ_BLOB_MAP;");
            writer.println("DROP ALIAS READ_CLOB_MAP;");
            writer.println("DROP TABLE IF EXISTS INFORMATION_SCHEMA.LOB_BLOCKS;");
        } catch (Throwable e) {
            writeError(writer, e);
        } finally {
            mv.close();
        }
    }

    private static void dumpMeta(PrintWriter writer, MVStore mv) {
        MVMap<String, String> meta = mv.getMetaMap();
        for (Entry<String, String> e : meta.entrySet()) {
            writer.println("-- " + e.getKey() + " = " + e.getValue());
        }
    }

    private void dumpLobMaps(PrintWriter writer, MVStore mv) {
        lobMaps = mv.hasMap("lobData");
        if (!lobMaps) {
            return;
        }
        MVMap<Long, byte[]> lobData = mv.openMap("lobData");
        StreamStore streamStore = new StreamStore(lobData);
        MVMap<Long, Object[]> lobMap = mv.openMap("lobMap");
        writer.println("-- LOB");
        writer.println("CREATE TABLE IF NOT EXISTS " +
                "INFORMATION_SCHEMA.LOB_BLOCKS(" +
                "LOB_ID BIGINT, SEQ INT, DATA BINARY, " +
                "PRIMARY KEY(LOB_ID, SEQ));");
        boolean hasErrors = false;
        for (Entry<Long, Object[]> e : lobMap.entrySet()) {
            long lobId = e.getKey();
            Object[] value = e.getValue();
            byte[] streamStoreId = (byte[]) value[0];
            InputStream in = streamStore.get(streamStoreId);
            int len = 8 * 1024;
            byte[] block = new byte[len];
            try {
                for (int seq = 0;; seq++) {
                    int l = IOUtils.readFully(in, block, block.length);
                    String x = StringUtils.convertBytesToHex(block, l);
                    if (l > 0) {
                        writer.println("INSERT INTO INFORMATION_SCHEMA.LOB_BLOCKS " +
                                "VALUES(" + lobId + ", " + seq + ", '" + x + "');");
                    }
                    if (l != len) {
                        break;
                    }
                }
            } catch (IOException ex) {
                writeError(writer, ex);
                hasErrors = true;
            }
        }
        writer.println("-- lobMap.size: " + lobMap.sizeAsLong());
        writer.println("-- lobData.size: " + lobData.sizeAsLong());

        if (hasErrors) {
            writer.println("-- lobMap");
            for (Long k : lobMap.keyList()) {
                Object[] value = lobMap.get(k);
                byte[] streamStoreId = (byte[]) value[0];
                writer.println("--     " + k + " " + StreamStore.toString(streamStoreId));
            }
            writer.println("-- lobData");
            for (Long k : lobData.keyList()) {
                writer.println("--     " + k + " len " + lobData.get(k).length);
            }
        }
    }

    private static String getPageType(int type) {
        switch (type) {
        case 0:
            return "free";
        case Page.TYPE_DATA_LEAF:
            return "data leaf";
        case Page.TYPE_DATA_NODE:
            return "data node";
        case Page.TYPE_DATA_OVERFLOW:
            return "data overflow";
        case Page.TYPE_BTREE_LEAF:
            return "btree leaf";
        case Page.TYPE_BTREE_NODE:
            return "btree node";
        case Page.TYPE_FREE_LIST:
            return "free list";
        case Page.TYPE_STREAM_TRUNK:
            return "stream trunk";
        case Page.TYPE_STREAM_DATA:
            return "stream data";
        }
        return "[" + type + "]";
    }

    private void dumpPageStore(PrintWriter writer, long pageCount) {
        Data s = Data.create(this, pageSize);
        for (long page = 3; page < pageCount; page++) {
            s = Data.create(this, pageSize);
            seek(page);
            store.readFully(s.getBytes(), 0, pageSize);
            dumpPage(writer, s, page, pageCount);
        }
    }

    private void dumpPage(PrintWriter writer, Data s, long page, long pageCount) {
        try {
            int type = s.readByte();
            switch (type) {
            case Page.TYPE_EMPTY:
                stat.pageTypeCount[type]++;
                return;
            }
            boolean last = (type & Page.FLAG_LAST) != 0;
            type &= ~Page.FLAG_LAST;
            if (!PageStore.checksumTest(s.getBytes(), (int) page, pageSize)) {
                writeDataError(writer, "checksum mismatch type: " + type, s.getBytes());
            }
            s.readShortInt();
            switch (type) {
            // type 1
            case Page.TYPE_DATA_LEAF: {
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                int columnCount = s.readVarInt();
                int entries = s.readShortInt();
                writer.println("-- page " + page + ": data leaf " +
                        (last ? "(last) " : "") + "parent: " + parentPageId +
                        " table: " + storageId + " entries: " + entries +
                        " columns: " + columnCount);
                dumpPageDataLeaf(writer, s, last, page, columnCount, entries);
                break;
            }
            // type 2
            case Page.TYPE_DATA_NODE: {
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                int rowCount = s.readInt();
                int entries = s.readShortInt();
                writer.println("-- page " + page + ": data node " +
                        (last ? "(last) " : "") + "parent: " + parentPageId +
                        " table: " + storageId + " entries: " + entries +
                        " rowCount: " + rowCount);
                dumpPageDataNode(writer, s, page, entries);
                break;
            }
            // type 3
            case Page.TYPE_DATA_OVERFLOW:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": data overflow " +
                        (last ? "(last) " : ""));
                break;
            // type 4
            case Page.TYPE_BTREE_LEAF: {
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                int entries = s.readShortInt();
                writer.println("-- page " + page + ": b-tree leaf " +
                        (last ? "(last) " : "") + "parent: " + parentPageId +
                        " index: " + storageId + " entries: " + entries);
                if (trace) {
                    dumpPageBtreeLeaf(writer, s, entries, !last);
                }
                break;
            }
            // type 5
            case Page.TYPE_BTREE_NODE:
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                writer.println("-- page " + page + ": b-tree node " +
                        (last ? "(last) " : "") +  "parent: " + parentPageId +
                        " index: " + storageId);
                dumpPageBtreeNode(writer, s, page, !last);
                break;
            // type 6
            case Page.TYPE_FREE_LIST:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": free list " + (last ? "(last)" : ""));
                stat.free += dumpPageFreeList(writer, s, page, pageCount);
                break;
            // type 7
            case Page.TYPE_STREAM_TRUNK:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": log trunk");
                break;
            // type 8
            case Page.TYPE_STREAM_DATA:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": log data");
                break;
            default:
                writer.println("-- ERROR page " + page + " unknown type " + type);
                break;
            }
        } catch (Exception e) {
            writeError(writer, e);
        }
    }

    private void dumpPageLogStream(PrintWriter writer, int logKey,
            int logFirstTrunkPage, int logFirstDataPage, long pageCount)
            throws IOException {
        Data s = Data.create(this, pageSize);
        DataReader in = new DataReader(
                new PageInputStream(writer, this, store, logKey,
                logFirstTrunkPage, logFirstDataPage, pageSize)
        );
        writer.println("---- Transaction log ----");
        CompressLZF compress = new CompressLZF();
        while (true) {
            int x = in.readByte();
            if (x < 0) {
                break;
            }
            if (x == PageLog.NOOP) {
                // ignore
            } else if (x == PageLog.UNDO) {
                int pageId = in.readVarInt();
                int size = in.readVarInt();
                byte[] data = new byte[pageSize];
                if (size == 0) {
                    in.readFully(data, pageSize);
                } else if (size == 1) {
                    // empty
                } else {
                    byte[] compressBuffer = new byte[size];
                    in.readFully(compressBuffer, size);
                    try {
                        compress.expand(compressBuffer, 0, size, data, 0, pageSize);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw DbException.convertToIOException(e);
                    }
                }
                String typeName = "";
                int type = data[0];
                boolean last = (type & Page.FLAG_LAST) != 0;
                type &= ~Page.FLAG_LAST;
                switch (type) {
                case Page.TYPE_EMPTY:
                    typeName = "empty";
                    break;
                case Page.TYPE_DATA_LEAF:
                    typeName = "data leaf " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_DATA_NODE:
                    typeName = "data node " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_DATA_OVERFLOW:
                    typeName = "data overflow " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_BTREE_LEAF:
                    typeName = "b-tree leaf " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_BTREE_NODE:
                    typeName = "b-tree node " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_FREE_LIST:
                    typeName = "free list " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_STREAM_TRUNK:
                    typeName = "log trunk";
                    break;
                case Page.TYPE_STREAM_DATA:
                    typeName = "log data";
                    break;
                default:
                    typeName = "ERROR: unknown type " + type;
                    break;
                }
                writer.println("-- undo page " + pageId + " " + typeName);
                if (trace) {
                    Data d = Data.create(null, data);
                    dumpPage(writer, d, pageId, pageCount);
                }
            } else if (x == PageLog.ADD) {
                int sessionId = in.readVarInt();
                setStorage(in.readVarInt());
                Row row = PageLog.readRow(RowFactory.DEFAULT, in, s);
                writer.println("-- session " + sessionId +
                        " table " + storageId +
                        " + " + row.toString());
                if (transactionLog) {
                    if (storageId == 0 && row.getColumnCount() >= 4) {
                        int tableId = (int) row.getKey();
                        String sql = row.getValue(3).getString();
                        String name = extractTableOrViewName(sql);
                        if (row.getValue(2).getInt() == DbObject.TABLE_OR_VIEW) {
                            tableMap.put(tableId, name);
                        }
                        writer.println(sql + ";");
                    } else {
                        String tableName = tableMap.get(storageId);
                        if (tableName != null) {
                            StatementBuilder buff = new StatementBuilder();
                            buff.append("INSERT INTO ").append(tableName).
                                    append(" VALUES(");
                            for (int i = 0; i < row.getColumnCount(); i++) {
                                buff.appendExceptFirst(", ");
                                buff.append(row.getValue(i).getSQL());
                            }
                            buff.append(");");
                            writer.println(buff.toString());
                        }
                    }
                }
            } else if (x == PageLog.REMOVE) {
                int sessionId = in.readVarInt();
                setStorage(in.readVarInt());
                long key = in.readVarLong();
                writer.println("-- session " + sessionId +
                        " table " + storageId +
                        " - " + key);
                if (transactionLog) {
                    if (storageId == 0) {
                        int tableId = (int) key;
                        String tableName = tableMap.get(tableId);
                        if (tableName != null) {
                            writer.println("DROP TABLE IF EXISTS " + tableName + ";");
                        }
                    } else {
                        String tableName = tableMap.get(storageId);
                        if (tableName != null) {
                            String sql = "DELETE FROM " + tableName +
                                    " WHERE _ROWID_ = " + key + ";";
                            writer.println(sql);
                        }
                    }
                }
            } else if (x == PageLog.TRUNCATE) {
                int sessionId = in.readVarInt();
                setStorage(in.readVarInt());
                writer.println("-- session " + sessionId +
                        " table " + storageId +
                        " truncate");
                if (transactionLog) {
                    writer.println("TRUNCATE TABLE " + storageId);
                }
            } else if (x == PageLog.COMMIT) {
                int sessionId = in.readVarInt();
                writer.println("-- commit " + sessionId);
            } else if (x == PageLog.ROLLBACK) {
                int sessionId = in.readVarInt();
                writer.println("-- rollback " + sessionId);
            } else if (x == PageLog.PREPARE_COMMIT) {
                int sessionId = in.readVarInt();
                String transaction = in.readString();
                writer.println("-- prepare commit " + sessionId + " " + transaction);
            } else if (x == PageLog.NOOP) {
                // nothing to do
            } else if (x == PageLog.CHECKPOINT) {
                writer.println("-- checkpoint");
            } else if (x == PageLog.FREE_LOG) {
                int size = in.readVarInt();
                StringBuilder buff = new StringBuilder("-- free");
                for (int i = 0; i < size; i++) {
                    buff.append(' ').append(in.readVarInt());
                }
                writer.println(buff);
            } else {
                writer.println("-- ERROR: unknown operation " + x);
                break;
            }
        }
    }

    private String setStorage(int storageId) {
        this.storageId = storageId;
        this.storageName = "O_" + String.valueOf(storageId).replace('-', 'M');
        return storageName;
    }

    /**
     * An input stream that reads the data from a page store.
     */
    static class PageInputStream extends InputStream {

        private final PrintWriter writer;
        private final FileStore store;
        private final Data page;
        private final int pageSize;
        private long trunkPage;
        private long nextTrunkPage;
        private long dataPage;
        private final IntArray dataPages = new IntArray();
        private boolean endOfFile;
        private int remaining;
        private int logKey;

        public PageInputStream(PrintWriter writer, DataHandler handler,
                FileStore store, int logKey, long firstTrunkPage,
                long firstDataPage, int pageSize) {
            this.writer = writer;
            this.store = store;
            this.pageSize = pageSize;
            this.logKey = logKey - 1;
            this.nextTrunkPage = firstTrunkPage;
            this.dataPage = firstDataPage;
            page = Data.create(handler, pageSize);
        }

        @Override
        public int read() {
            byte[] b = { 0 };
            int len = read(b);
            return len < 0 ? -1 : (b[0] & 255);
        }

        @Override
        public int read(byte[] b) {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            int read = 0;
            while (len > 0) {
                int r = readBlock(b, off, len);
                if (r < 0) {
                    break;
                }
                read += r;
                off += r;
                len -= r;
            }
            return read == 0 ? -1 : read;
        }

        private int readBlock(byte[] buff, int off, int len) {
            fillBuffer();
            if (endOfFile) {
                return -1;
            }
            int l = Math.min(remaining, len);
            page.read(buff, off, l);
            remaining -= l;
            return l;
        }

        private void fillBuffer() {
            if (remaining > 0 || endOfFile) {
                return;
            }
            while (dataPages.size() == 0) {
                if (nextTrunkPage == 0) {
                    endOfFile = true;
                    return;
                }
                trunkPage = nextTrunkPage;
                store.seek(trunkPage * pageSize);
                store.readFully(page.getBytes(), 0, pageSize);
                page.reset();
                if (!PageStore.checksumTest(page.getBytes(), (int) trunkPage, pageSize)) {
                    writer.println("-- ERROR: checksum mismatch page: " +trunkPage);
                    endOfFile = true;
                    return;
                }
                int t = page.readByte();
                page.readShortInt();
                if (t != Page.TYPE_STREAM_TRUNK) {
                    writer.println("-- log eof " + trunkPage + " type: " + t +
                            " expected type: " + Page.TYPE_STREAM_TRUNK);
                    endOfFile = true;
                    return;
                }
                page.readInt();
                int key = page.readInt();
                logKey++;
                if (key != logKey) {
                    writer.println("-- log eof " + trunkPage +
                            " type: " + t + " expected key: " + logKey + " got: " + key);
                }
                nextTrunkPage = page.readInt();
                writer.println("-- log " + key + ":" + trunkPage +
                        " next: " + nextTrunkPage);
                int pageCount = page.readShortInt();
                for (int i = 0; i < pageCount; i++) {
                    int d = page.readInt();
                    if (dataPage != 0) {
                        if (d == dataPage) {
                            dataPage = 0;
                        } else {
                            // ignore the pages before the starting page
                            continue;
                        }
                    }
                    dataPages.add(d);
                }
            }
            if (dataPages.size() > 0) {
                page.reset();
                long nextPage = dataPages.get(0);
                dataPages.remove(0);
                store.seek(nextPage * pageSize);
                store.readFully(page.getBytes(), 0, pageSize);
                page.reset();
                int t = page.readByte();
                if (t != 0 && !PageStore.checksumTest(page.getBytes(),
                        (int) nextPage, pageSize)) {
                    writer.println("-- ERROR: checksum mismatch page: " +nextPage);
                    endOfFile = true;
                    return;
                }
                page.readShortInt();
                int p = page.readInt();
                int k = page.readInt();
                writer.println("-- log " + k + ":" + trunkPage + "/" + nextPage);
                if (t != Page.TYPE_STREAM_DATA) {
                    writer.println("-- log eof " +nextPage+ " type: " + t + " parent: " + p +
                            " expected type: " + Page.TYPE_STREAM_DATA);
                    endOfFile = true;
                    return;
                } else if (k != logKey) {
                    writer.println("-- log eof " +nextPage+ " type: " + t + " parent: " + p +
                            " expected key: " + logKey + " got: " + k);
                    endOfFile = true;
                    return;
                }
                remaining = pageSize - page.length();
            }
        }
    }

    private void dumpPageBtreeNode(PrintWriter writer, Data s, long pageId,
            boolean positionOnly) {
        int rowCount = s.readInt();
        int entryCount = s.readShortInt();
        int[] children = new int[entryCount + 1];
        int[] offsets = new int[entryCount];
        children[entryCount] = s.readInt();
        checkParent(writer, pageId, children, entryCount);
        int empty = Integer.MAX_VALUE;
        for (int i = 0; i < entryCount; i++) {
            children[i] = s.readInt();
            checkParent(writer, pageId, children, i);
            int off = s.readShortInt();
            empty = Math.min(off, empty);
            offsets[i] = off;
        }
        empty = empty - s.length();
        if (!trace) {
            return;
        }
        writer.println("--   empty: " + empty);
        for (int i = 0; i < entryCount; i++) {
            int off = offsets[i];
            s.setPos(off);
            long key = s.readVarLong();
            Value data;
            if (positionOnly) {
                data = ValueLong.get(key);
            } else {
                try {
                    data = s.readValue();
                } catch (Throwable e) {
                    writeDataError(writer, "exception " + e, s.getBytes());
                    continue;
                }
            }
            writer.println("-- [" + i + "] child: " + children[i] +
                    " key: " + key + " data: " + data);
        }
        writer.println("-- [" + entryCount + "] child: " +
                children[entryCount] + " rowCount: " + rowCount);
    }

    private int dumpPageFreeList(PrintWriter writer, Data s, long pageId,
            long pageCount) {
        int pagesAddressed = PageFreeList.getPagesAddressed(pageSize);
        BitField used = new BitField();
        for (int i = 0; i < pagesAddressed; i += 8) {
            int x = s.readByte() & 255;
            for (int j = 0; j < 8; j++) {
                if ((x & (1 << j)) != 0) {
                    used.set(i + j);
                }
            }
        }
        int free = 0;
        for (long i = 0, j = pageId; i < pagesAddressed && j < pageCount; i++, j++) {
            if (i == 0 || j % 100 == 0) {
                if (i > 0) {
                    writer.println();
                }
                writer.print("-- " + j + " ");
            } else if (j % 20 == 0) {
                writer.print(" - ");
            } else if (j % 10 == 0) {
                writer.print(' ');
            }
            writer.print(used.get((int) i) ? '1' : '0');
            if (!used.get((int) i)) {
                free++;
            }
        }
        writer.println();
        return free;
    }

    private void dumpPageBtreeLeaf(PrintWriter writer, Data s, int entryCount,
            boolean positionOnly) {
        int[] offsets = new int[entryCount];
        int empty = Integer.MAX_VALUE;
        for (int i = 0; i < entryCount; i++) {
            int off = s.readShortInt();
            empty = Math.min(off, empty);
            offsets[i] = off;
        }
        empty = empty - s.length();
        writer.println("--   empty: " + empty);
        for (int i = 0; i < entryCount; i++) {
            int off = offsets[i];
            s.setPos(off);
            long key = s.readVarLong();
            Value data;
            if (positionOnly) {
                data = ValueLong.get(key);
            } else {
                try {
                    data = s.readValue();
                } catch (Throwable e) {
                    writeDataError(writer, "exception " + e, s.getBytes());
                    continue;
                }
            }
            writer.println("-- [" + i + "] key: " + key + " data: " + data);
        }
    }

    private void checkParent(PrintWriter writer, long pageId, int[] children,
            int index) {
        int child = children[index];
        if (child < 0 || child >= parents.length) {
            writer.println("-- ERROR [" + pageId + "] child[" +
                    index + "]: " + child + " >= page count: " + parents.length);
        } else if (parents[child] != pageId) {
            writer.println("-- ERROR [" + pageId + "] child[" +
                    index + "]: " + child + " parent: " + parents[child]);
        }
    }

    private void dumpPageDataNode(PrintWriter writer, Data s, long pageId,
            int entryCount) {
        int[] children = new int[entryCount + 1];
        long[] keys = new long[entryCount];
        children[entryCount] = s.readInt();
        checkParent(writer, pageId, children, entryCount);
        for (int i = 0; i < entryCount; i++) {
            children[i] = s.readInt();
            checkParent(writer, pageId, children, i);
            keys[i] = s.readVarLong();
        }
        if (!trace) {
            return;
        }
        for (int i = 0; i < entryCount; i++) {
            writer.println("-- [" + i + "] child: " + children[i] + " key: " + keys[i]);
        }
        writer.println("-- [" + entryCount + "] child: " + children[entryCount]);
    }

    private void dumpPageDataLeaf(PrintWriter writer, Data s, boolean last,
            long pageId, int columnCount, int entryCount) {
        long[] keys = new long[entryCount];
        int[] offsets = new int[entryCount];
        long next = 0;
        if (!last) {
            next = s.readInt();
            writer.println("--   next: " + next);
        }
        int empty = pageSize;
        for (int i = 0; i < entryCount; i++) {
            keys[i] = s.readVarLong();
            int off = s.readShortInt();
            empty = Math.min(off, empty);
            offsets[i] = off;
        }
        stat.pageDataRows += pageSize - empty;
        empty = empty - s.length();
        stat.pageDataHead += s.length();
        stat.pageDataEmpty += empty;
        if (trace) {
            writer.println("--   empty: " + empty);
        }
        if (!last) {
            Data s2 = Data.create(this, pageSize);
            s.setPos(pageSize);
            long parent = pageId;
            while (true) {
                checkParent(writer, parent, new int[]{(int) next}, 0);
                parent = next;
                seek(next);
                store.readFully(s2.getBytes(), 0, pageSize);
                s2.reset();
                int type = s2.readByte();
                s2.readShortInt();
                s2.readInt();
                if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
                    int size = s2.readShortInt();
                    writer.println("-- chain: " + next +
                            " type: " + type + " size: " + size);
                    s.checkCapacity(size);
                    s.write(s2.getBytes(), s2.length(), size);
                    break;
                } else if (type == Page.TYPE_DATA_OVERFLOW) {
                    next = s2.readInt();
                    if (next == 0) {
                        writeDataError(writer, "next:0", s2.getBytes());
                        break;
                    }
                    int size = pageSize - s2.length();
                    writer.println("-- chain: " + next + " type: " + type +
                            " size: " + size + " next: " + next);
                    s.checkCapacity(size);
                    s.write(s2.getBytes(), s2.length(), size);
                } else {
                    writeDataError(writer, "type: " + type, s2.getBytes());
                    break;
                }
            }
        }
        for (int i = 0; i < entryCount; i++) {
            long key = keys[i];
            int off = offsets[i];
            if (trace) {
                writer.println("-- [" + i + "] storage: " + storageId +
                        " key: " + key + " off: " + off);
            }
            s.setPos(off);
            Value[] data = createRecord(writer, s, columnCount);
            if (data != null) {
                createTemporaryTable(writer);
                writeRow(writer, s, data);
                if (remove && storageId == 0) {
                    String sql = data[3].getString();
                    if (sql.startsWith("CREATE USER ")) {
                        int saltIndex = Utils.indexOf(s.getBytes(), "SALT ".getBytes(), off);
                        if (saltIndex >= 0) {
                            String userName = sql.substring("CREATE USER ".length(),
                                    sql.indexOf("SALT ") - 1);
                            if (userName.startsWith("IF NOT EXISTS ")) {
                                userName = userName.substring("IF NOT EXISTS ".length());
                            }
                            if (userName.startsWith("\"")) {
                                // TODO doesn't work for all cases ("" inside
                                // user name)
                                userName = userName.substring(1, userName.length() - 1);
                            }
                            byte[] userPasswordHash = SHA256.getKeyPasswordHash(
                                    userName, "".toCharArray());
                            byte[] salt = MathUtils.secureRandomBytes(Constants.SALT_LEN);
                            byte[] passwordHash = SHA256.getHashWithSalt(
                                    userPasswordHash, salt);
                            StringBuilder buff = new StringBuilder();
                            buff.append("SALT '").
                                append(StringUtils.convertBytesToHex(salt)).
                                append("' HASH '").
                                append(StringUtils.convertBytesToHex(passwordHash)).
                                append('\'');
                            byte[] replacement = buff.toString().getBytes();
                            System.arraycopy(replacement, 0, s.getBytes(),
                                    saltIndex, replacement.length);
                            seek(pageId);
                            store.write(s.getBytes(), 0, pageSize);
                            if (trace) {
                                out.println("User: " + userName);
                            }
                            remove = false;
                        }
                    }
                }
            }
        }
    }

    private void seek(long page) {
        // page is long to avoid integer overflow
        store.seek(page * pageSize);
    }

    private Value[] createRecord(PrintWriter writer, Data s, int columnCount) {
        recordLength = columnCount;
        if (columnCount <= 0) {
            writeDataError(writer, "columnCount<0", s.getBytes());
            return null;
        }
        Value[] data;
        try {
            data = new Value[columnCount];
        } catch (OutOfMemoryError e) {
            writeDataError(writer, "out of memory", s.getBytes());
            return null;
        }
        return data;
    }

    private void writeRow(PrintWriter writer, Data s, Value[] data) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(storageName).append(" VALUES(");
        for (valueId = 0; valueId < recordLength; valueId++) {
            try {
                Value v = s.readValue();
                data[valueId] = v;
                if (valueId > 0) {
                    sb.append(", ");
                }
                String columnName = storageName + "." + valueId;
                sb.append(getSQL(columnName, v));
            } catch (Exception e) {
                writeDataError(writer, "exception " + e, s.getBytes());
            } catch (OutOfMemoryError e) {
                writeDataError(writer, "out of memory", s.getBytes());
            }
        }
        sb.append(");");
        writer.println(sb.toString());
        if (storageId == 0) {
            try {
                SimpleRow r = new SimpleRow(data);
                MetaRecord meta = new MetaRecord(r);
                schema.add(meta);
                if (meta.getObjectType() == DbObject.TABLE_OR_VIEW) {
                    String sql = data[3].getString();
                    String name = extractTableOrViewName(sql);
                    tableMap.put(meta.getId(), name);
                }
            } catch (Throwable t) {
                writeError(writer, t);
            }
        }
    }

    private void resetSchema() {
        schema = New.arrayList();
        objectIdSet = new HashSet<>();
        tableMap = new HashMap<>();
        columnTypeMap = new HashMap<>();
    }

    private void writeSchema(PrintWriter writer) {
        writer.println("---- Schema ----");
        Collections.sort(schema);
        for (MetaRecord m : schema) {
            if (!isSchemaObjectTypeDelayed(m)) {
                // create, but not referential integrity constraints and so on
                // because they could fail on duplicate keys
                String sql = m.getSQL();
                writer.println(sql + ";");
            }
        }
        // first, copy the lob storage (if there is any)
        // must occur before copying data,
        // otherwise the lob storage may be overwritten
        boolean deleteLobs = false;
        for (Map.Entry<Integer, String> entry : tableMap.entrySet()) {
            Integer objectId = entry.getKey();
            String name = entry.getValue();
            if (objectIdSet.contains(objectId)) {
                if (name.startsWith("INFORMATION_SCHEMA.LOB")) {
                    setStorage(objectId);
                    writer.println("DELETE FROM " + name + ";");
                    writer.println("INSERT INTO " + name + " SELECT * FROM " + storageName + ";");
                    if (name.startsWith("INFORMATION_SCHEMA.LOBS")) {
                        writer.println("UPDATE " + name + " SET TABLE = " +
                                LobStorageFrontend.TABLE_TEMP + ";");
                        deleteLobs = true;
                    }
                }
            }
        }
        for (Map.Entry<Integer, String> entry : tableMap.entrySet()) {
            Integer objectId = entry.getKey();
            String name = entry.getValue();
            if (objectIdSet.contains(objectId)) {
                setStorage(objectId);
                if (name.startsWith("INFORMATION_SCHEMA.LOB")) {
                    continue;
                }
                writer.println("INSERT INTO " + name + " SELECT * FROM " + storageName + ";");
            }
        }
        for (Integer objectId : objectIdSet) {
            setStorage(objectId);
            writer.println("DROP TABLE " + storageName + ";");
        }
        writer.println("DROP ALIAS READ_BLOB;");
        writer.println("DROP ALIAS READ_CLOB;");
        writer.println("DROP ALIAS READ_BLOB_DB;");
        writer.println("DROP ALIAS READ_CLOB_DB;");
        if (deleteLobs) {
            writer.println("DELETE FROM INFORMATION_SCHEMA.LOBS WHERE TABLE = " +
                    LobStorageFrontend.TABLE_TEMP + ";");
        }
        for (MetaRecord m : schema) {
            if (isSchemaObjectTypeDelayed(m)) {
                String sql = m.getSQL();
                writer.println(sql + ";");
            }
        }
    }

    private static boolean isSchemaObjectTypeDelayed(MetaRecord m) {
        switch (m.getObjectType()) {
        case DbObject.INDEX:
        case DbObject.CONSTRAINT:
        case DbObject.TRIGGER:
            return true;
        }
        return false;
    }

    private void createTemporaryTable(PrintWriter writer) {
        if (!objectIdSet.contains(storageId)) {
            objectIdSet.add(storageId);
            StatementBuilder buff = new StatementBuilder("CREATE TABLE ");
            buff.append(storageName).append('(');
            for (int i = 0; i < recordLength; i++) {
                buff.appendExceptFirst(", ");
                buff.append('C').append(i).append(' ');
                String columnType = columnTypeMap.get(storageName + "." + i);
                if (columnType == null) {
                    buff.append("VARCHAR");
                } else {
                    buff.append(columnType);
                }
            }
            writer.println(buff.append(");").toString());
            writer.flush();
        }
    }

    private static String extractTableOrViewName(String sql) {
        int indexTable = sql.indexOf(" TABLE ");
        int indexView = sql.indexOf(" VIEW ");
        if (indexTable > 0 && indexView > 0) {
            if (indexTable < indexView) {
                indexView = -1;
            } else {
                indexTable = -1;
            }
        }
        if (indexView > 0) {
            sql = sql.substring(indexView + " VIEW ".length());
        } else if (indexTable > 0) {
            sql = sql.substring(indexTable + " TABLE ".length());
        } else {
            return "UNKNOWN";
        }
        if (sql.startsWith("IF NOT EXISTS ")) {
            sql = sql.substring("IF NOT EXISTS ".length());
        }
        boolean ignore = false;
        // sql is modified in the loop
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\"') {
                ignore = !ignore;
            } else if (!ignore && (ch <= ' ' || ch == '(')) {
                sql = sql.substring(0, i);
                return sql;
            }
        }
        return "UNKNOWN";
    }


    private static void closeSilently(FileStore fileStore) {
        if (fileStore != null) {
            fileStore.closeSilently();
        }
    }

    private void writeError(PrintWriter writer, Throwable e) {
        if (writer != null) {
            writer.println("// error: " + e);
        }
        traceError("Error", e);
    }

    /**
     * INTERNAL
     */
    @Override
    public String getDatabasePath() {
        return databaseName;
    }

    /**
     * INTERNAL
     */
    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return FileStore.open(this, name, "rw");
    }

    /**
     * INTERNAL
     */
    @Override
    public void checkPowerOff() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void checkWritingAllowed() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public int getMaxLengthInplaceLob() {
        throw DbException.throwInternalError();
    }

    /**
     * INTERNAL
     */
    @Override
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getLobSyncObject() {
        return this;
    }

    /**
     * INTERNAL
     */
    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    /**
     * INTERNAL
     */
    @Override
    public LobStorageBackend getLobStorage() {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff,
            int off, int length) {
        throw DbException.throwInternalError();
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        return null;
    }

    @Override
    public CompareMode getCompareMode() {
        return CompareMode.getInstance(null, 0);
    }
}
