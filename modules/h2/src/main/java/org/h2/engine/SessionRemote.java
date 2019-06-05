/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.command.CommandInterface;
import org.h2.command.CommandRemote;
import org.h2.command.dml.SetTypes;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.ResultInterface;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.fs.FileUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.CompareMode;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * The client side part of a session when using the server mode. This object
 * communicates with a Session on the server side.
 */
public class SessionRemote extends SessionWithState implements DataHandler {

    public static final int SESSION_PREPARE = 0;
    public static final int SESSION_CLOSE = 1;
    public static final int COMMAND_EXECUTE_QUERY = 2;
    public static final int COMMAND_EXECUTE_UPDATE = 3;
    public static final int COMMAND_CLOSE = 4;
    public static final int RESULT_FETCH_ROWS = 5;
    public static final int RESULT_RESET = 6;
    public static final int RESULT_CLOSE = 7;
    public static final int COMMAND_COMMIT = 8;
    public static final int CHANGE_ID = 9;
    public static final int COMMAND_GET_META_DATA = 10;
    public static final int SESSION_PREPARE_READ_PARAMS = 11;
    public static final int SESSION_SET_ID = 12;
    public static final int SESSION_CANCEL_STATEMENT = 13;
    public static final int SESSION_CHECK_KEY = 14;
    public static final int SESSION_SET_AUTOCOMMIT = 15;
    public static final int SESSION_HAS_PENDING_TRANSACTION = 16;
    public static final int LOB_READ = 17;
    public static final int SESSION_PREPARE_READ_PARAMS2 = 18;

    public static final int STATUS_ERROR = 0;
    public static final int STATUS_OK = 1;
    public static final int STATUS_CLOSED = 2;
    public static final int STATUS_OK_STATE_CHANGED = 3;

    private static SessionFactory sessionFactory;

    private TraceSystem traceSystem;
    private Trace trace;
    private ArrayList<Transfer> transferList = New.arrayList();
    private int nextId;
    private boolean autoCommit = true;
    private CommandInterface autoCommitFalse, autoCommitTrue;
    private ConnectionInfo connectionInfo;
    private String databaseName;
    private String cipher;
    private byte[] fileEncryptionKey;
    private final Object lobSyncObject = new Object();
    private String sessionId;
    private int clientVersion;
    private boolean autoReconnect;
    private int lastReconnect;
    private SessionInterface embedded;
    private DatabaseEventListener eventListener;
    private LobStorageFrontend lobStorage;
    private boolean cluster;
    private TempFileDeleter tempFileDeleter;

    private JavaObjectSerializer javaObjectSerializer;
    private volatile boolean javaObjectSerializerInitialized;

    private final CompareMode compareMode = CompareMode.getInstance(null, 0);

    public SessionRemote(ConnectionInfo ci) {
        this.connectionInfo = ci;
    }

    @Override
    public ArrayList<String> getClusterServers() {
        ArrayList<String> serverList = new ArrayList<>();
        for (Transfer transfer : transferList) {
            serverList.add(transfer.getSocket().getInetAddress().
                    getHostAddress() + ":" +
                    transfer.getSocket().getPort());
        }
        return serverList;
    }

    private Transfer initTransfer(ConnectionInfo ci, String db, String server)
            throws IOException {
        Socket socket = NetUtils.createSocket(server,
                Constants.DEFAULT_TCP_PORT, ci.isSSL());
        Transfer trans = new Transfer(this, socket);
        trans.setSSL(ci.isSSL());
        trans.init();
        trans.writeInt(Constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED);
        trans.writeInt(Constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED);
        trans.writeString(db);
        trans.writeString(ci.getOriginalURL());
        trans.writeString(ci.getUserName());
        trans.writeBytes(ci.getUserPasswordHash());
        trans.writeBytes(ci.getFilePasswordHash());
        String[] keys = ci.getKeys();
        trans.writeInt(keys.length);
        for (String key : keys) {
            trans.writeString(key).writeString(ci.getProperty(key));
        }
        try {
            done(trans);
            clientVersion = trans.readInt();
            trans.setVersion(clientVersion);
            if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_14) {
                if (ci.getFileEncryptionKey() != null) {
                    trans.writeBytes(ci.getFileEncryptionKey());
                }
            }
            trans.writeInt(SessionRemote.SESSION_SET_ID);
            trans.writeString(sessionId);
            done(trans);
            if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_15) {
                autoCommit = trans.readBoolean();
            } else {
                autoCommit = true;
            }
            return trans;
        } catch (DbException e) {
            trans.close();
            throw e;
        }
    }

    @Override
    public boolean hasPendingTransaction() {
        if (clientVersion < Constants.TCP_PROTOCOL_VERSION_10) {
            return true;
        }
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            Transfer transfer = transferList.get(i);
            try {
                traceOperation("SESSION_HAS_PENDING_TRANSACTION", 0);
                transfer.writeInt(
                        SessionRemote.SESSION_HAS_PENDING_TRANSACTION);
                done(transfer);
                return transfer.readInt() != 0;
            } catch (IOException e) {
                removeServer(e, i--, ++count);
            }
        }
        return true;
    }

    @Override
    public void cancel() {
        // this method is called when closing the connection
        // the statement that is currently running is not canceled in this case
        // however Statement.cancel is supported
    }

    /**
     * Cancel the statement with the given id.
     *
     * @param id the statement id
     */
    public void cancelStatement(int id) {
        for (Transfer transfer : transferList) {
            try {
                Transfer trans = transfer.openNewConnection();
                trans.init();
                trans.writeInt(clientVersion);
                trans.writeInt(clientVersion);
                trans.writeString(null);
                trans.writeString(null);
                trans.writeString(sessionId);
                trans.writeInt(SessionRemote.SESSION_CANCEL_STATEMENT);
                trans.writeInt(id);
                trans.close();
            } catch (IOException e) {
                trace.debug(e, "could not cancel statement");
            }
        }
    }

    private void checkClusterDisableAutoCommit(String serverList) {
        if (autoCommit && transferList.size() > 1) {
            setAutoCommitSend(false);
            CommandInterface c = prepareCommand(
                    "SET CLUSTER " + serverList, Integer.MAX_VALUE);
            // this will set autoCommit to false
            c.executeUpdate(false);
            // so we need to switch it on
            autoCommit = true;
            cluster = true;
        }
    }

    public int getClientVersion() {
        return clientVersion;
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        if (!cluster) {
            setAutoCommitSend(autoCommit);
        }
        this.autoCommit = autoCommit;
    }

    public void setAutoCommitFromServer(boolean autoCommit) {
        if (cluster) {
            if (autoCommit) {
                // the user executed SET AUTOCOMMIT TRUE
                setAutoCommitSend(false);
                this.autoCommit = true;
            }
        } else {
            this.autoCommit = autoCommit;
        }
    }

    private synchronized void setAutoCommitSend(boolean autoCommit) {
        if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_8) {
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                Transfer transfer = transferList.get(i);
                try {
                    traceOperation("SESSION_SET_AUTOCOMMIT", autoCommit ? 1 : 0);
                    transfer.writeInt(SessionRemote.SESSION_SET_AUTOCOMMIT).
                            writeBoolean(autoCommit);
                    done(transfer);
                } catch (IOException e) {
                    removeServer(e, i--, ++count);
                }
            }
        } else {
            if (autoCommit) {
                if (autoCommitTrue == null) {
                    autoCommitTrue = prepareCommand(
                            "SET AUTOCOMMIT TRUE", Integer.MAX_VALUE);
                }
                autoCommitTrue.executeUpdate(false);
            } else {
                if (autoCommitFalse == null) {
                    autoCommitFalse = prepareCommand(
                            "SET AUTOCOMMIT FALSE", Integer.MAX_VALUE);
                }
                autoCommitFalse.executeUpdate(false);
            }
        }
    }

    /**
     * Calls COMMIT if the session is in cluster mode.
     */
    public void autoCommitIfCluster() {
        if (autoCommit && cluster) {
            // server side auto commit is off because of race conditions
            // (update set id=1 where id=0, but update set id=2 where id=0 is
            // faster)
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                Transfer transfer = transferList.get(i);
                try {
                    traceOperation("COMMAND_COMMIT", 0);
                    transfer.writeInt(SessionRemote.COMMAND_COMMIT);
                    done(transfer);
                } catch (IOException e) {
                    removeServer(e, i--, ++count);
                }
            }
        }
    }

    private String getFilePrefix(String dir) {
        StringBuilder buff = new StringBuilder(dir);
        buff.append('/');
        for (int i = 0; i < databaseName.length(); i++) {
            char ch = databaseName.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                buff.append(ch);
            } else {
                buff.append('_');
            }
        }
        return buff.toString();
    }

    @Override
    public int getPowerOffCount() {
        return 0;
    }

    @Override
    public void setPowerOffCount(int count) {
        throw DbException.getUnsupportedException("remote");
    }

    /**
     * Open a new (remote or embedded) session.
     *
     * @param openNew whether to open a new session in any case
     * @return the session
     */
    public SessionInterface connectEmbeddedOrServer(boolean openNew) {
        ConnectionInfo ci = connectionInfo;
        if (ci.isRemote()) {
            connectServer(ci);
            return this;
        }
        // create the session using reflection,
        // so that the JDBC layer can be compiled without it
        boolean autoServerMode = ci.getProperty("AUTO_SERVER", false);
        ConnectionInfo backup = null;
        try {
            if (autoServerMode) {
                backup = ci.clone();
                connectionInfo = ci.clone();
            }
            if (openNew) {
                ci.setProperty("OPEN_NEW", "true");
            }
            if (sessionFactory == null) {
                sessionFactory = (SessionFactory) Class.forName(
                        "org.h2.engine.Engine").getMethod("getInstance").invoke(null);
            }
            return sessionFactory.createSession(ci);
        } catch (Exception re) {
            DbException e = DbException.convert(re);
            if (e.getErrorCode() == ErrorCode.DATABASE_ALREADY_OPEN_1) {
                if (autoServerMode) {
                    String serverKey = ((JdbcSQLException) e.getSQLException()).
                            getSQL();
                    if (serverKey != null) {
                        backup.setServerKey(serverKey);
                        // OPEN_NEW must be removed now, otherwise
                        // opening a session with AUTO_SERVER fails
                        // if another connection is already open
                        backup.removeProperty("OPEN_NEW", null);
                        connectServer(backup);
                        return this;
                    }
                }
            }
            throw e;
        }
    }

    private void connectServer(ConnectionInfo ci) {
        String name = ci.getName();
        if (name.startsWith("//")) {
            name = name.substring("//".length());
        }
        int idx = name.indexOf('/');
        if (idx < 0) {
            throw ci.getFormatException();
        }
        databaseName = name.substring(idx + 1);
        String server = name.substring(0, idx);
        traceSystem = new TraceSystem(null);
        String traceLevelFile = ci.getProperty(
                SetTypes.TRACE_LEVEL_FILE, null);
        if (traceLevelFile != null) {
            int level = Integer.parseInt(traceLevelFile);
            String prefix = getFilePrefix(
                    SysProperties.CLIENT_TRACE_DIRECTORY);
            try {
                traceSystem.setLevelFile(level);
                if (level > 0 && level < 4) {
                    String file = FileUtils.createTempFile(prefix,
                            Constants.SUFFIX_TRACE_FILE, false, false);
                    traceSystem.setFileName(file);
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, prefix);
            }
        }
        String traceLevelSystemOut = ci.getProperty(
                SetTypes.TRACE_LEVEL_SYSTEM_OUT, null);
        if (traceLevelSystemOut != null) {
            int level = Integer.parseInt(traceLevelSystemOut);
            traceSystem.setLevelSystemOut(level);
        }
        trace = traceSystem.getTrace(Trace.JDBC);
        String serverList = null;
        if (server.indexOf(',') >= 0) {
            serverList = StringUtils.quoteStringSQL(server);
            ci.setProperty("CLUSTER", Constants.CLUSTERING_ENABLED);
        }
        autoReconnect = ci.getProperty("AUTO_RECONNECT", false);
        // AUTO_SERVER implies AUTO_RECONNECT
        boolean autoServer = ci.getProperty("AUTO_SERVER", false);
        if (autoServer && serverList != null) {
            throw DbException
                    .getUnsupportedException("autoServer && serverList != null");
        }
        autoReconnect |= autoServer;
        if (autoReconnect) {
            String className = ci.getProperty("DATABASE_EVENT_LISTENER");
            if (className != null) {
                className = StringUtils.trim(className, true, true, "'");
                try {
                    eventListener = (DatabaseEventListener) JdbcUtils
                            .loadUserClass(className).newInstance();
                } catch (Throwable e) {
                    throw DbException.convert(e);
                }
            }
        }
        cipher = ci.getProperty("CIPHER");
        if (cipher != null) {
            fileEncryptionKey = MathUtils.secureRandomBytes(32);
        }
        String[] servers = StringUtils.arraySplit(server, ',', true);
        int len = servers.length;
        transferList.clear();
        sessionId = StringUtils.convertBytesToHex(MathUtils.secureRandomBytes(32));
        // TODO cluster: support more than 2 connections
        boolean switchOffCluster = false;
        try {
            for (String s : servers) {
                try {
                    Transfer trans = initTransfer(ci, databaseName, s);
                    transferList.add(trans);
                } catch (IOException e) {
                    if (len == 1) {
                        throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, e + ": " + s);
                    }
                    switchOffCluster = true;
                }
            }
            checkClosed();
            if (switchOffCluster) {
                switchOffCluster();
            }
            checkClusterDisableAutoCommit(serverList);
        } catch (DbException e) {
            traceSystem.close();
            throw e;
        }
    }

    private void switchOffCluster() {
        CommandInterface ci = prepareCommand("SET CLUSTER ''", Integer.MAX_VALUE);
        ci.executeUpdate(false);
    }

    /**
     * Remove a server from the list of cluster nodes and disables the cluster
     * mode.
     *
     * @param e the exception (used for debugging)
     * @param i the index of the server to remove
     * @param count the retry count index
     */
    public void removeServer(IOException e, int i, int count) {
        trace.debug(e, "removing server because of exception");
        transferList.remove(i);
        if (transferList.isEmpty() && autoReconnect(count)) {
            return;
        }
        checkClosed();
        switchOffCluster();
    }

    @Override
    public synchronized CommandInterface prepareCommand(String sql, int fetchSize) {
        checkClosed();
        return new CommandRemote(this, transferList, sql, fetchSize);
    }

    /**
     * Automatically re-connect if necessary and if configured to do so.
     *
     * @param count the retry count index
     * @return true if reconnected
     */
    private boolean autoReconnect(int count) {
        if (!isClosed()) {
            return false;
        }
        if (!autoReconnect) {
            return false;
        }
        if (!cluster && !autoCommit) {
            return false;
        }
        if (count > SysProperties.MAX_RECONNECT) {
            return false;
        }
        lastReconnect++;
        while (true) {
            try {
                embedded = connectEmbeddedOrServer(false);
                break;
            } catch (DbException e) {
                if (e.getErrorCode() != ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE) {
                    throw e;
                }
                // exclusive mode: re-try endlessly
                try {
                    Thread.sleep(500);
                } catch (Exception e2) {
                    // ignore
                }
            }
        }
        if (embedded == this) {
            // connected to a server somewhere else
            embedded = null;
        } else {
            // opened an embedded connection now -
            // must connect to this database in server mode
            // unfortunately
            connectEmbeddedOrServer(true);
        }
        recreateSessionState();
        if (eventListener != null) {
            eventListener.setProgress(DatabaseEventListener.STATE_RECONNECTED,
                    databaseName, count, SysProperties.MAX_RECONNECT);
        }
        return true;
    }

    /**
     * Check if this session is closed and throws an exception if so.
     *
     * @throws DbException if the session is closed
     */
    public void checkClosed() {
        if (isClosed()) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
    }

    @Override
    public void close() {
        RuntimeException closeError = null;
        if (transferList != null) {
            synchronized (this) {
                for (Transfer transfer : transferList) {
                    try {
                        traceOperation("SESSION_CLOSE", 0);
                        transfer.writeInt(SessionRemote.SESSION_CLOSE);
                        done(transfer);
                        transfer.close();
                    } catch (RuntimeException e) {
                        trace.error(e, "close");
                        closeError = e;
                    } catch (Exception e) {
                        trace.error(e, "close");
                    }
                }
            }
            transferList = null;
        }
        traceSystem.close();
        if (embedded != null) {
            embedded.close();
            embedded = null;
        }
        if (closeError != null) {
            throw closeError;
        }
    }

    @Override
    public Trace getTrace() {
        return traceSystem.getTrace(Trace.JDBC);
    }

    public int getNextId() {
        return nextId++;
    }

    public int getCurrentId() {
        return nextId;
    }

    /**
     * Called to flush the output after data has been sent to the server and
     * just before receiving data. This method also reads the status code from
     * the server and throws any exception the server sent.
     *
     * @param transfer the transfer object
     * @throws DbException if the server sent an exception
     * @throws IOException if there is a communication problem between client
     *             and server
     */
    public void done(Transfer transfer) throws IOException {
        transfer.flush();
        int status = transfer.readInt();
        if (status == STATUS_ERROR) {
            String sqlstate = transfer.readString();
            String message = transfer.readString();
            String sql = transfer.readString();
            int errorCode = transfer.readInt();
            String stackTrace = transfer.readString();
            JdbcSQLException s = new JdbcSQLException(message, sql, sqlstate,
                    errorCode, null, stackTrace);
            if (errorCode == ErrorCode.CONNECTION_BROKEN_1) {
                // allow re-connect
                throw new IOException(s.toString(), s);
            }
            throw DbException.convert(s);
        } else if (status == STATUS_CLOSED) {
            transferList = null;
        } else if (status == STATUS_OK_STATE_CHANGED) {
            sessionStateChanged = true;
        } else if (status == STATUS_OK) {
            // ok
        } else {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "unexpected status " + status);
        }
    }

    /**
     * Returns true if the connection was opened in cluster mode.
     *
     * @return true if it is
     */
    public boolean isClustered() {
        return cluster;
    }

    @Override
    public boolean isClosed() {
        return transferList == null || transferList.isEmpty();
    }

    /**
     * Write the operation to the trace system if debug trace is enabled.
     *
     * @param operation the operation performed
     * @param id the id of the operation
     */
    public void traceOperation(String operation, int id) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} {1}", operation, id);
        }
    }

    @Override
    public void checkPowerOff() {
        // ok
    }

    @Override
    public void checkWritingAllowed() {
        // ok
    }

    @Override
    public String getDatabasePath() {
        return "";
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return SysProperties.LOB_CLIENT_MAX_SIZE_MEMORY;
    }

    @Override
    public FileStore openFile(String name, String mode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStore store;
        if (cipher == null) {
            store = FileStore.open(this, name, mode);
        } else {
            store = FileStore.open(this, name, mode, cipher, fileEncryptionKey, 0);
        }
        store.setCheckedWriting(false);
        try {
            store.init();
        } catch (DbException e) {
            store.closeSilently();
            throw e;
        }
        return store;
    }

    @Override
    public DataHandler getDataHandler() {
        return this;
    }

    @Override
    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    public int getLastReconnect() {
        return lastReconnect;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        if (tempFileDeleter == null) {
            tempFileDeleter = TempFileDeleter.getInstance();
        }
        return tempFileDeleter;
    }

    @Override
    public boolean isReconnectNeeded(boolean write) {
        return false;
    }

    @Override
    public SessionInterface reconnect(boolean write) {
        return this;
    }

    @Override
    public void afterWriting() {
        // nothing to do
    }

    @Override
    public LobStorageInterface getLobStorage() {
        if (lobStorage == null) {
            lobStorage = new LobStorageFrontend(this);
        }
        return lobStorage;
    }

    @Override
    public synchronized int readLob(long lobId, byte[] hmac, long offset,
            byte[] buff, int off, int length) {
        checkClosed();
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            Transfer transfer = transferList.get(i);
            try {
                traceOperation("LOB_READ", (int) lobId);
                transfer.writeInt(SessionRemote.LOB_READ);
                transfer.writeLong(lobId);
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_12) {
                    transfer.writeBytes(hmac);
                }
                transfer.writeLong(offset);
                transfer.writeInt(length);
                done(transfer);
                length = transfer.readInt();
                if (length <= 0) {
                    return length;
                }
                transfer.readBytes(buff, off, length);
                return length;
            } catch (IOException e) {
                removeServer(e, i--, ++count);
            }
        }
        return 1;
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        initJavaObjectSerializer();
        return javaObjectSerializer;
    }

    private void initJavaObjectSerializer() {
        if (javaObjectSerializerInitialized) {
            return;
        }
        synchronized (this) {
            if (javaObjectSerializerInitialized) {
                return;
            }
            String serializerFQN = readSerializationSettings();
            if (serializerFQN != null) {
                serializerFQN = serializerFQN.trim();
                if (!serializerFQN.isEmpty() && !serializerFQN.equals("null")) {
                    try {
                        javaObjectSerializer = (JavaObjectSerializer) JdbcUtils
                                .loadUserClass(serializerFQN).newInstance();
                    } catch (Exception e) {
                        throw DbException.convert(e);
                    }
                }
            }
            javaObjectSerializerInitialized = true;
        }
    }

    /**
     * Read the serializer name from the persistent database settings.
     *
     * @return the serializer
     */
    private String readSerializationSettings() {
        String javaObjectSerializerFQN = null;
        CommandInterface ci = prepareCommand(
                "SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS "+
                " WHERE NAME='JAVA_OBJECT_SERIALIZER'", Integer.MAX_VALUE);
        try {
            ResultInterface result = ci.executeQuery(0, false);
            if (result.next()) {
                Value[] row = result.currentRow();
                javaObjectSerializerFQN = row[0].getString();
            }
        } finally {
            ci.close();
        }
        return javaObjectSerializerFQN;
    }

    @Override
    public void addTemporaryLob(Value v) {
        // do nothing
    }

    @Override
    public CompareMode getCompareMode() {
        return compareMode;
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public String getCurrentSchemaName() {
        throw DbException.getUnsupportedException("getSchema && remote session");
    }

    @Override
    public void setCurrentSchemaName(String schema) {
        throw DbException.getUnsupportedException("setSchema && remote session");
    }

    @Override
    public boolean isSupportsGeneratedKeys() {
        return getClientVersion() >= Constants.TCP_PROTOCOL_VERSION_17;
    }

}
