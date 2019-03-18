/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jmx;

import java.lang.management.ManagementFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.h2.command.Command;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.store.PageStore;
import org.h2.table.Table;

/**
 * The MBean implementation.
 *
 * @author Eric Dong
 * @author Thomas Mueller
 */
public class DatabaseInfo implements DatabaseInfoMBean {

    private static final Map<String, ObjectName> MBEANS = new HashMap<>();

    /** Database. */
    private final Database database;

    private DatabaseInfo(Database database) {
        if (database == null) {
            throw new IllegalArgumentException("Argument 'database' must not be null");
        }
        this.database = database;
    }

    /**
     * Returns a JMX new ObjectName instance.
     *
     * @param name name of the MBean
     * @param path the path
     * @return a new ObjectName instance
     * @throws JMException if the ObjectName could not be created
     */
    private static ObjectName getObjectName(String name, String path)
            throws JMException {
        name = name.replace(':', '_');
        path = path.replace(':', '_');
        Hashtable<String, String> map = new Hashtable<>();
        map.put("name", name);
        map.put("path", path);
        return new ObjectName("org.h2", map);
    }

    /**
     * Registers an MBean for the database.
     *
     * @param connectionInfo connection info
     * @param database database
     */
    public static void registerMBean(ConnectionInfo connectionInfo,
            Database database) throws JMException {
        String path = connectionInfo.getName();
        if (!MBEANS.containsKey(path)) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            String name = database.getShortName();
            ObjectName mbeanObjectName = getObjectName(name, path);
            MBEANS.put(path, mbeanObjectName);
            DatabaseInfo info = new DatabaseInfo(database);
            Object mbean = new DocumentedMBean(info, DatabaseInfoMBean.class);
            mbeanServer.registerMBean(mbean, mbeanObjectName);
        }
    }

    /**
     * Unregisters the MBean for the database if one is registered.
     *
     * @param name database name
     */
    public static void unregisterMBean(String name) throws Exception {
        ObjectName mbeanObjectName = MBEANS.remove(name);
        if (mbeanObjectName != null) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            mbeanServer.unregisterMBean(mbeanObjectName);
        }
    }

    @Override
    public boolean isExclusive() {
        return database.getExclusiveSession() != null;
    }

    @Override
    public boolean isReadOnly() {
        return database.isReadOnly();
    }

    @Override
    public String getMode() {
        return database.getMode().getName();
    }

    @Override
    public boolean isMultiThreaded() {
        return database.isMultiThreaded();
    }

    @Override
    public boolean isMvcc() {
        return database.isMultiVersion();
    }

    @Override
    public int getLogMode() {
        return database.getLogMode();
    }

    @Override
    public void setLogMode(int value) {
        database.setLogMode(value);
    }

    @Override
    public int getTraceLevel() {
        return database.getTraceSystem().getLevelFile();
    }

    @Override
    public void setTraceLevel(int level) {
        database.getTraceSystem().setLevelFile(level);
    }

    @Override
    public long getFileWriteCountTotal() {
        if (!database.isPersistent()) {
            return 0;
        }
        PageStore p = database.getPageStore();
        if (p != null) {
            return p.getWriteCountTotal();
        }
        // TODO remove this method when removing the page store
        // (the MVStore doesn't support it)
        return 0;
    }

    @Override
    public long getFileWriteCount() {
        if (!database.isPersistent()) {
            return 0;
        }
        PageStore p = database.getPageStore();
        if (p != null) {
            return p.getWriteCount();
        }
        return database.getMvStore().getStore().getFileStore().getReadCount();
    }

    @Override
    public long getFileReadCount() {
        if (!database.isPersistent()) {
            return 0;
        }
        PageStore p = database.getPageStore();
        if (p != null) {
            return p.getReadCount();
        }
        return database.getMvStore().getStore().getFileStore().getReadCount();
    }

    @Override
    public long getFileSize() {
        if (!database.isPersistent()) {
            return 0;
        }
        PageStore p = database.getPageStore();
        if (p != null) {
            return p.getPageCount() * p.getPageSize() / 1024;
        }
        return database.getMvStore().getStore().getFileStore().size();
    }

    @Override
    public int getCacheSizeMax() {
        if (!database.isPersistent()) {
            return 0;
        }
        PageStore p = database.getPageStore();
        if (p != null) {
            return p.getCache().getMaxMemory();
        }
        return database.getMvStore().getStore().getCacheSize() * 1024;
    }

    @Override
    public void setCacheSizeMax(int kb) {
        if (database.isPersistent()) {
            database.setCacheSize(kb);
        }
    }

    @Override
    public int getCacheSize() {
        if (!database.isPersistent()) {
            return 0;
        }
        PageStore p = database.getPageStore();
        if (p != null) {
            return p.getCache().getMemory();
        }
        return database.getMvStore().getStore().getCacheSizeUsed() * 1024;
    }

    @Override
    public String getVersion() {
        return Constants.getFullVersion();
    }

    @Override
    public String listSettings() {
        StringBuilder buff = new StringBuilder();
        for (Map.Entry<String, String> e :
                new TreeMap<>(
                database.getSettings().getSettings()).entrySet()) {
            buff.append(e.getKey()).append(" = ").append(e.getValue()).append('\n');
        }
        return buff.toString();
    }

    @Override
    public String listSessions() {
        StringBuilder buff = new StringBuilder();
        for (Session session : database.getSessions(false)) {
            buff.append("session id: ").append(session.getId());
            buff.append(" user: ").
                    append(session.getUser().getName()).
                    append('\n');
            buff.append("connected: ").
                    append(new Timestamp(session.getSessionStart())).
                    append('\n');
            Command command = session.getCurrentCommand();
            if (command != null) {
                buff.append("statement: ").
                        append(session.getCurrentCommand()).
                        append('\n');
                long commandStart = session.getCurrentCommandStart();
                if (commandStart != 0) {
                    buff.append("started: ").append(
                            new Timestamp(commandStart)).
                            append('\n');
                }
            }
            Table[] t = session.getLocks();
            if (t.length > 0) {
                for (Table table : session.getLocks()) {
                    if (table.isLockedExclusivelyBy(session)) {
                        buff.append("write lock on ");
                    } else {
                        buff.append("read lock on ");
                    }
                    buff.append(table.getSchema().getName()).
                            append('.').append(table.getName()).
                            append('\n');
                }
            }
            buff.append('\n');
        }
        return buff.toString();
    }

}
