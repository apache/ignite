/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import javax.naming.Context;
import javax.sql.DataSource;
import org.h2.api.CustomDataTypesHandler;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.util.Utils.ClassFactory;

/**
 * This is a utility class with JDBC helper functions.
 */
public class JdbcUtils {

    /**
     * The serializer to use.
     */
    public static JavaObjectSerializer serializer;

    /**
     * Custom data types handler to use.
     */
    public static CustomDataTypesHandler customDataTypesHandler;

    private static final String[] DRIVERS = {
        "h2:", "org.h2.Driver",
        "Cache:", "com.intersys.jdbc.CacheDriver",
        "daffodilDB://", "in.co.daffodil.db.rmi.RmiDaffodilDBDriver",
        "daffodil", "in.co.daffodil.db.jdbc.DaffodilDBDriver",
        "db2:", "com.ibm.db2.jcc.DB2Driver",
        "derby:net:", "org.apache.derby.jdbc.ClientDriver",
        "derby://", "org.apache.derby.jdbc.ClientDriver",
        "derby:", "org.apache.derby.jdbc.EmbeddedDriver",
        "FrontBase:", "com.frontbase.jdbc.FBJDriver",
        "firebirdsql:", "org.firebirdsql.jdbc.FBDriver",
        "hsqldb:", "org.hsqldb.jdbcDriver",
        "informix-sqli:", "com.informix.jdbc.IfxDriver",
        "jtds:", "net.sourceforge.jtds.jdbc.Driver",
        "microsoft:", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "mimer:", "com.mimer.jdbc.Driver",
        "mysql:", "com.mysql.jdbc.Driver",
        "odbc:", "sun.jdbc.odbc.JdbcOdbcDriver",
        "oracle:", "oracle.jdbc.driver.OracleDriver",
        "pervasive:", "com.pervasive.jdbc.v2.Driver",
        "pointbase:micro:", "com.pointbase.me.jdbc.jdbcDriver",
        "pointbase:", "com.pointbase.jdbc.jdbcUniversalDriver",
        "postgresql:", "org.postgresql.Driver",
        "sybase:", "com.sybase.jdbc3.jdbc.SybDriver",
        "sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "teradata:", "com.ncr.teradata.TeraDriver",
    };

    private static boolean allowAllClasses;
    private static HashSet<String> allowedClassNames;

    /**
     *  In order to manage more than one class loader
     */
    private static ArrayList<ClassFactory> userClassFactories =
            new ArrayList<>();

    private static String[] allowedClassNamePrefixes;

    private JdbcUtils() {
        // utility class
    }

    /**
     * Add a class factory in order to manage more than one class loader.
     *
     * @param classFactory An object that implements ClassFactory
     */
    public static void addClassFactory(ClassFactory classFactory) {
        getUserClassFactories().add(classFactory);
    }

    /**
     * Remove a class factory
     *
     * @param classFactory Already inserted class factory instance
     */
    public static void removeClassFactory(ClassFactory classFactory) {
        getUserClassFactories().remove(classFactory);
    }

    private static ArrayList<ClassFactory> getUserClassFactories() {
        if (userClassFactories == null) {
            // initially, it is empty
            // but Apache Tomcat may clear the fields as well
            userClassFactories = new ArrayList<>();
        }
        return userClassFactories;
    }

    static {
        String clazz = SysProperties.JAVA_OBJECT_SERIALIZER;
        if (clazz != null) {
            try {
                serializer = (JavaObjectSerializer) loadUserClass(clazz).newInstance();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }

        String customTypeHandlerClass = SysProperties.CUSTOM_DATA_TYPES_HANDLER;
        if (customTypeHandlerClass != null) {
            try {
                customDataTypesHandler = (CustomDataTypesHandler)
                        loadUserClass(customTypeHandlerClass).newInstance();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    /**
     * Load a class, but check if it is allowed to load this class first. To
     * perform access rights checking, the system property h2.allowedClasses
     * needs to be set to a list of class file name prefixes.
     *
     * @param className the name of the class
     * @return the class object
     */
    @SuppressWarnings("unchecked")
    public static <Z> Class<Z> loadUserClass(String className) {
        if (allowedClassNames == null) {
            // initialize the static fields
            String s = SysProperties.ALLOWED_CLASSES;
            ArrayList<String> prefixes = New.arrayList();
            boolean allowAll = false;
            HashSet<String> classNames = new HashSet<>();
            for (String p : StringUtils.arraySplit(s, ',', true)) {
                if (p.equals("*")) {
                    allowAll = true;
                } else if (p.endsWith("*")) {
                    prefixes.add(p.substring(0, p.length() - 1));
                } else {
                    classNames.add(p);
                }
            }
            allowedClassNamePrefixes = prefixes.toArray(new String[0]);
            allowAllClasses = allowAll;
            allowedClassNames = classNames;
        }
        if (!allowAllClasses && !allowedClassNames.contains(className)) {
            boolean allowed = false;
            for (String s : allowedClassNamePrefixes) {
                if (className.startsWith(s)) {
                    allowed = true;
                }
            }
            if (!allowed) {
                throw DbException.get(
                        ErrorCode.ACCESS_DENIED_TO_CLASS_1, className);
            }
        }
        // Use provided class factory first.
        for (ClassFactory classFactory : getUserClassFactories()) {
            if (classFactory.match(className)) {
                try {
                    Class<?> userClass = classFactory.loadClass(className);
                    if (!(userClass == null)) {
                        return (Class<Z>) userClass;
                    }
                } catch (Exception e) {
                    throw DbException.get(
                            ErrorCode.CLASS_NOT_FOUND_1, e, className);
                }
            }
        }
        // Use local ClassLoader
        try {
            return (Class<Z>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            try {
                return (Class<Z>) Class.forName(
                        className, true,
                        Thread.currentThread().getContextClassLoader());
            } catch (Exception e2) {
                throw DbException.get(
                        ErrorCode.CLASS_NOT_FOUND_1, e, className);
            }
        } catch (NoClassDefFoundError e) {
            throw DbException.get(
                    ErrorCode.CLASS_NOT_FOUND_1, e, className);
        } catch (Error e) {
            // UnsupportedClassVersionError
            throw DbException.get(
                    ErrorCode.GENERAL_ERROR_1, e, className);
        }
    }

    /**
     * Close a statement without throwing an exception.
     *
     * @param stat the statement or null
     */
    public static void closeSilently(Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a connection without throwing an exception.
     *
     * @param conn the connection or null
     */
    public static void closeSilently(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a result set without throwing an exception.
     *
     * @param rs the result set or null
     */
    public static void closeSilently(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url,
            String user, String password) throws SQLException {
        Properties prop = new Properties();
        if (user != null) {
            prop.setProperty("user", user);
        }
        if (password != null) {
            prop.setProperty("password", password);
        }
        return getConnection(driver, url, prop);
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param prop the properties containing at least the user name and password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url,
            Properties prop) throws SQLException {
        if (StringUtils.isNullOrEmpty(driver)) {
            JdbcUtils.load(url);
        } else {
            Class<?> d = loadUserClass(driver);
            if (java.sql.Driver.class.isAssignableFrom(d)) {
                try {
                    Driver driverInstance = (Driver) d.newInstance();
                    return driverInstance.connect(url, prop); /*fix issue #695 with drivers with the same
                    jdbc subprotocol in classpath of jdbc drivers (as example redshift and postgresql drivers)*/
                } catch (Exception e) {
                    throw DbException.toSQLException(e);
                }
            } else if (javax.naming.Context.class.isAssignableFrom(d)) {
                // JNDI context
                try {
                    Context context = (Context) d.newInstance();
                    DataSource ds = (DataSource) context.lookup(url);
                    String user = prop.getProperty("user");
                    String password = prop.getProperty("password");
                    if (StringUtils.isNullOrEmpty(user) && StringUtils.isNullOrEmpty(password)) {
                        return ds.getConnection();
                    }
                    return ds.getConnection(user, password);
                } catch (Exception e) {
                    throw DbException.toSQLException(e);
                }
            } else {
                // don't know, but maybe it loaded a JDBC Driver
                return DriverManager.getConnection(url, prop);
            }
        }
        return DriverManager.getConnection(url, prop);
    }

    /**
     * Get the driver class name for the given URL, or null if the URL is
     * unknown.
     *
     * @param url the database URL
     * @return the driver class name
     */
    public static String getDriver(String url) {
        if (url.startsWith("jdbc:")) {
            url = url.substring("jdbc:".length());
            for (int i = 0; i < DRIVERS.length; i += 2) {
                String prefix = DRIVERS[i];
                if (url.startsWith(prefix)) {
                    return DRIVERS[i + 1];
                }
            }
        }
        return null;
    }

    /**
     * Load the driver class for the given URL, if the database URL is known.
     *
     * @param url the database URL
     */
    public static void load(String url) {
        String driver = getDriver(url);
        if (driver != null) {
            loadUserClass(driver);
        }
    }

    /**
     * Serialize the object to a byte array, using the serializer specified by
     * the connection info if set, or the default serializer.
     *
     * @param obj the object to serialize
     * @param dataHandler provides the object serializer (may be null)
     * @return the byte array
     */
    public static byte[] serialize(Object obj, DataHandler dataHandler) {
        try {
            JavaObjectSerializer handlerSerializer = null;
            if (dataHandler != null) {
                handlerSerializer = dataHandler.getJavaObjectSerializer();
            }
            if (handlerSerializer != null) {
                return handlerSerializer.serialize(obj);
            }
            if (serializer != null) {
                return serializer.serialize(obj);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw DbException.get(ErrorCode.SERIALIZATION_FAILED_1, e, e.toString());
        }
    }

    /**
     * De-serialize the byte array to an object, eventually using the serializer
     * specified by the connection info.
     *
     * @param data the byte array
     * @param dataHandler provides the object serializer (may be null)
     * @return the object
     * @throws DbException if serialization fails
     */
    public static Object deserialize(byte[] data, DataHandler dataHandler) {
        try {
            JavaObjectSerializer dbJavaObjectSerializer = null;
            if (dataHandler != null) {
                dbJavaObjectSerializer = dataHandler.getJavaObjectSerializer();
            }
            if (dbJavaObjectSerializer != null) {
                return dbJavaObjectSerializer.deserialize(data);
            }
            if (serializer != null) {
                return serializer.deserialize(data);
            }
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is;
            if (SysProperties.USE_THREAD_CONTEXT_CLASS_LOADER) {
                final ClassLoader loader = Thread.currentThread().getContextClassLoader();
                is = new ObjectInputStream(in) {
                    @Override
                    protected Class<?> resolveClass(ObjectStreamClass desc)
                            throws IOException, ClassNotFoundException {
                        try {
                            return Class.forName(desc.getName(), true, loader);
                        } catch (ClassNotFoundException e) {
                            return super.resolveClass(desc);
                        }
                    }
                };
            } else {
                is = new ObjectInputStream(in);
            }
            return is.readObject();
        } catch (Throwable e) {
            throw DbException.get(ErrorCode.DESERIALIZATION_FAILED_1, e, e.toString());
        }
    }

}
