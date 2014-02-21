// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.security.*;
import java.util.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Contains the list with one class from every used library.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridLibraryConsistencyCheck {
    /** List that contains by one class from each library. */
    public static final String[] CLASS_LIST = new String[] {
        "org.jetbrains.annotations.Nullable",                             // annotations-XXX.jar
        "org.aopalliance.intercept.MethodInterceptor",                    // aopalliance-XXX.jar
        "org.aspectj.lang.annotation.Aspect",                             // aspectjrt-XXX.jar
        "com.amazonaws.services.s3.AmazonS3",                             // aws-java-sdk-XXX.jar
        "org.apache.commons.beanutils.converters.DateTimeConverter",      // commons-beanutils-XXX.jar
        "org.apache.commons.cli.CommandLineParser",                       // commons-cli-XXX.jar
        "org.apache.commons.codec.digest.DigestUtils",                    // commons-codec-XXX.jar
        "org.apache.commons.collections.CollectionUtils",                 // commons-collections-XXX.jar
        "org.apache.commons.configuration.HierarchicalINIConfiguration",  // commons-configuration-XXX.jar
        "org.apache.commons.io.FileUtils",                                // commons-io-XXX.jar
        "org.apache.commons.jexl2.JexlContext",                           // commons-jexl-XXX.jar
        "org.apache.commons.lang.ArrayUtils",                             // commons-lang-XXX.jar
        "org.apache.commons.logging.LogFactory",                          // commons-logging-XXX.jar
        "org.apache.commons.net.util.Base64",                             // commons-net-XXX.jar
        "it.sauronsoftware.cron4j.Scheduler",                             // cron4j-XXX.jar
        "org.dom4j.io.SAXReader",                                         // dom4j-XXX.jar
        "com.enterprisedt.net.ftp.FTPFile",                               // edtFTPj-XXX.jar
        "com.google.common.collect.MinMaxPriorityQueue",                  // guava-XXX.jar
        "org.h2.result.SearchRow",                                        // h2-XXX.jar
        "org.apache.hadoop.mapred.OutputCollector",                       // hadoop-mapreduce-client-core-XXX.jar
        "org.apache.hadoop.fs.AbstractFileSystem",                        // hadoop-common-XXX.jar
        "org.hibernate.Session",                                          // hibernate-XXX.jar
        "javax.persistence.Entity",                                       // hibernate-jpa-2.0-api-XXX.jar
        "org.apache.http.conn.ssl.SSLSocketFactory",                      // httpclient-XXX.jar
        "org.apache.http.HttpResponse",                                   // httpcore-XXX.jar
        "javassist.util.proxy.ProxyFactory",                              // javassist-XXX.jar
        "javax.servlet.http.HttpServlet",                                 // javax.servlet-api-XXX.jar
        "com.beust.jcommander.JCommander",                                // jcommander-XXX.jar
        "org.eclipse.jetty.server.NetworkConnector",                      // jetty-server-XXX.jar
        "org.eclipse.jetty.util.MultiException",                          // jetty-util-XXX.jar
        "org.eclipse.jetty.xml.XmlConfiguration",                         // jetty-xml-XXX.jar
        "com.jidesoft.chart.model.Chartable",                             // jide-charts-XXX.jar
        "com.jidesoft.swing.JideScrollPane",                              // jide-common-XXX.jar
        "com.jidesoft.status.StatusBarItem",                              // jide-components-XXX.jar
        "com.jidesoft.grid.SortEvent",                                    // jide-grids-XXX.jar
        "org.fife.ui.rsyntaxtextarea.RSyntaxTextArea",                    // rsyntaxtextarea-XXX.jar
        "org.fife.ui.autocomplete.DefaultCompletionProvider",             // autocomplete-XXX.jar
        "com.jcraft.jsch.JSch",                                           // jsch-XXX.jar
        "net.sf.json.JSONSerializer",                                     // json-lib-XXX.jar
        "org.w3c.tidy.Tidy",                                              // jtidy-r938.jar
        "org.mozilla.universalchardet.UniversalDetector",                 // juniversalchardet-XXX.jar
        "org.apache.log4j.RollingFileAppender",                           // log4j-XXX.jar
        "org.apache.lucene.store.IndexOutput",                            // lucene-core-XXX.jar
        "javax.mail.internet.MimeMessage",                                // mail-XXX.jar
        "net.miginfocom.layout.LayoutCallback",                           // miglayout-core-XXX.jar
        "net.miginfocom.swing.MigLayout",                                 // miglayout-swing-XXX.jar
        "com.mongodb.DBCollection",                                       // mongo-java-driver-XXX.jar
        "io.netty.channel.Channel",                                       // netty-all-XXX.jar
        "com.google.protobuf.ByteString",                                 // protobuf-java-XXX.jar
        "org.slf4j.LoggerFactory",                                        // slf4j-api-XXX.jar
        "edu.stanford.ppl.concurrent.SnapTreeMap",                        // snaptree-gg-XXX.jar
        "org.springframework.aop.support.DefaultPointcutAdvisor",         // spring-aop-XXX.jar
        "org.springframework.beans.factory.ListableBeanFactory",          // spring-beans-XXX.jar
        "org.springframework.context.ApplicationContext",                 // spring-context-XXX.jar
        "org.springframework.core.io.UrlResource",                        // spring-core-XXX.jar
        "org.apache.tika.io.TikaInputStream",                             // tika-core-XXX.jar
        "org.apache.tika.parser.txt.UniversalEncodingDetector"            // tika-parsers-1.3.jar
    };

    /** */
    public static final String NOT_FOUND_MESSAGE = "not found";

    /** @return List with library names. */
    public static ArrayList<String> libraries() {
        // Don't load classes, if check is disabled.
        if (!checkEnabled())
            return new ArrayList<>(0);

        ArrayList<String> res = new ArrayList<>(CLASS_LIST.length);

        for (String clsName : CLASS_LIST) {
            Class<?> cls;

            try {
                cls = Class.forName(clsName);
            }
            catch (ClassNotFoundException | LinkageError ignored) {
                res.add(NOT_FOUND_MESSAGE);

                continue;
            }

            CodeSource codeSrc;

            try {
                codeSrc = cls.getProtectionDomain().getCodeSource();
            }
            catch (SecurityException ignored) {
                res.add(NOT_FOUND_MESSAGE);

                continue;
            }

            // Unlikely to happen, but to make sure our code is not broken.
            if (codeSrc == null || codeSrc.getLocation() == null || codeSrc.getLocation().getPath() == null) {
                res.add(NOT_FOUND_MESSAGE);

                continue;
            }

            String lib = codeSrc.getLocation().getPath();

            String separator = "/";

            // Use common separator, for example slash or backslash can be used on windows.
            lib = lib.replace(File.separator, separator);

            // Class can be located in jar file or can be added as is to classpath.
            res.add(lib.endsWith(".jar") || lib.endsWith(".zip") ? lib.substring(lib.lastIndexOf(separator) + 1) :
                "not jar or zip file");
        }

        return res;
    }

    /**
     * @param libs1 First list with lib names.
     * @param libs2 Second list with lib names.
     * @return Tuple list with lib names which differ, empty if libs are equal or the check is disabled.
     * @throws GridException If libraries do not match.
     */
    public static List<GridBiTuple<String, String>> check(List<String> libs1, List<String> libs2) throws GridException {
        // The check is disabled on the local or remote nodes.
        if (F.isEmpty(libs1) || F.isEmpty(libs2) || !checkEnabled())
            return Collections.emptyList();

        // Must not happen.
        if (libs1.size() != libs2.size() || libs1.size() != CLASS_LIST.length)
            throw new GridException("GridGain dependency libraries do not match (is GridGain version the same?)");

        List<GridBiTuple<String, String>> res = new ArrayList<>();

        for (int i = 0; i < libs1.size(); i++) {
            if (!F.eq(libs1.get(i), libs2.get(i)))
                res.add(new GridBiTuple<>(libs1.get(i), libs2.get(i)));
        }

        return res;
    }

    /** @return {@code true} if check for library consistency should be performed, {@code false} otherwise. */
    private static boolean checkEnabled() {
        return Boolean.parseBoolean(System.getProperty(GG_LIBRARY_CONSISTENCY_CHECK, "true"));
    }

    /** Constructor. */
    private GridLibraryConsistencyCheck() {
        // No-op.
    }
}
