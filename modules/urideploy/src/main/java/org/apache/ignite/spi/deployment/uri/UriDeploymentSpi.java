/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.spi.deployment.DeploymentResourceAdapter;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.uri.scanners.GridUriDeploymentScannerListener;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScanner;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScannerManager;
import org.apache.ignite.spi.deployment.uri.scanners.file.UriDeploymentFileScanner;
import org.apache.ignite.spi.deployment.uri.scanners.http.UriDeploymentHttpScanner;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link org.apache.ignite.spi.deployment.DeploymentSpi} which can deploy tasks from
 * different sources like file system folders, email and HTTP.
 * There are different ways to deploy tasks in grid and every deploy method
 * depends on selected source protocol. This SPI is configured to work
 * with a list of URI's. Every URI contains all data about protocol/transport
 * plus configuration parameters like credentials, scan frequency, and others.
 * <p>
 * When SPI establishes a connection with an URI, it downloads deployable units
 * to the temporary directory in order to prevent it from any changes while
 * scanning. Use method {@link #setTemporaryDirectoryPath(String) setTemporaryDirectoryPath(String)})
 * to set custom temporary folder for downloaded deployment units. SPI will
 * create folder under the path with name identical to local node ID.
 * <p>
 * SPI tracks all changes of every given URI. This means that if any file is
 * changed or deleted, SPI will re-deploy or delete corresponding tasks.
 * Note that the very first apply to {@link #findResource(String)}
 * is blocked until SPI finishes scanning all URI's at least once.
 * <p>
 * There are several deployable unit types supported:
 * <ul>
 * <li>GAR file.</li>
 * <li>Local disk folder with structure of unpacked GAR file.</li>
 * <li>Local disk folder containing only compiled Java classes.</li>
 * </ul>
 * <h1 class="header">GAR file</h1>
 * GAR file is a deployable unit. GAR file is based on <a href="http://www.gzip.org/zlib/">ZLIB</a>
 * compression format like simple JAR file and its structure is similar to WAR archive.
 * GAR file has {@code '.gar'} extension.
 * <p>
 * GAR file structure (file or directory ending with {@code '.gar'}):
 *   <pre class="snippet">
 *      META-INF/
 *              |
 *               - ignite.xml
 *               - ...
 *      lib/
 *         |
 *          -some-lib.jar
 *          - ...
 *      xyz.class
 *      ...</pre>
 * <ul>
 * <li>
 * {@code META-INF/} entry may contain {@code ignite.xml} file which is a
 * task descriptor file. The purpose of task descriptor XML file is to specify
 * all tasks to be deployed. This file is a regular
 * <a href="https://spring.io/docs">Spring</a> XML
 * definition file.  {@code META-INF/} entry may also contain any other file
 * specified by JAR format.
 * </li>
 * <li>
 * {@code lib/} entry contains all library dependencies.
 * </li>
 * <li>Compiled Java classes must be placed in the root of a GAR file.</li>
 * </ul>
 * GAR file may be deployed without descriptor file. If there is no descriptor file, SPI
 * will scan all classes in archive and instantiate those that implement
 * {@link org.apache.ignite.compute.ComputeTask} interface. In that case, all grid task classes must have a
 * public no-argument constructor. Use {@link org.apache.ignite.compute.ComputeTaskAdapter} adapter for
 * convenience when creating grid tasks.
 * <p>
 * By default, all downloaded GAR files that have digital signature in {@code META-INF}
 * folder will be verified and deployed only if signature is valid.
 * <p>
 * <h1 class="header">URI</h1>
 * This SPI uses a hierarchical URI definition. For more information about standard URI
 * syntax refer to {@link URI java.net.URI} documentation.
 * <blockquote class="snippet">
 * [<i>scheme</i><tt><b>:</b></tt>][<tt><b>//</b></tt><i>authority</i>][<i>path</i>][<tt><b>?</b></tt><i>query</i>][<tt><b>#</b></tt><i>fragment</i>]
 * </blockquote>
 * <p>
 * Every URI defines its own deployment repository which will be scanned for any changes.
 * URI itself has all information about protocol, connectivity, scan intervals and other
 * parameters.
 * <p>
 * URI's may contain special characters, like spaces. If {@code encodeUri}
 * flag is set to {@code true} (see {@link #setEncodeUri(boolean)}), then
 * URI 'path' field will be automatically encoded. By default this flag is
 * set to {@code true}.
 * <p>
 * <h1 class="header">Code Example</h1>
 * The following example demonstrates how the deployment SPI can be used. It expects that you have a GAR file
 * in 'home/username/ignite/work/my_deployment/file' folder which contains 'myproject.HelloWorldTask' class.
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * DeploymentSpi deploymentSpi = new UriDeploymentSpi();
 *
 * deploymentSpi.setUriList(Arrays.asList("file:///home/username/ignite/work/my_deployment/file"));
 *
 * cfg.setDeploymentSpi(deploymentSpi);
 *
 * try (Ignite ignite = Ignition.start(cfg)) {
 *     ignite.compute().execute("myproject.HelloWorldTask", "my args");
 * }
 * </pre>
 * <h1 class="header">Configuration</h1>
 * {@code UriDeploymentSpi} has the following optional configuration
 * parameters (there are no mandatory parameters):
 * <ul>
 * <li>
 * Array of {@link UriDeploymentScanner}-s which will be used to deploy resources
 * (see {@link #setScanners(UriDeploymentScanner...)}). If not specified, preconfigured {@link UriDeploymentFileScanner}
 * and {@link UriDeploymentHttpScanner} are used. You can implement your own scanner
 * by implementing {@link UriDeploymentScanner} interface.
 * </li>
 * <li>
 * Temporary directory path where scanned GAR files and directories are
 * copied to (see {@link #setTemporaryDirectoryPath(String) setTemporaryDirectoryPath(String)}).
 * </li>
 * <li>
 * List of URIs to scan (see {@link #setUriList(List)}). If not
 * specified, then URI specified by {@link #DFLT_DEPLOY_DIR DFLT_DEPLOY_DIR} is used.
 * </li>
 * <li>
 * Flag to control encoding of the {@code 'path'} portion of URI
 * (see {@link #setEncodeUri(boolean) setEncodeUri(boolean)}).
 * </li>
 * </ul>
 * <h1 class="header">Protocols</h1>
 * Following protocols are supported by this SPI out of the box:
 * <ul>
 * <li><a href="#file">file://</a> - File protocol</li>
 * <li><a href="#http">http://</a> - HTTP protocol</li>
 * <li><a href="#http">https://</a> - Secure HTTP protocol</li>
 * </ul>
 * <strong>Custom Protocols.</strong>
 * <p>
 * You can add support for additional protocols if needed. To do this implement UriDeploymentScanner interface and
 * plug your implementation into the SPI via {@link #setScanners(UriDeploymentScanner...)} method.
 * <p>
 * In addition to SPI configuration parameters, all necessary configuration
 * parameters for selected URI should be defined in URI. Different protocols
 * have different configuration parameters described below. Parameters are
 * separated by '{@code ;}' character.
 * <p>
 * <h1 class="header">File</h1>
 * For this protocol SPI will scan folder specified by URI on file system and
 * download any GAR files or directories that end with .gar from source
 * directory defined in URI. For file system URI must have scheme equal to {@code file}.
 * <p>
 * Following parameters are supported:
 * <table class="doctable">
 *  <tr>
 *      <th>Parameter</th>
 *      <th>Description</th>
 *      <th>Optional</th>
 *      <th>Default</th>
 *  </tr>
 *  <tr>
 *      <td>freq</td>
 *      <td>Scanning frequency in milliseconds.</td>
 *      <td>Yes</td>
 *      <td>{@code 5000} ms specified in {@link UriDeploymentFileScanner#DFLT_SCAN_FREQ}.</td>
 *  </tr>
 * </table>
 * <h2 class="header">File URI Example</h2>
 * The following example will scan {@code 'c:/Program files/ignite/deployment'}
 * folder on local box every {@code '1000'} milliseconds. Note that since path
 * has spaces, {@link #setEncodeUri(boolean) setEncodeUri(boolean)} parameter must
 * be set to {@code true} (which is default behavior).
 * <blockquote class="snippet">
 * {@code file://freq=2000@localhost/c:/Program files/ignite/deployment}
 * </blockquote>
 * <a name="classes"></a>
 * <h2 class="header">HTTP/HTTPS</h2>
 * URI deployment scanner tries to read DOM of the html it points to and parses out href attributes of all &lt;a&gt; tags
 * - this becomes the collection of URLs to GAR files that should be deployed. It's important that HTTP scanner
 * uses {@code URLConnection.getLastModified()} method to check if there were any changes since last iteration
 * for each GAR-file before redeploying.
 * <p>
 * Following parameters are supported:
 * <table class="doctable">
 *  <tr>
 *      <th>Parameter</th>
 *      <th>Description</th>
 *      <th>Optional</th>
 *      <th>Default</th>
 *  </tr>
 *  <tr>
 *      <td>freq</td>
 *      <td>Scanning frequency in milliseconds.</td>
 *      <td>Yes</td>
 *      <td>{@code 300000} ms specified in {@link UriDeploymentHttpScanner#DFLT_SCAN_FREQ}.</td>
 *  </tr>
 * </table>
 * <h2 class="header">HTTP URI Example</h2>
 * The following example will download the page `www.mysite.com/ignite/deployment`, parse it and download and deploy
 * all GAR files specified by href attributes of &lt;a&gt; elements on the page using authentication
 * {@code 'username:password'} every '10000' milliseconds (only new/updated GAR-s).
 * <blockquote class="snippet">
 * {@code http://username:password;freq=10000@www.mysite.com:110/ignite/deployment}
 * </blockquote>
 * <h2 class="header">Java Example</h2>
 * UriDeploymentSpi needs to be explicitly configured to override default local deployment SPI.
 * <pre name="code" class="java">
 * UriDeploymentSpi deploySpi = new UriDeploymentSpi();
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Set URIs.
 * deploySpi.setUriList(Arrays.asList("http://www.site.com/tasks",
 *     "file://freq=20000@localhost/c:/Program files/gg-deployment"));
 *
 * // Override temporary directory path.
 * deploySpi.setTemporaryDirectoryPath("c:/tmp/grid");
 *
 * //  Override default deployment SPI.
 * cfg.setDeploymentSpi(deploySpi);
 *
 * //  Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <p>
 * <h2 class="header">Spring Example</h2>
 * UriDeploymentSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="deploymentSpi"&gt;
 *             &lt;bean class="org.apache.ignite.grid.spi.deployment.uri.UriDeploymentSpi"&gt;
 *                 &lt;property name="temporaryDirectoryPath" value="c:/tmp/grid"/&gt;
 *                 &lt;property name="uriList"&gt;
 *                     &lt;list&gt;
 *                         &lt;value&gt;http://www.site.com/tasks&lt;/value&gt;
 *                         &lt;value&gt;file://freq=20000@localhost/c:/Program files/gg-deployment&lt;/value&gt;
 *                     &lt;/list&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.deployment.DeploymentSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
@IgniteSpiConsistencyChecked(optional = false)
@SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
public class UriDeploymentSpi extends IgniteSpiAdapter implements DeploymentSpi, UriDeploymentSpiMBean {
    /**
     * Default deployment directory where SPI will pick up GAR files. Note that this path is relative to
     * {@code IGNITE_HOME/work} folder if {@code IGNITE_HOME} system or environment variable specified,
     * otherwise it is relative to {@code work} folder under system {@code java.io.tmpdir} folder.
     *
     * @see org.apache.ignite.configuration.IgniteConfiguration#getWorkDirectory()
     */
    public static final String DFLT_DEPLOY_DIR = "deployment/file";

    /** Default task description file path and name (value is {@code META-INF/ignite.xml}). */
    public static final String XML_DESCRIPTOR_PATH = "META-INF/ignite.xml";

    /**
     * Default temporary directory name relative to file path
     * {@link #setTemporaryDirectoryPath(String)}} (value is {@code gg.uri.deployment.tmp}).
     */
    public static final String DEPLOY_TMP_ROOT_NAME = "gg.uri.deployment.tmp";

    /** Temporary directory name. */
    private String tmpDirPath;

    /** Sub-folder of 'tmpDirPath'. */
    private String deployTmpDirPath;

    /** List of URIs to be scanned. */
    private List<String> uriList = new ArrayList<>();

    /** List of encoded URIs. */
    private Collection<URI> uriEncodedList = new ArrayList<>();

    /** Indicates whether md5 digest should be checked by this SPI before file deployment. */
    private boolean checkMd5;

    /** */
    @SuppressWarnings({"CollectionDeclaredAsConcreteClass"})
    private final LinkedList<GridUriDeploymentUnitDescriptor> unitLoaders = new LinkedList<>();

    /** */
    @SuppressWarnings({"TypeMayBeWeakened"})
    private final LastTimeUnitDescriptorComparator unitComp = new LastTimeUnitDescriptorComparator();

    /** List of scanner managers. Every URI has it's own manager. */
    private final Collection<UriDeploymentScannerManager> mgrs = new ArrayList<>();

    /** Whether URIs should be encoded or not. */
    private boolean encodeUri = true;

    /** Whether first scan cycle is completed or not. */
    private int firstScanCntr;

    /** Deployment listener which processes all notifications from scanners. */
    private volatile DeploymentListener lsnr;

    /** */
    private final Object mux = new Object();

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** NOTE: flag for test purposes only. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean delayOnNewOrUpdatedFile;

    /** Configured scanners. */
    private UriDeploymentScanner[] scanners;

    /**
     * Sets absolute path to temporary directory which will be used by
     * deployment SPI to keep all deployed classes in.
     * <p>
     * If not provided, default value is {@code java.io.tmpdir} system property value.
     *
     * @param tmpDirPath Temporary directory path.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setTemporaryDirectoryPath(String tmpDirPath) {
        this.tmpDirPath = tmpDirPath;
    }

    /**
     * Sets list of URI which point to GAR file and which should be
     * scanned by SPI for the new tasks.
     * <p>
     * If not provided, default value is list with
     * {@code file://${IGNITE_HOME}/work/deployment/file} element.
     * Note that system property {@code IGNITE_HOME} must be set.
     * For unknown {@code IGNITE_HOME} list of URI must be provided explicitly.
     *
     * @param uriList GAR file URIs.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setUriList(List<String> uriList) {
        this.uriList = uriList;
    }

    /**
     * If set to {@code true} then SPI should exclude files with same md5s from deployment.
     * Otherwise it should try to load new unit regardless to possible file duplication.
     *
     * @param checkMd5 new value for the property
     */
    @IgniteSpiConfiguration(optional = true)
    public void setCheckMd5(boolean checkMd5) {
        this.checkMd5 = checkMd5;
    }

    /**
     * Gets {@code checkMd5} property.
     *
     * @return value of the {@code checkMd5} property.
     */
    @Override public boolean isCheckMd5() {
        return checkMd5;
    }

    /**
     * Indicates that URI must be encoded before usage. Encoding means replacing
     * all occurrences of space with '%20', percent sign with '%25'
     * and semicolon with '%3B'.
     * <p>
     * If not provided, default value is {@code true}.
     *
     * @param encodeUri {@code true} if every URI should be encoded and
     *      {@code false} otherwise.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setEncodeUri(boolean encodeUri) {
        this.encodeUri = encodeUri;
    }

    /** {@inheritDoc} */
    @Override public String getTemporaryDirectoryPath() {
        return tmpDirPath;
    }

    /** {@inheritDoc} */
    @Override public List<String> getUriList() {
        return Collections.unmodifiableList(uriList);
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DeploymentListener lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * Gets scanners.
     *
     * @return Scanners.
     */
    public UriDeploymentScanner[] getScanners() {
        return scanners;
    }

    /**
     * Sets scanners.
     *
     * @param scanners Scanners.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setScanners(UriDeploymentScanner... scanners) {
        this.scanners = scanners;
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        for (UriDeploymentScannerManager mgr : mgrs)
            mgr.cancel();

        for (UriDeploymentScannerManager mgr : mgrs)
            mgr.join();

        // Clear inner collections.
        uriEncodedList.clear();
        mgrs.clear();

        List<ClassLoader> tmpClsLdrs;

        // Release all class loaders.
        synchronized (mux) {
            tmpClsLdrs = new ArrayList<>(unitLoaders.size());

            for (GridUriDeploymentUnitDescriptor desc : unitLoaders)
                tmpClsLdrs.add(desc.getClassLoader());
        }

        for (ClassLoader ldr : tmpClsLdrs)
            onUnitReleased(ldr);

        // Delete temp directory.
        if (deployTmpDirPath != null)
            U.delete(new File(deployTmpDirPath));

        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(uriList != null, "uriList != null");

        initializeUriList();

        if (uriEncodedList.isEmpty())
            addDefaultUri();

        initializeTemporaryDirectoryPath();

        registerMBean(gridName, this, UriDeploymentSpiMBean.class);

        FilenameFilter filter = new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                assert name != null;

                return name.toLowerCase().endsWith(".gar");
            }
        };

        firstScanCntr = 0;

        GridUriDeploymentScannerListener lsnr = new GridUriDeploymentScannerListener() {
            @Override public void onNewOrUpdatedFile(File file, String uri, long tstamp) {
                if (log.isInfoEnabled())
                    log.info("Found new or updated GAR units [uri=" + U.hidePassword(uri) +
                        ", file=" + file.getAbsolutePath() + ", tstamp=" + tstamp + ']');

                if (delayOnNewOrUpdatedFile) {
                    U.warn(log, "Delaying onNewOrUpdatedFile() by 10000 ms since 'delayOnNewOrUpdatedFile' " +
                        "is set to true (is this intentional?).");

                    try {
                        U.sleep(10000);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        // No-op
                    }

                    U.warn(log, "Delay finished.");
                }

                try {
                    GridUriDeploymentFileProcessorResult fileRes = GridUriDeploymentFileProcessor.processFile(file, uri,
                        new File(deployTmpDirPath), log);

                    if (fileRes != null)
                        newUnitReceived(uri, fileRes.getFile(), tstamp, fileRes.getClassLoader(),
                            fileRes.getTaskClasses(), fileRes.getMd5());
                }
                catch (IgniteSpiException e) {
                    U.error(log, "Error when processing file: " + file.getAbsolutePath(), e);
                }
            }

            /** {@inheritDoc} */
            @Override public void onDeletedFiles(List<String> uris) {
                if (log.isInfoEnabled()) {
                    List<String> uriList = null;

                    if (uris != null) {
                        uriList = new ArrayList<>();

                        for (String uri : uris)
                            uriList.add(U.hidePassword(uri));
                    }

                    log.info("Found deleted GAR units [uris=" + uriList + ']');
                }

                processDeletedFiles(uris);
            }

            /** {@inheritDoc} */
            @Override public void onFirstScanFinished() {
                synchronized (mux) {
                    firstScanCntr++;

                    if (isFirstScanFinished(firstScanCntr))
                        mux.notifyAll();
                }
            }
        };

        // Set default scanners if none are configured.
        if (scanners == null) {
            scanners = new UriDeploymentScanner[2];

            scanners[0] = new UriDeploymentFileScanner();
            scanners[1] = new UriDeploymentHttpScanner();
        }

        for (URI uri : uriEncodedList) {
            File file = new File(deployTmpDirPath);

            long freq = -1;

            try {
                freq = getFrequencyFromUri(uri);
            }
            catch (NumberFormatException e) {
                U.error(log, "Error parsing parameter value for frequency.", e);
            }

            UriDeploymentScannerManager mgr = null;

            for (UriDeploymentScanner scanner : scanners) {
                if (scanner.acceptsURI(uri)) {
                    mgr = new UriDeploymentScannerManager(gridName, uri, file, freq > 0 ? freq :
                        scanner.getDefaultScanFrequency(), filter, lsnr, log, scanner);

                    break;
                }
            }

            if (mgr == null)
                throw new IgniteSpiException("Unsupported URI (please configure appropriate scanner): " + uri);

            mgrs.add(mgr);

            mgr.start();
        }

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("tmpDirPath", tmpDirPath));
            log.debug(configInfo("uriList", uriList));
            log.debug(configInfo("encodeUri", encodeUri));
            log.debug(configInfo("scanners", mgrs));
        }

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * Gets URI refresh frequency.
     * URI is parsed and {@code freq} parameter value returned.
     *
     * @param uri URI to be parsed.
     * @return {@code -1} if there if no {@code freq} parameter otherwise
     *      returns frequency.
     * @throws NumberFormatException Thrown if {@code freq} parameter value
     *      is not a number.
     */
    private long getFrequencyFromUri(URI uri) throws NumberFormatException {
        assert uri != null;

        String userInfo = uri.getUserInfo();

        if (userInfo != null) {
            String[] arr = userInfo.split(";");

            if (arr.length > 0)
                for (String el : arr)
                    if (el.startsWith("freq="))
                        return Long.parseLong(el.substring(5));
        }

        return -1;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public DeploymentResource findResource(String rsrcName) {
        assert rsrcName != null;

        // Wait until all scanner managers finish their first scanning.
        try {
            synchronized (mux) {
                while (!isFirstScanFinished(firstScanCntr))
                    mux.wait(5000);
            }
        }
        catch (InterruptedException e) {
            U.error(log, "Failed to wait while all scanner managers finish their first scanning.", e);

            Thread.currentThread().interrupt();

            return null;
        }

        synchronized (mux) {
            // Last updated class loader has highest priority in search.
            for (GridUriDeploymentUnitDescriptor unitDesc : unitLoaders) {
                // Try to find resource for current class loader.
                String clsName = rsrcName;
                Class<?> clsByAlias = unitDesc.getResourcesByAlias().get(rsrcName);

                if (clsByAlias != null)
                    clsName = clsByAlias.getName();

                try {
                    ClassLoader ldr = unitDesc.getClassLoader();

                    Class<?> cls = ldr instanceof GridUriDeploymentClassLoader ?
                        ((GridUriDeploymentClassLoader)ldr).loadClassGarOnly(clsName) :
                        ldr.loadClass(clsName);

                    assert cls != null;

                    IgniteBiTuple<Class<?>, String> rsrc = unitDesc.findResource(rsrcName);

                    if (rsrc != null) {
                        // Recalculate resource name in case if access is performed by
                        // class name and not the resource name.
                        String alias = rsrc.get2();

                        return new DeploymentResourceAdapter(
                            alias != null ? alias : rsrcName,
                            cls,
                            unitDesc.getClassLoader());
                    }
                    // Ignore invalid tasks.
                    else if (!ComputeTask.class.isAssignableFrom(cls)) {
                        unitDesc.addResource(cls);

                        return new DeploymentResourceAdapter(rsrcName, cls, unitDesc.getClassLoader());
                    }
                }
                catch (ClassNotFoundException ignored) {
                    // No-op.
                }
            }

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean register(ClassLoader ldr, Class<?> rsrc) throws IgniteSpiException {
        A.notNull(ldr, "ldr");
        A.notNull(rsrc, "rsrc");

        long tstamp = U.currentTimeMillis();

        Collection<ClassLoader> rmvClsLdrs = new ArrayList<>();

        Map<String, String> newRsrcs;

        synchronized (mux) {
            GridUriDeploymentUnitDescriptor desc = null;

            // Find existing class loader.
            for (GridUriDeploymentUnitDescriptor unitDesc : unitLoaders)
                if (unitDesc.getClassLoader().equals(ldr)) {
                    desc = unitDesc;

                    break;
                }

            if (desc == null) {
                desc = new GridUriDeploymentUnitDescriptor(tstamp, ldr);

                // New unit has largest timestamp.
                assert unitLoaders.size() <= 0 || unitComp.compare(desc, unitLoaders.getFirst()) <= 0;

                unitLoaders.addFirst(desc);
            }

            newRsrcs = addResources(ldr, desc, new Class<?>[]{rsrc});

            if (!F.isEmpty(newRsrcs))
                removeResources(ldr, newRsrcs, rmvClsLdrs);
        }

        for (ClassLoader cldLdr : rmvClsLdrs)
            onUnitReleased(cldLdr);

        return !F.isEmpty(newRsrcs);
    }

    /** {@inheritDoc} */
    @Override public boolean unregister(String rsrcName) {
        assert rsrcName != null;

        Collection<ClassLoader> rmvClsLdrs = new ArrayList<>();

        boolean rmv;

        synchronized (mux) {
            Map<String, String> rsrcs = U.newHashMap(1);

            rsrcs.put(rsrcName, rsrcName);

            rmv = removeResources(null, rsrcs, rmvClsLdrs);
        }

        for (ClassLoader cldLdr : rmvClsLdrs)
            onUnitReleased(cldLdr);

        return rmv;
    }

    /**
     * Add new classes in class loader resource map.
     * Note that resource map may contain two entries for one added class:
     * task name -> class name and class name -> class name.
     *
     * @param ldr Registered class loader.
     * @param desc Deployment descriptor.
     * @param clss Registered classes array.
     * @return Map of new resources added for registered class loader.
     * @throws org.apache.ignite.spi.IgniteSpiException If resource already registered. Exception thrown
     * if registered resources conflicts with rule when all task classes must be
     * annotated with different task names.
     */
    @Nullable
    private Map<String, String> addResources(ClassLoader ldr, GridUriDeploymentUnitDescriptor desc, Class<?>[] clss)
        throws IgniteSpiException {
        assert ldr != null;
        assert desc != null;
        assert clss != null;

        // Maps resources to classes.
        // Map may contain 2 entries for one class.
        Map<String, Class<?>> alias2Cls = new HashMap<>(clss.length * 2, 1.0f);

        // Check alias collision between added classes.
        for (Class<?> cls : clss) {
            String alias = null;

            if (ComputeTask.class.isAssignableFrom(cls)) {
                ComputeTaskName nameAnn = U.getAnnotation(cls, ComputeTaskName.class);

                if (nameAnn != null)
                    alias = nameAnn.value();
            }

            // If added classes maps to one alias.
            if (alias != null && alias2Cls.containsKey(alias) && !alias2Cls.get(alias).equals(cls))
                throw new IgniteSpiException("Failed to register resources with given task name " +
                    "(found another class with same task name) [taskName=" + alias +
                    ", cls1=" + cls.getName() + ", cls2=" + alias2Cls.get(alias).getName() + ", ldr=" + ldr + ']');

            if (alias != null) {
                alias2Cls.put(alias, cls);

                desc.addResource(alias, cls);
            }
            else
                desc.addResource(cls);
        }

        Map<String, String> newRsrcs = null;

        // Check collisions between added and exist classes.
        for (Entry<String, Class<?>> entry : alias2Cls.entrySet()) {
            String newAlias = entry.getKey();
            String newName = entry.getValue().getName();
            Class<?> cls = desc.getResourceByAlias(newAlias);

            if (cls != null) {
                // Different classes for the same resource name.
                if (!cls.getName().equals(newName))
                    throw new IgniteSpiException("Failed to register resources with given task name " +
                        "(found another class with same task name in the same class loader) [taskName=" + newAlias +
                        ", existingCls=" + cls.getName() + ", newCls=" + newName + ", ldr=" + ldr + ']');
            }
            // Add resources that should be removed for another class loaders.
            else {
                if (newRsrcs == null)
                    newRsrcs = U.newHashMap(alias2Cls.size() + clss.length);

                newRsrcs.put(newAlias, newName);
                newRsrcs.put(newName, newName);
            }
        }

        return newRsrcs;
    }

    /**
     * Remove resources for all class loaders except {@code ignoreClsLdr}.
     *
     * @param ignoreClsLdr Ignored class loader or {@code null} to remove for all class loaders.
     * @param rsrcs Resources that should be used in search for class loader to remove.
     * @param rmvClsLdrs Class loaders to remove.
     * @return {@code True} if resource was removed.
     */
    private boolean removeResources(@Nullable ClassLoader ignoreClsLdr, Map<String, String> rsrcs,
        Collection<ClassLoader> rmvClsLdrs) {
        assert Thread.holdsLock(mux);
        assert rsrcs != null;

        boolean res = false;

        for (Iterator<GridUriDeploymentUnitDescriptor> iter = unitLoaders.iterator(); iter.hasNext();) {
            GridUriDeploymentUnitDescriptor desc = iter.next();
            ClassLoader ldr = desc.getClassLoader();

            if (ignoreClsLdr == null || !ldr.equals(ignoreClsLdr)) {
                boolean isRmv = false;

                // Check class loader's registered resources.
                for (String rsrcName : rsrcs.keySet()) {
                    IgniteBiTuple<Class<?>, String> rsrc = desc.findResource(rsrcName);

                    // Remove class loader if resource found.
                    if (rsrc != null) {
                        iter.remove();

                        // Add class loaders in collection to notify listener outside synchronization block.
                        rmvClsLdrs.add(ldr);

                        isRmv = true;
                        res = true;

                        break;
                    }
                }

                if (isRmv)
                    continue;

                // Check is possible to load resources with classloader.
                for (Entry<String, String> entry : rsrcs.entrySet()) {
                    // Check classes with class loader only when classes points to classes to avoid redundant check.
                    // Resources map contains two entries for class with task name(alias).
                    if (entry.getKey().equals(entry.getValue()) && isResourceExist(ldr, entry.getKey())) {
                        iter.remove();

                        // Add class loaders in collection to notify listener outside synchronization block.
                        rmvClsLdrs.add(ldr);

                        res = true;

                        break;
                    }
                }
            }
        }

        return res;
    }

    /**
     * Check is class can be reached.
     *
     * @param ldr Class loader.
     * @param clsName Class name.
     * @return {@code true} if class can be loaded.
     */
    private boolean isResourceExist(ClassLoader ldr, String clsName) {
        String rsrc = clsName.replaceAll("\\.", "/") + ".class";

        InputStream in = null;

        try {
            in = ldr instanceof GridUriDeploymentClassLoader ?
                ((GridUriDeploymentClassLoader)ldr).getResourceAsStreamGarOnly(rsrc) :
                ldr.getResourceAsStream(rsrc);

            return in != null;
        }
        finally {
            U.close(in, log);
        }
    }

    /**
     * Tests whether first scan is finished or not.
     *
     * @param cntr Number of already scanned URIs.
     * @return {@code true} if all URIs have been scanned at least once and
     *      {@code false} otherwise.
     */
    private boolean isFirstScanFinished(int cntr) {
        assert uriEncodedList != null;

        return cntr >= uriEncodedList.size();
    }

    /**
     * Fills in list of URIs with all available URIs and encodes them if
     * encoding is enabled.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if at least one URI has incorrect syntax.
     */
    private void initializeUriList() throws IgniteSpiException {
        for (String uri : uriList) {
            assertParameter(uri != null, "uriList.get(X) != null");

            String encUri = encodeUri(uri.replaceAll("\\\\", "/"));

            URI uriObj;

            try {
                uriObj = new URI(encUri);
            }
            catch (URISyntaxException e) {
                throw new IgniteSpiException("Failed to parse URI [uri=" + U.hidePassword(uri) +
                    ", encodedUri=" + U.hidePassword(encUri) + ']', e);
            }

            if (uriObj.getScheme() == null || uriObj.getScheme().trim().isEmpty())
                throw new IgniteSpiException("Failed to get 'scheme' from URI [uri=" +
                    U.hidePassword(uri) +
                    ", encodedUri=" + U.hidePassword(encUri) + ']');

            uriEncodedList.add(uriObj);
        }
    }

    /**
     * Add configuration for file scanner.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if default URI syntax is incorrect.
     */
    private void addDefaultUri() throws IgniteSpiException {
        assert uriEncodedList != null;

        URI uri;

        try {
            uri = U.resolveWorkDirectory(ignite.configuration().getWorkDirectory(), DFLT_DEPLOY_DIR, false).toURI();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to initialize default file scanner", e);
        }

        uriEncodedList.add(uri);
    }

    /**
     * Encode URI path if encoding is enabled. Set of encoded characters
     * in path is (' ', ';', '%').
     *
     * @param path URI which should be encoded.
     * @return Either encoded URI if encoding is enabled or given one
     *      if encoding is disabled.
     */
    private String encodeUri(String path) {
        return encodeUri ? new GridUriDeploymentUriParser(path).parse() : path;
    }

    /**
     * Initializes temporary directory path. Path consists of base path
     * (either {@link #tmpDirPath} value or {@code java.io.tmpdir}
     * system property value if first is {@code null}) and path relative
     * to base one - {@link #DEPLOY_TMP_ROOT_NAME}/{@code local node ID}.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if temporary directory could not be created.
     */
    private void initializeTemporaryDirectoryPath() throws IgniteSpiException {
        String tmpDirPath = this.tmpDirPath == null ? System.getProperty("java.io.tmpdir") : this.tmpDirPath;

        if (tmpDirPath == null)
            throw new IgniteSpiException("Error initializing temporary deployment directory.");

        File dir = new File(tmpDirPath + File.separator + DEPLOY_TMP_ROOT_NAME + File.separator +
            ignite.configuration().getNodeId());

        if (!U.mkdirs(dir))
            throw new IgniteSpiException("Error initializing temporary deployment directory: " + dir);

        if (!dir.isDirectory())
            throw new IgniteSpiException("Temporary deployment directory path is not a valid directory: " + dir);

        if (!dir.canRead() || !dir.canWrite())
            throw new IgniteSpiException("Can not write to or read from temporary deployment directory: " + dir);

        this.tmpDirPath = tmpDirPath;

        deployTmpDirPath = dir.getPath();
    }

    /**
     * Deploys all tasks that correspond to given descriptor.
     * First method checks tasks versions and stops processing tasks that
     * have both versioned and unversioned instances.
     * <p>
     * Than it deletes tasks with lower version and deploys newest tasks.
     *
     * @param newDesc Tasks deployment descriptor.
     * @param clss Registered classes.
     */
    private void newUnitReceived(GridUriDeploymentUnitDescriptor newDesc, Collection<Class<?>> clss) {
        assert newDesc != null;
        assert newDesc.getType() == GridUriDeploymentUnitDescriptor.Type.FILE;

        if (clss != null && !clss.isEmpty()) {
            try {
                addResources(newDesc.getClassLoader(), newDesc, clss.toArray(new Class<?>[clss.size()]));
            }
            catch (IgniteSpiException e) {
                U.warn(log, "Failed to register GAR class loader [newDesc=" + newDesc +
                    ", msg=" + e.getMessage() + ']');
            }
        }

        Collection<ClassLoader> rmvClsLdrs = new ArrayList<>();

        synchronized (mux) {
            if (checkMd5 && unitDeployed(newDesc.getMd5())) {
                if (log.isInfoEnabled())
                    LT.info(log, "Skipping new deployment unit because of md5 match " +
                        "[uri='" + U.hidePassword(newDesc.getUri()) +
                        "', file=" + (newDesc.getFile() == null ? "N/A" : newDesc.getFile()) + ']');

                return;
            }

            boolean isAdded = false;
            boolean ignoreNewUnit = false;

            for (ListIterator<GridUriDeploymentUnitDescriptor> iter = unitLoaders.listIterator();
                 iter.hasNext();) {
                GridUriDeploymentUnitDescriptor desc = iter.next();

                assert !newDesc.getClassLoader().equals(desc.getClassLoader()) :
                    "URI scanners always create new class loader for every GAR file: " + newDesc;

                // Only for GAR files. Undeploy all for overwritten GAR files.
                if (desc.getType() == GridUriDeploymentUnitDescriptor.Type.FILE &&
                    newDesc.getUri().equals(desc.getUri()) && !newDesc.getFile().equals(desc.getFile())) {
                    // Remove descriptor.
                    iter.remove();

                    // Add class loaders in collection to notify listener outside synchronization block.
                    rmvClsLdrs.add(desc.getClassLoader());

                    // Last descriptor.
                    if (!iter.hasNext())
                        // New descriptor will be added after loop.
                        break;

                    continue;
                }

                if (!isAdded) {
                    // Unit with largest timestamp win.
                    // Insert it before current element.
                    if (unitComp.compare(newDesc, desc) <= 0) {
                        // Remove current class loader if found collisions.
                        if (checkUnitCollision(desc, newDesc)) {
                            iter.remove();
                            iter.add(newDesc);

                            // Add class loaders in collection to notify listener outside synchronization block.
                            rmvClsLdrs.add(desc.getClassLoader());
                        }
                        // Or add new class loader before current class loader.
                        else {
                            iter.set(newDesc);
                            iter.add(desc);
                        }

                        isAdded = true;
                    }
                    else if (checkUnitCollision(newDesc, desc)) {
                        // Don't add new unit if found collisions with latest class loader.
                        ignoreNewUnit = true;
                        break;
                    }
                }
                // New descriptor already added and we need to check other class loaders for collisions.
                else if (checkUnitCollision(newDesc, desc)) {
                    iter.remove();

                    // Add class loaders in collection to notify listener outside synchronization block.
                    rmvClsLdrs.add(desc.getClassLoader());
                }
            }

            if (!ignoreNewUnit) {
                if (!isAdded)
                    unitLoaders.add(newDesc);

                if (log.isDebugEnabled())
                    LT.info(log, "Class loader (re)registered [clsLdr=" + newDesc.getClassLoader() +
                        ", tstamp=" + newDesc.getTimestamp() +
                        ", uri='" + U.hidePassword(newDesc.getUri()) +
                        "', file=" + (newDesc.getFile() == null ? "N/A" : newDesc.getFile()) + ']');
            }
        }

        for (ClassLoader cldLdr : rmvClsLdrs)
            onUnitReleased(cldLdr);
    }

    /**
     * Check task resource collisions in added descriptor {@code newDesc} with another
     * descriptor {@code existDesc}.
     *
     * @param newDesc New added descriptor.
     * @param existDesc Exist descriptor.
     * @return {@code True} if collisions found.
     */
    private boolean checkUnitCollision(GridUriDeploymentUnitDescriptor newDesc,
        GridUriDeploymentUnitDescriptor existDesc) {
        assert newDesc != null;
        assert existDesc != null;

        Map<String, Class<?>> rsrcsByAlias = newDesc.getResourcesByAlias();

        for (Entry<String, Class<?>> entry : existDesc.getResourcesByAlias().entrySet()) {
            String rsrcName = entry.getKey();

            if (rsrcsByAlias.containsKey(rsrcName)) {
                U.warn(log, "Found collision with task name in different GAR files. " +
                    "Class loader will be removed [taskName=" + rsrcName + ", cls1=" + rsrcsByAlias.get(rsrcName) +
                    ", cls2=" + entry.getValue() + ", newDesc=" + newDesc + ", existDesc=" + existDesc + ']');

                return true;
            }
        }

        for (Class<?> rsrcCls : existDesc.getResources()) {
            if (!ComputeTask.class.isAssignableFrom(rsrcCls) &&
                isResourceExist(newDesc.getClassLoader(), rsrcCls.getName())) {
                U.warn(log, "Found collision with task class in different GAR files. " +
                    "Class loader will be removed [taskCls=" + rsrcCls +
                    ", removedDesc=" + newDesc + ", existDesc=" + existDesc + ']');

                return true;
            }
        }

        return false;
    }

    /**
     * Deploys or redeploys given tasks.
     *
     * @param uri GAR file deployment URI.
     * @param file GAR file.
     * @param tstamp File modification date.
     * @param ldr Class loader.
     * @param clss List of tasks which were found in GAR file.
     * @param md5 md5 of the new unit.
     */
    private void newUnitReceived(String uri, File file, long tstamp, ClassLoader ldr,
        Collection<Class<? extends ComputeTask<?, ?>>> clss, @Nullable String md5) {
        assert uri != null;
        assert file != null;
        assert tstamp > 0;

        // To avoid units with incorrect timestamp.
        tstamp = Math.min(tstamp, U.currentTimeMillis());

        // Create descriptor.
        GridUriDeploymentUnitDescriptor desc = new GridUriDeploymentUnitDescriptor(uri, file, tstamp, ldr, md5);

        newUnitReceived(desc, clss != null && !clss.isEmpty() ? new ArrayList<Class<?>>(clss) : null);
    }

    /**
     * Removes all tasks that belong to GAR files which are on list
     * of removed files.
     *
     * @param uris List of removed files.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    private void processDeletedFiles(List<String> uris) {
        assert uris != null;

        if (uris.isEmpty())
            return;

        synchronized (mux) {
            Set<String> uriSet = new HashSet<>(uris);

            for (Iterator<GridUriDeploymentUnitDescriptor> iter = unitLoaders.iterator(); iter.hasNext();) {
                GridUriDeploymentUnitDescriptor desc = iter.next();

                if (desc.getType() == GridUriDeploymentUnitDescriptor.Type.FILE && uriSet.contains(desc.getUri())) {
                    // Remove descriptor.
                    iter.remove();

                    onUnitReleased(desc.getClassLoader());
                }
            }
        }
    }

    /**
     * Notifies listener about released class loader.
     *
     * @param clsLdr Released class loader.
     */
    private void onUnitReleased(ClassLoader clsLdr) {
        // Make sure we don't remove system class loader.
        if (!clsLdr.equals(getClass().getClassLoader()))
            GridUriDeploymentFileProcessor.cleanupUnit(clsLdr, log);

        DeploymentListener tmp = lsnr;

        if (tmp != null)
            tmp.onUnregistered(clsLdr);
    }

    /**
     * Checks if a nut with the same md5 ai already deployed with this SPI.
     *
     * @param md5 md5 of a new unit.
     * @return {@code true} if this unit deployed, {@code false} otherwise.
     */
    private boolean unitDeployed(String md5) {
        assert Thread.holdsLock(mux);

        if (md5 != null) {
            for (GridUriDeploymentUnitDescriptor d: unitLoaders)
                if (md5.equals(d.getMd5()))
                    return true;
        }

        return false;
    }

    /**
     * Task deployment descriptor comparator.
     * The greater descriptor is those one that has less timestamp.
     */
    private static class LastTimeUnitDescriptorComparator implements Comparator<GridUriDeploymentUnitDescriptor>,
        Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(GridUriDeploymentUnitDescriptor o1, GridUriDeploymentUnitDescriptor o2) {
            if (o1.getTimestamp() < o2.getTimestamp())
                return 1;

            return o1.getTimestamp() == o2.getTimestamp() ? 0 : -1;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UriDeploymentSpi.class, this);
    }
}
