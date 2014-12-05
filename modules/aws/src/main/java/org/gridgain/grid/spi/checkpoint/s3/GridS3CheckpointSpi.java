/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.s3;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * This class defines Amazon S3-based implementation for checkpoint SPI.
 * <p>
 * For information about Amazon S3 visit <a href="http://aws.amazon.com">aws.amazon.com</a>.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has one mandatory configuration parameter:
 * <ul>
 * <li>{@link #setAwsCredentials(AWSCredentials)}</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 * <li>{@link #setBucketNameSuffix(String)}</li>
 * <li>{@link #setClientConfiguration(ClientConfiguration)}</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * {@link GridS3CheckpointSpi} can be configured as follows:
 * <pre name="code" class="java">
 * GridConfiguration cfg = new GridConfiguration();
 *
 * GridS3CheckpointSpi spi = new GridS3CheckpointSpi();
 *
 * AWSCredentials cred = new BasicAWSCredentials(YOUR_ACCESS_KEY_ID, YOUR_SECRET_ACCESS_KEY);
 *
 * spi.setAwsCredentials(cred);
 *
 * spi.setBucketNameSuffix("checkpoints");
 *
 * // Override default checkpoint SPI.
 * cfg.setCheckpointSpi(cpSpi);
 *
 * // Start grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * {@link GridS3CheckpointSpi} can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *     ...
 *        &lt;property name=&quot;checkpointSpi&quot;&gt;
 *            &lt;bean class=&quot;org.gridgain.grid.spi.checkpoint.s3.GridS3CheckpointSpi&quot;&gt;
 *                &lt;property name=&quot;awsCredentials&quot;&gt;
 *                    &lt;bean class=&quot;com.amazonaws.auth.BasicAWSCredentials&quot;&gt;
 *                        &lt;constructor-arg value=&quot;YOUR_ACCESS_KEY_ID&quot; /&gt;
 *                        &lt;constructor-arg value=&quot;YOUR_SECRET_ACCESS_KEY&quot; /&gt;
 *                    &lt;/bean&gt;
 *                &lt;/property&gt;
 *            &lt;/bean&gt;
 *        &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * Note that storing data in AWS S3 service will result in charges to your AWS account.
 * Choose another implementation of {@link GridCheckpointSpi} for local or
 * home network tests.
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridCheckpointSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
public class GridS3CheckpointSpi extends IgniteSpiAdapter implements GridCheckpointSpi, GridS3CheckpointSpiMBean {
    /** Logger. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Marshaller. */
    @IgniteMarshallerResource
    private IgniteMarshaller marsh;

    /** Task that takes care about outdated files. */
    private GridS3TimeoutWorker timeoutWrk;

    /** Listener. */
    private GridCheckpointListener lsnr;

    /** Prefix to use in bucket name generation. */
    public static final String BUCKET_NAME_PREFIX = "gridgain-checkpoint-";

    /** Suffix to use in bucket name generation. */
    public static final String DFLT_BUCKET_NAME_SUFFIX = "default-bucket";

    /** Client to interact with S3 storage. */
    @GridToStringExclude
    private AmazonS3 s3;

    /** Bucket name suffix (set by user). */
    private String bucketNameSuffix;

    /** Bucket name (generated). */
    private String bucketName;

    /** Amazon client configuration. */
    private ClientConfiguration cfg;

    /** AWS Credentials. */
    @GridToStringExclude
    private AWSCredentials cred;

    @IgniteNameResource
    /** Grid name. */
    private String gridName;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Gets S3 bucket name to use.
     *
     * @return S3 bucket name to use.
     */
    @Override public String getBucketName() {
        return bucketName;
    }

    /**
     * Gets S3 access key.
     *
     * @return S3 access key.
     */
    @Override public String getAccessKey() {
        return cred.getAWSAccessKeyId();
    }

    /**
     * Gets S3 secret key.
     *
     * @return S3 secret key.
     */
    public String getSecretAccessKey() {
        return cred.getAWSSecretKey();
    }

    /**
     * Gets HTTP proxy host.
     *
     * @return HTTP proxy host.
     */
    @Override public String getProxyHost() {
        return cfg.getProxyHost();
    }

    /**
     * Gets HTTP proxy port.
     *
     * @return HTTP proxy port.
     */
    @Override public int getProxyPort() {
        return cfg.getProxyPort();
    }

    /**
     * Gets HTTP proxy user name.
     *
     * @return HTTP proxy user name.
     */
    @Override public String getProxyUsername() {
        return cfg.getProxyUsername();
    }

    /**
     * Gets HTTP proxy password.
     *
     * @return HTTP proxy password.
     */
    public String getProxyPassword() {
        return cfg.getProxyPassword();
    }

    /**
     * Sets bucket name suffix.
     *
     * @param bucketNameSuffix Bucket name suffix.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setBucketNameSuffix(String bucketNameSuffix) {
        this.bucketNameSuffix = bucketNameSuffix;
    }

    /**
     * Sets Amazon client configuration.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cfg Amazon client configuration.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setClientConfiguration(ClientConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Sets AWS credentials.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cred AWS credentials.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setAwsCredentials(AWSCredentials cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait"})
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(cred != null, "awsCredentials != null");

        if (log.isDebugEnabled()) {
            log.debug(configInfo("awsCredentials", cred));
            log.debug(configInfo("clientConfiguration", cfg));
            log.debug(configInfo("bucketNameSuffix", bucketNameSuffix));
        }

        if (cfg == null)
            U.warn(log, "Amazon client configuration is not set (will use default).");

        if (F.isEmpty(bucketNameSuffix)) {
            U.warn(log, "Bucket name suffix is null or empty (will use default bucket name).");

            bucketName = BUCKET_NAME_PREFIX + DFLT_BUCKET_NAME_SUFFIX;
        }
        else
            bucketName = BUCKET_NAME_PREFIX + bucketNameSuffix;

        s3 = cfg != null ? new AmazonS3Client(cred, cfg) : new AmazonS3Client(cred);

        if (!s3.doesBucketExist(bucketName)) {
            try {
                s3.createBucket(bucketName);

                if (log.isDebugEnabled())
                    log.debug("Created S3 bucket: " + bucketName);

                while (!s3.doesBucketExist(bucketName))
                    try {
                        U.sleep(200);
                    }
                    catch (GridInterruptedException e) {
                        throw new IgniteSpiException("Thread has been interrupted.", e);
                    }
            }
            catch (AmazonClientException e) {
                try {
                    if (!s3.doesBucketExist(bucketName))
                        throw new IgniteSpiException("Failed to create bucket: " + bucketName, e);
                }
                catch (AmazonClientException ignored) {
                    throw new IgniteSpiException("Failed to create bucket: " + bucketName, e);
                }
            }
        }

        Collection<GridS3TimeData> s3TimeDataLst = new LinkedList<>();

        try {
            ObjectListing list = s3.listObjects(bucketName);

            while (true) {
                for (S3ObjectSummary sum : list.getObjectSummaries()) {
                    GridS3CheckpointData data = read(sum.getKey());

                    if (data != null) {
                        s3TimeDataLst.add(new GridS3TimeData(data.getExpireTime(), data.getKey()));

                        if (log.isDebugEnabled())
                            log.debug("Registered existing checkpoint from key: " + data.getKey());
                    }
                }

                if (list.isTruncated())
                    list = s3.listNextBatchOfObjects(list);
                else
                    break;
            }
        }
        catch (AmazonClientException e) {
            throw new IgniteSpiException("Failed to read checkpoint bucket: " + bucketName, e);
        }
        catch (GridException e) {
            throw new IgniteSpiException("Failed to marshal/unmarshal objects in bucket: " + bucketName, e);
        }

        // Track expiration for only those data that are made by this node
        timeoutWrk = new GridS3TimeoutWorker();

        timeoutWrk.add(s3TimeDataLst);

        timeoutWrk.start();

        registerMBean(gridName, this, GridS3CheckpointSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (timeoutWrk != null) {
            GridUtils.interrupt(timeoutWrk);
            GridUtils.join(timeoutWrk, log);
        }

        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public byte[] loadCheckpoint(String key) throws IgniteSpiException {
        assert !F.isEmpty(key);

        try {
            GridS3CheckpointData data = read(key);

            return data != null ?
                data.getExpireTime() == 0 || data.getExpireTime() > U.currentTimeMillis() ?
                    data.getState() :
                    null :
                null;
        }
        catch (AmazonClientException e) {
            throw new IgniteSpiException("Failed to read checkpoint key: " + key, e);
        }
        catch (GridException e) {
            throw new IgniteSpiException("Failed to marshal/unmarshal objects in checkpoint key: " + key, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite)
        throws IgniteSpiException {
        assert !F.isEmpty(key);

        long expireTime = 0;

        if (timeout > 0) {
            expireTime = U.currentTimeMillis() + timeout;

            if (expireTime < 0)
                expireTime = Long.MAX_VALUE;
        }

        try {
            if (hasKey(key)) {
                if (!overwrite)
                    return false;

                if (log.isDebugEnabled())
                    log.debug("Overriding existing key: " + key);
            }

            GridS3CheckpointData data = new GridS3CheckpointData(state, expireTime, key);

            write(data);
        }
        catch (AmazonClientException e) {
            throw new IgniteSpiException("Failed to write checkpoint data [key=" + key + ", state=" +
                Arrays.toString(state) + ']', e);
        }
        catch (GridException e) {
            throw new IgniteSpiException("Failed to marshal checkpoint data [key=" + key + ", state=" +
                Arrays.toString(state) + ']', e);
        }

        if (timeout > 0)
            timeoutWrk.add(new GridS3TimeData(expireTime, key));

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        assert !F.isEmpty(key);

        timeoutWrk.remove(key);

        boolean rmv = false;

        try {
            rmv = delete(key);
        }
        catch (AmazonClientException e) {
            U.error(log, "Failed to delete data by key: " + key, e);
        }

        if (rmv) {
            GridCheckpointListener tmpLsnr = lsnr;

            if (tmpLsnr != null)
                tmpLsnr.onCheckpointRemoved(key);
        }

        return rmv;
    }

    /**
     * Reads checkpoint data.
     *
     * @param key Key name to read data from.
     * @return Checkpoint data object.
     * @throws GridException Thrown if an error occurs while unmarshalling.
     * @throws AmazonClientException If an error occurs while querying Amazon S3.
     */
    @Nullable private GridS3CheckpointData read(String key) throws GridException, AmazonClientException {
        assert !F.isEmpty(key);

        if (log.isDebugEnabled())
            log.debug("Reading data from S3 [bucket=" + bucketName + ", key=" + key + ']');

        try {
            S3Object obj = s3.getObject(bucketName, key);

            InputStream in = obj.getObjectContent();

            try {
                return marsh.unmarshal(in, U.gridClassLoader());
            }
            finally {
                U.closeQuiet(in);
            }
        }
        catch (AmazonServiceException e) {
            if (e.getStatusCode() != 404)
                throw e;
        }

        return null;
    }

    /**
     * Writes given checkpoint data to a given S3 bucket. Data is serialized to
     * the binary stream and saved to the S3.
     *
     * @param data Checkpoint data.
     * @throws GridException Thrown if an error occurs while marshalling.
     * @throws AmazonClientException If an error occurs while querying Amazon S3.
     */
    private void write(GridS3CheckpointData data) throws GridException, AmazonClientException {
        assert data != null;

        if (log.isDebugEnabled())
            log.debug("Writing data to S3 [bucket=" + bucketName + ", key=" + data.getKey() + ']');

        byte[] buf = marsh.marshal(data);

        ObjectMetadata meta = new ObjectMetadata();

        meta.setContentLength(buf.length);

        s3.putObject(bucketName, data.getKey(), new ByteArrayInputStream(buf), meta);
    }


    /**
     * Deletes checkpoint data.
     *
     * @param key Key of the data in storage.
     * @return {@code True} if operations succeeds and data is actually removed.
     * @throws AmazonClientException If an error occurs while querying Amazon S3.
     */
    private boolean delete(String key) throws AmazonClientException {
        assert !F.isEmpty(key);

        if (log.isDebugEnabled())
            log.debug("Removing data from S3 [bucket=" + bucketName + ", key=" + key + ']');

        if (!hasKey(key))
            return false;

        s3.deleteObject(bucketName, key);

        return true;
    }

    /**
     * Returns {@code true} if mapping presents for the provided key.
     *
     * @param key Key to check mapping for.
     * @return {@code true} if mapping presents for key.
     * @throws AmazonClientException If an error occurs while querying Amazon S3.
     */
    boolean hasKey(String key) throws AmazonClientException {
        assert !F.isEmpty(key);

        try {
            return s3.getObjectMetadata(bucketName, key).getContentLength() != 0;
        }
        catch (AmazonServiceException e) {
            if (e.getStatusCode() != 404)
                throw e;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void setCheckpointListener(GridCheckpointListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridS3CheckpointSpi.class, this);
    }

    /**
     * Implementation of {@link org.apache.ignite.spi.IgniteSpiThread} that takes care about outdated S3 data.
     * Every checkpoint has expiration date after which it makes no sense to
     * keep it. This worker periodically cleans S3 bucket according to checkpoints
     * expiration time.
     */
    private class GridS3TimeoutWorker extends IgniteSpiThread {
        /** List of data with access and expiration date. */
        private Map<String, GridS3TimeData> map = new HashMap<>();

        /**
         * Constructor.
         */
        GridS3TimeoutWorker() {
            super(gridName, "grid-s3-checkpoint-worker", log);
        }

        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException {
            long nextTime = 0;

            Collection<String> rmvKeys = new HashSet<>();

            while (!isInterrupted()) {
                rmvKeys.clear();

                synchronized (mux) {
                    long delay = U.currentTimeMillis() - nextTime;

                    if (nextTime != 0 && delay > 0)
                        mux.wait(delay);

                    long now = U.currentTimeMillis();

                    nextTime = -1;

                    // check map one by one and physically remove
                    // if (now - last modification date) > expiration time
                    for (Iterator<Map.Entry<String, GridS3TimeData>> iter = map.entrySet().iterator(); iter.hasNext();) {
                        Map.Entry<String, GridS3TimeData> entry = iter.next();

                        String key = entry.getKey();

                        GridS3TimeData timeData = entry.getValue();

                        if (timeData.getExpireTime() > 0)
                            if (timeData.getExpireTime() <= now) {
                                try {
                                   delete(key);

                                   if (log.isDebugEnabled())
                                        log.debug("Data was deleted by timeout: " + key);
                                }
                                catch (AmazonClientException e) {
                                    U.error(log, "Failed to delete data by key: " + key, e);
                                }

                                iter.remove();

                                rmvKeys.add(timeData.getKey());
                            }
                            else
                                if (timeData.getExpireTime() < nextTime || nextTime == -1)
                                    nextTime = timeData.getExpireTime();
                    }
                }

                GridCheckpointListener tmpLsnr = lsnr;

                if (tmpLsnr != null)
                    for (String key : rmvKeys)
                        tmpLsnr.onCheckpointRemoved(key);
            }

            synchronized (mux) {
                map.clear();
            }
        }

        /**
         * Adds data to a list of files this task should look after.
         *
         * @param timeData File expiration and access information.
         */
        void add(GridS3TimeData timeData) {
            assert timeData != null;

            synchronized (mux) {
                map.put(timeData.getKey(), timeData);

                mux.notifyAll();
            }
        }

        /**
         * Adds list of data this task should look after.
         *
         * @param newData List of data.
         */
        void add(Iterable<GridS3TimeData> newData) {
            assert newData != null;

            synchronized (mux) {
                for(GridS3TimeData data : newData)
                    map.put(data.getKey(), data);

                mux.notifyAll();
            }
        }

        /**
         * Removes data.
         *
         * @param key Checkpoint key.
         */
        public void remove(String key) {
            assert key != null;

            synchronized (mux) {
                map.remove(key);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridS3TimeoutWorker.class, this);
        }
    }
}
