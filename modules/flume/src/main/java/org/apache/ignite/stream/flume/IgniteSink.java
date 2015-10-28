package org.apache.ignite.stream.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume custom sink for Apache Ignite.
 */
public class IgniteSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(IgniteSink.class);

    /**
     * Ignite configuration file.
     */
    private String springCfgPath;

    /**
     * Cache name.
     */
    private String cacheName;

    /**
     * Streamer implementation class.
     */
    private String streamerType;

    private FlumeStreamer flumeStreamer;
    private Ignite ignite;

    /**
     * Method to receive an initialized {@link Ignite} instance. Normally, it is sufficient to specify {@link
     * #springCfgPath} to access the grid from the sink.
     *
     * @param ignite {@link Ignite} instance.
     */
    public void specifyGrid(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Returns cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sink configurations with Ignite-specific settings.
     */
    @Override public void configure(Context context) {
        springCfgPath = context.getString(IgniteSinkConstants.CFG_PATH);
        cacheName = context.getString(IgniteSinkConstants.CFG_CACHE_NAME);
        streamerType = context.getString(IgniteSinkConstants.CFG_STREAMER_TYPE);
    }

    /**
     * Starts a grid and a streamer.
     */
    @Override synchronized public void start() {
        Ignition.setClientMode(true);

        try {
            if (ignite == null)
                ignite = Ignition.start(springCfgPath);

            if (ignite.cluster().forServers().nodes().isEmpty())
                throw new IgniteException("No remote server nodes!");

            if (streamerType != null && !streamerType.isEmpty()) {
                Class<? extends FlumeStreamer> clazz =
                    (Class<? extends FlumeStreamer>)Class.forName(streamerType);
                flumeStreamer = clazz.newInstance();

                flumeStreamer.start(ignite, cacheName);
            }
        }
        catch (Exception e) {
            log.error("Failed while starting an Ignite streamer", e);
            throw new FlumeException("Failed while starting an Ignite streamer", e);
        }

        super.start();
    }

    @Override synchronized public void stop() {
        if (flumeStreamer != null)
            flumeStreamer.stop();

        if (ignite != null)
            ignite.close();

        super.stop();
    }

    @Override public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;

        try {
            transaction.begin();
            event = channel.take();

            if (event != null) {
                flumeStreamer.writeEvent(event);
            }
            else {
                status = Status.BACKOFF;
            }

            transaction.commit();
        }
        catch (Exception e) {
            log.error("Failed to process events", e);
            transaction.rollback();
            throw new EventDeliveryException(e);
        }
        finally {
            transaction.close();
        }

        return status;
    }
}
