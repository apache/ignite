package events;

import java.time.LocalDateTime;
import java.util.logging.Logger;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

public class AuditService<E,T> implements Service {

    /** Ignite. */
    @IgniteInstanceResource Ignite ignite;
    /** Logger. */
    final Logger LOGGER = Logger.getLogger(this.getClass().getName());
    /** Cache. */
    private IgniteCache cache;
    /** Audit cache. */
    private IgniteCache<LocalDateTime, String> auditCache;

    private Audit audit;
    private Auditor auditor;
    private ContinuousQuery<T, E> qry;

    /**
     * @param cache Cache.
     * @param auditCache Audit cache.
     */
    public AuditService(IgniteCache cache,
        IgniteCache<LocalDateTime, String> auditCache) {
        this.cache = cache;
        this.auditCache = auditCache;
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {

    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) {
        audit = message -> auditCache.put(LocalDateTime.now(), message);

        qry = new ContinuousQuery<>();
        qry.setAutoUnsubscribe(false);
        qry.setLocalListener(evnts -> evnts.forEach(this::accept));

        cache.query(qry);

        auditor=new Auditor(cache,audit);
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) {
auditor.init();
    }

    private void accept(CacheEntryEvent evt) {

        EventType type = evt.getEventType();
        E newVal, oldVal;
        newVal = (E)evt.getValue();
        String msg;

        switch (type) {
            case CREATED:
                msg = "Created new Entity key=" + evt.getKey() + " value={" + newVal.toString() + "}";
                sendMessage(msg);
                break;

            case REMOVED:
                oldVal = (E)evt.getOldValue();
                msg = "Remove Entity key=" + evt.getKey()
                    + " value={" + oldVal.toString() + "}";
                sendMessage(msg);
                break;

            case UPDATED:
                oldVal = (E)evt.getOldValue();
                msg = "Update Entity key=" + evt.getKey() + " "
                    + "OLD value={" + oldVal.toString() + "}, "
                    + "NEW value={" + newVal.toString() + "}";
                sendMessage(msg);
                break;

            default:
                msg = "UNKNOWN EVENT=" + evt.toString();
                sendMessage(msg);
                break;
        }

    }

    private void sendMessage(String msg){
        LOGGER.info(msg);
        System.out.println(msg);
        audit.sendMessage(msg);
    }
}
