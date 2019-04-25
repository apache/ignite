package events;

import com.sbt.sbergrid.security.cache.SecurityInfo;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auditor for Ignite cache storing security information. in the constructor, accepts a cache object and an audit
 * object. Creates a continuous request and sets up a local event listener, logs events and sends them to the audit
 * system.
 */
public class AuditorSecurityInfo extends Auditor {

    final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private IgniteCache<String, SecurityInfo> cache;

    public AuditorSecurityInfo(IgniteCache<String, SecurityInfo> cache, Audit audit) {
        super(cache,audit);
    }

    @Override
    public void accept(CacheEntryEvent event) {

        EventType type = event.getEventType();
        SecurityInfo userInfo, userInfoOld;
        userInfo = (SecurityInfo)event.getValue();
        String message;

        switch (type) {
            case CREATED:
                message = "Created new User name=" + event.getKey()
                    + " with Security permissions={" + userInfo.getPermissions() + "}, User's roles={" + userInfo.getRoles() + "}";
                sendMessage(message);
                break;

            case REMOVED:
                userInfoOld = (SecurityInfo)event.getOldValue();
                message = "Remove User name=" + event.getKey()
                    + " with Security permissions={" + userInfoOld.getPermissions() + "}, User's roles={" + userInfoOld.getRoles() + "}";
                sendMessage(message);
                break;

            case UPDATED:
                userInfoOld = (SecurityInfo)event.getOldValue();
                message = "Update User name=" + event.getKey()
                    + " OLD Value Security permissions={" + userInfoOld.getPermissions() + "}, User's roles={" + userInfoOld.getRoles() + "}, "
                    + " NEW Values Security permissions={" + userInfo.getPermissions() + "}, User's roles={" + userInfo.getRoles() + "}";
                sendMessage(message);
                break;

            default:
                message = "UNKNOWN EVENT=" + event.toString();
                sendMessage(message);
                break;
        }

    }

}
