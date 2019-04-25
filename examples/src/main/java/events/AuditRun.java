package events;

import java.io.Serializable;
import java.time.LocalDateTime;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

public class AuditRun implements IgniteRunnable, Serializable {

    @IgniteInstanceResource
    Ignite ignite;

    private IgniteCache cache;

    private Audit audit;

    private Auditor auditor;

    public AuditRun(IgniteCache cache) {
        this.cache = cache;
    }

    @Override public void run() {
        ClusterNode node = ignite.cluster().localNode();
        audit = new Audit() {
            @Override public void sendMessage(String message) {
                System.out.println(LocalDateTime.now()+" "+ message);
            }
        };
        auditor = new Auditor(cache, audit);
        auditor.init();
    }
}
