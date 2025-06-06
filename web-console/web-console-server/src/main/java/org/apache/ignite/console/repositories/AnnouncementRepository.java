

package org.apache.ignite.console.repositories;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with announcement.
 */
@Repository
public class AnnouncementRepository {
    /** Announcement ID. */
    private static final UUID ID = UUID.fromString("46e47a4b-fb92-4462-808a-4350fd9855de");

    /** */
    private final TransactionManager txMgr;

    /** */
    private Table<Announcement> announcementTbl;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public AnnouncementRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> announcementTbl = new Table<>(ignite, "wc_announcement"));
    }

    /**
     * @return Announcement.
     */
    public Announcement load() {
        return txMgr.doInTransaction(() -> {
            Announcement ann = announcementTbl.get(ID);

            if (ann == null) {
                ann = new Announcement(ID, "", false);

                announcementTbl.save(ann);
            }

            return ann;
        });
    }

    /**
     * Save announcement.
     *
     * @param ann Announcement.
     */
    public void save(Announcement ann) {
        txMgr.doInTransaction(() -> {
            ann.setId(ID);

            announcementTbl.save(ann);
        });
    }
}
