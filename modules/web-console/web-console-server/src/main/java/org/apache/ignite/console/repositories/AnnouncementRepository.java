/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

        txMgr.registerStarter("announcement", () -> announcementTbl = new Table<>(ignite, "wc_announcement"));
    }

    /**
     * @return Announcement.
     */
    public Announcement load() {
        return txMgr.doInTransaction(() -> {
            Announcement ann = announcementTbl.load(ID);

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
