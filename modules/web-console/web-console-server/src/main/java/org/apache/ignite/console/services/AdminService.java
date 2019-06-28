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

package org.apache.ignite.console.services;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.AnnouncementRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.socket.WebSocketsManager;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.notification.NotificationDescriptor.ACCOUNT_DELETED;
import static org.apache.ignite.console.notification.NotificationDescriptor.ADMIN_WELCOME_LETTER;

/**
 * Service to handle administrator actions.
 */
@Service
public class AdminService {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final AccountsService accountsSrv;

    /** */
    private final ConfigurationsService cfgsSrv;

    /** */
    private final NotebooksService notebooksSrv;

    /** */
    private final ActivitiesService activitiesSrv;

    /** */
    private final NotificationService notificationSrv;

    /** */
    private final AnnouncementRepository annRepo;

    /** */
    private final WebSocketsManager wsm;

    /**
     * @param txMgr Transactions manager.
     * @param accountsSrv Service to work with accounts.
     * @param cfgsSrv Service to work with configurations.
     * @param notebooksSrv Service to work with notebooks.
     * @param activitiesSrv Service to work with activities.
     * @param notificationSrv Service to send notifications.
     * @param annRepo Repository to work with announcement.
     * @param wsm Web sockets manager.
     */
    public AdminService(
        TransactionManager txMgr,
        AccountsService accountsSrv,
        ConfigurationsService cfgsSrv,
        NotebooksService notebooksSrv,
        ActivitiesService activitiesSrv,
        NotificationService notificationSrv,
        AnnouncementRepository annRepo,
        WebSocketsManager wsm
    ) {
        this.txMgr = txMgr;
        this.accountsSrv = accountsSrv;
        this.cfgsSrv = cfgsSrv;
        this.notebooksSrv = notebooksSrv;
        this.activitiesSrv = activitiesSrv;
        this.notificationSrv = notificationSrv;
        this.annRepo = annRepo;
        this.wsm = wsm;
    }

    /**
     * @param startDate Start date.
     * @param endDate End date.
     * @return List of all users.
     */
    public JsonArray list(long startDate, long endDate) {
        List<Account> accounts = accountsSrv.list();

        JsonArray res = new JsonArray();

        accounts.forEach(account ->
            res.add(new JsonObject()
                .add("id", account.getId())
                .add("firstName", account.getFirstName())
                .add("lastName", account.getLastName())
                .add("admin", account.isAdmin())
                .add("email", account.getUsername())
                .add("company", account.getCompany())
                .add("country", account.getCountry())
                .add("lastLogin", account.lastLogin())
                .add("lastActivity", account.lastActivity())
                .add("activated", account.isEnabled())
                .add("counters", new JsonObject()
                    .add("clusters", 0)
                    .add("caches", 0)
                    .add("models", 0))
                .add("activitiesDetail", activitiesSrv.activitiesForPeriod(account.getId(), startDate, endDate))
            )
        );

        return res;
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     */
    public void delete(UUID accId) {
        Account acc = txMgr.doInTransaction(() -> {
            cfgsSrv.deleteByAccountId(accId);

            notebooksSrv.deleteByAccountId(accId);

            return accountsSrv.delete(accId);
        });

        notificationSrv.sendEmail(ACCOUNT_DELETED, acc);
    }

    /**
     * @param accId Account ID.
     * @param admin Admin flag.
     */
    public void toggle(UUID accId, boolean admin) {
        accountsSrv.toggle(accId, admin);
    }

    /**
     * @param params SignUp params.
     */
    public Account registerUser(SignUpRequest params) {
        Account acc = accountsSrv.create(params);

        notificationSrv.sendEmail(ADMIN_WELCOME_LETTER, acc);

        return acc;
    }

    /** */
    @EventListener(ApplicationReadyEvent.class)
    public void initAnnouncement() {
        updateAnnouncement(annRepo.load());
    }

    /**
     * @param ann Announcement.
     */
    public void updateAnnouncement(Announcement ann) {
        annRepo.save(ann);

        wsm.broadcastAnnouncement(ann);
    }
}
