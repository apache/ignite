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

package org.apache.ignite.console.migration;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.dto.ActivityKey;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.repositories.NotebooksRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.migration.MigrateUtils.asListOfObjectIds;
import static org.apache.ignite.console.migration.MigrateUtils.asPrimitives;
import static org.apache.ignite.console.migration.MigrateUtils.asStrings;
import static org.apache.ignite.console.migration.MigrateUtils.getDocument;
import static org.apache.ignite.console.migration.MigrateUtils.getInteger;
import static org.apache.ignite.console.migration.MigrateUtils.mongoIdsToNewIds;
import static org.apache.ignite.console.migration.MigrateUtils.mongoToJson;
import static org.apache.ignite.console.migration.MigrateUtils.off;
import static org.apache.ignite.console.utils.Utils.fromJson;

/**
 * Service to migrate user data from MongoDB to GridGain persistence.
 */
@Service
public class MigrationFromMongo {
    /** */
    private static final Logger log = LoggerFactory.getLogger(MigrationFromMongo.class);

    /** */
    private final TransactionManager txMgr;

    /** */
    private final AccountsRepository accRepo;

    /** */
    private final NotebooksRepository notebooksRepo;

    /** */
    private final ConfigurationsRepository cfgsRepo;

    /** */
    private final ActivitiesRepository activitiesRepo;

    /** */
    protected MongoDatabase mongoDb;

    /** */
    @Value("${migration.mongo.db.name:}")
    private String mongoDbName;

    /**
     * Initialize migration service.
     *
     * @param ignite Ignite.
     * @param txMgr Transaction manager.
     * @param accRepo Repository to work with accounts.
     * @param notebooksRepo Repository to work with notebooks.
     * @param cfgsRepo Repository to work with configurations.
     * @param activitiesRepo Repository to work with activities.
     */
    public MigrationFromMongo(
        Ignite ignite,
        TransactionManager txMgr,
        AccountsRepository accRepo,
        NotebooksRepository notebooksRepo,
        ConfigurationsRepository cfgsRepo,
        ActivitiesRepository activitiesRepo
    ) {
        this.txMgr = txMgr;
        this.accRepo = accRepo;
        this.notebooksRepo = notebooksRepo;
        this.cfgsRepo = cfgsRepo;
        this.activitiesRepo = activitiesRepo;
    }

    /**
     * Migrate from Mongo to GridGain.
     */
    public void migrate() {
        if (F.isEmpty(mongoDbName)) {
            log.info("MongoDB database name was not specified. Migration disabled.");

            return;
        }

        boolean migrationNeeded;

        try(Transaction tx = txMgr.txStart()) {
            migrationNeeded = accRepo.ensureFirstUser();

            tx.rollback();
        }

        if (!migrationNeeded) {
            log.warn("Database was already migrated. Consider to disable migration in application settings.");

            return;
        }

        log.info("Migration started...");

        MongoClient mongoClient = MongoClients.create();

        try {
            mongoDb = mongoClient.getDatabase(mongoDbName);

            migrateAccounts();

            log.info("Migration finished!");
        }
        catch (Throwable e) {
            log.error("Migration failed", e);
        }
        finally {
            U.closeQuiet(mongoClient);
        }
    }

    /**
     * @param doc Mongo document.
     * 
     * @return Account to save.
     */
    protected Account createAccount(Document doc) {
        Account acc = new Account();

        acc.setId(UUID.randomUUID());
        acc.setEmail(doc.getString("email"));
        acc.setPassword("{pbkdf2}" + doc.getString("salt") + doc.getString("hash"));
        acc.setFirstName(doc.getString("firstName"));
        acc.setLastName(doc.getString("lastName"));
        acc.setPhone(doc.getString("phone"));
        acc.setCompany(doc.getString("company"));
        acc.setCountry(doc.getString("country"));
        acc.setToken(doc.getString("token"));
        acc.setResetPasswordToken(doc.getString("resetPasswordToken"));

        acc.setAdmin(doc.getBoolean("admin", false));

        return acc;
    }

    /**
     * Migrate accounts.
     */
    protected void migrateAccounts() {
        MongoCollection<Document> accountsCollection = mongoDb.getCollection("accounts");
        MongoCollection<Document> spacesCollection = mongoDb.getCollection("spaces");

        log.info(off(1, "Accounts to migrate: " + accountsCollection.countDocuments()));

        try (MongoCursor<Document> cursor = accountsCollection.find().iterator()) {
            while (cursor.hasNext()) {
                Document accMongo = cursor.next();

                ObjectId mongoAccId = accMongo.getObjectId("_id");
                String email = accMongo.getString("email");

                Document space = spacesCollection.find(
                    Filters.and(
                        Filters.eq("owner", mongoAccId),
                        Filters.eq("demo", false)
                    )
                ).first();

                if (space == null) {
                    log.warn("Space not found [owner=" + mongoAccId + ", email=" + email + "]");

                    continue;
                }

                log.info(off(2, "Migrating account [_id=" + mongoAccId + ", email=" + email + "]"));

                try (Transaction tx = txMgr.txStart()) {
                    Account acc = createAccount(accMongo);

                    accRepo.ensureFirstUser();
                    accRepo.save(acc);

                    migrateAccountObjects(accMongo, space, acc.getId());

                    tx.commit();
                }
            }
        }
    }

    /**
     * Migrate objects related to account.
     *
     * @param accMongo Mongo document with account.
     * @param space Mongo document with account space.
     * @param accId Account ID.
     */
    protected void migrateAccountObjects(Document accMongo, Document space, UUID accId) {
        migrateNotebooks(space, accId);
        migrateConfigurations(space, accId);
        migrateActivities(space, accId);
    }

    /**
     * @param space Account space.
     * @param accId Account ID.
     */
    private void migrateNotebooks(Document space, UUID accId) {
        MongoCollection<Document> notebooksCollection = mongoDb.getCollection("notebooks");

        ObjectId spaceId = space.getObjectId("_id");

        long cnt = notebooksCollection.countDocuments(Filters.eq("space", spaceId));

        log.info(off(2, "Migrating notebooks: " + cnt));

        try (MongoCursor<Document> cursor = notebooksCollection.find(Filters.eq("space", spaceId)).iterator()) {
            while (cursor.hasNext()) {
                Document notebookMongo = cursor.next();

                log.info(off(3, "Migrating notebook: [_id=" + notebookMongo.getObjectId("_id") + ", name=" + notebookMongo.getString("name") + "]"));

                Notebook notebook = new Notebook();
                notebook.setId(UUID.randomUUID());
                notebook.setName(notebookMongo.getString("name"));

                notebook.setExpandedParagraphs(asPrimitives(notebookMongo, "expandedParagraphs"));

                List<Document> paragraphsMongo = (List<Document>)notebookMongo.get("paragraphs");

                Notebook.Paragraph[] paragraphs = paragraphsMongo
                    .stream()
                    .map(paragraphMongo -> {
                        Notebook.Paragraph paragraph = new Notebook.Paragraph();

                        paragraph.setCacheName(paragraphMongo.getString("name"));
                        paragraph.setQuery(paragraphMongo.getString("query"));

                        Notebook.QueryType qryType = "scan".equalsIgnoreCase(paragraphMongo.getString("qryType"))
                            ? Notebook.QueryType.SCAN
                            : Notebook.QueryType.SQL_FIELDS;

                        paragraph.setQueryType(qryType);
                        paragraph.setResult(Notebook.ResultType.valueOf(paragraphMongo.getString("result").toUpperCase()));

                        paragraph.setPageSize(getInteger(paragraphMongo, "pageSize", 50));
                        paragraph.setTimeLineSpan(getInteger(paragraphMongo, "timeLineSpan", 1));
                        paragraph.setMaxPages(getInteger(paragraphMongo, "maxPages", 0));

                        paragraph.setUseAsDefaultSchema(paragraphMongo.getBoolean("useAsDefaultSchema", false));
                        paragraph.setNonCollocatedJoins(paragraphMongo.getBoolean("nonCollocatedJoins", false));
                        paragraph.setEnforceJoinOrder(paragraphMongo.getBoolean("enforceJoinOrder", false));
                        paragraph.setLazy(paragraphMongo.getBoolean("lazy", false));
                        paragraph.setCollocated(paragraphMongo.getBoolean("collocated", false));

                        Notebook.ChartOptions chartOpts = new Notebook.ChartOptions();

                        Document chartOptionsMongo = getDocument(paragraphMongo, "chartsOptions");

                        if (chartOptionsMongo != null) {
                            Document opts = getDocument(chartOptionsMongo, "areaChart");

                            if (opts != null)
                                chartOpts.setAreaChartStyle(opts.getString("style"));

                            opts = getDocument(chartOptionsMongo, "barChart");

                            if (opts != null)
                                chartOpts.setBarChartStacked(opts.getBoolean("stacked", true));
                        }

                        paragraph.setChartsOptions(chartOpts);

                        Notebook.Rate rate = new Notebook.Rate();

                        Document rateMongo = getDocument(paragraphMongo, "rate");

                        rate.setValue(getInteger(rateMongo, "value", 1));
                        rate.setUnit(getInteger(rateMongo, "unit", 60000));
                        rate.setInstalled(rateMongo.getBoolean("installed", false));

                        paragraph.setRate(rate);

                        return paragraph;
                    })
                    .toArray(Notebook.Paragraph[]::new);

                notebook.setParagraphs(paragraphs);

                notebooksRepo.save(accId, notebook);
            }
        }
    }

    /**
     * @param space Account space.
     * @param accId Account ID.
     */
    private void migrateConfigurations(Document space, UUID accId) {
        MongoCollection<Document> clusters = mongoDb.getCollection("clusters");

        long cnt = clusters.countDocuments(Filters.eq("space", space.getObjectId("_id")));

        log.info(off(2, "Migrating configurations: " + cnt));

        ConfigurationKey accKey = new ConfigurationKey(accId, false);

        try (MongoCursor<Document> cursor = clusters.find(Filters.eq("space", space.getObjectId("_id"))).iterator()) {
            while (cursor.hasNext()) {
                Document clusterMongo = cursor.next();

                ObjectId mongoClusterId = clusterMongo.getObjectId("_id");

                log.info(off(2, "Migrating cluster: [_id=" + mongoClusterId + ", name=" + clusterMongo.getString("name") + "]"));

                Map<ObjectId, UUID> cacheIds = new HashMap<>();

                List<ObjectId> cachesMongo = asListOfObjectIds(clusterMongo, "caches");

                if (!F.isEmpty(cachesMongo))
                    cachesMongo.forEach(oid -> cacheIds.put(oid, UUID.randomUUID()));

                clusterMongo.put("caches", asStrings(cacheIds.values()));

                Map<ObjectId, UUID> modelIds = new HashMap<>();

                List<ObjectId> modelsMongo = asListOfObjectIds(clusterMongo, "models");

                if (!F.isEmpty(modelsMongo))
                    modelsMongo.forEach(oid -> modelIds.put(oid, UUID.randomUUID()));

                clusterMongo.remove("space");
                clusterMongo.remove("igfss");

                UUID clusterId = UUID.randomUUID();

                clusterMongo.put("id", clusterId.toString());
                clusterMongo.put("models", asStrings(modelIds.values()));

                JsonObject cfg = new JsonObject()
                    .add("cluster", fromJson(mongoToJson(clusterMongo)));

                migrateCaches(cfg, cacheIds, modelIds);

                migrateModels(cfg, modelIds, cacheIds);

                try (Transaction tx = txMgr.txStart()) {
                    cfgsRepo.saveAdvancedCluster(accKey, cfg);

                    migrateConfigurationEx(mongoClusterId, accId, clusterId);

                    tx.commit();
                }
            }
        }
    }

    /**
     * Extension point for migration of extended configuration.
     *
     * @param mongoClusterId Cluster ID in MongoDB.
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     */
    protected void migrateConfigurationEx(ObjectId mongoClusterId, UUID accId, UUID clusterId) {
        // No-op.
    }

    /**
     * @param cfg Configuration.
     * @param cacheIds Links from cache Mongo ID to UUID.
     * @param modelIds Links from model Mongo ID to UUID.
     */
    private void migrateCaches(
        JsonObject cfg,
        Map<ObjectId, UUID> cacheIds,
        Map<ObjectId, UUID> modelIds
    ) {
        log.info(off(3, "Migrating cluster caches: " + cacheIds.size()));

        MongoCollection<Document> cachesCollection = mongoDb.getCollection("caches");

        JsonArray caches = new JsonArray();

        cacheIds.forEach((mongoId, cacheId) -> {
            try (MongoCursor<Document> cursor = cachesCollection.find(Filters.eq("_id", mongoId)).iterator()) {
                while(cursor.hasNext()) {
                    Document cacheMongo = cursor.next();

                    String cacheName = cacheMongo.getString("name");

                    log.info(off(4, "Migrating cache: [_id=" + mongoId + ", name=" + cacheName + "]"));

                    cacheMongo.remove("clusters");

                    List<ObjectId> cacheDomains = asListOfObjectIds(cacheMongo, "domains");

                    cacheMongo.put("id", cacheId.toString());
                    cacheMongo.put("domains", mongoIdsToNewIds(cacheDomains, modelIds));

                    caches.add(fromJson(mongoToJson(cacheMongo)));
                }
            }
        });

        cfg.add("caches", caches);
    }

    /**
     * @param cfg Configuration.
     * @param modelIds Links from model Mongo ID to UUID.
     * @param cacheIds Links from cache Mongo ID to UUID.
     */
    private void migrateModels(
        JsonObject cfg,
        Map<ObjectId, UUID> modelIds,
        Map<ObjectId, UUID> cacheIds
    ) {
        log.info(off(3, "Migrating cluster models: " + modelIds.size()));

        MongoCollection<Document> modelsCollection = mongoDb.getCollection("domainmodels");

        JsonArray models = new JsonArray();

        modelIds.forEach((mongoId, modelId) -> {
            try (MongoCursor<Document> cursor = modelsCollection.find(Filters.eq("_id", mongoId)).iterator()) {
                while(cursor.hasNext()) {
                    Document modelMongo = cursor.next();

                    log.info(off(4, "Migrating model: [_id=" + mongoId +
                        ", keyType=" + modelMongo.getString("keyType") +
                        ", valueType=" + modelMongo.getString("valueType") + "]"));

                    modelMongo.remove("clusters");

                    List<ObjectId> modelCaches = asListOfObjectIds(modelMongo, "caches");

                    modelMongo.put("id", modelId.toString());
                    modelMongo.put("caches",  mongoIdsToNewIds(modelCaches, cacheIds));

                    models.add(fromJson(mongoToJson(modelMongo)));
                }
            }
        });

        cfg.add("models", models);
    }

    /**
     * @param space Account space.
     * @param accId Account ID.
     */
    private void migrateActivities(Document space, UUID accId) {
        MongoCollection<Document> activitiesCollection = mongoDb.getCollection("activities");

        long cnt = activitiesCollection.countDocuments(Filters.eq("owner", space.getObjectId("owner")));

        log.info(off(2, "Migrating activities: " + cnt));

        try (MongoCursor<Document> cursor = activitiesCollection.find(Filters.eq("owner", space.getObjectId("owner"))).iterator()) {
            while (cursor.hasNext()) {
                Document activityMongo = cursor.next();

                String action = activityMongo.getString("action");

                if (action.contains("/igfs"))
                    continue;

                Date date = activityMongo.getDate("date");

                log.info(off(3, "Migrating activity: [_id=" + activityMongo.getObjectId("_id") +
                    ", action=" + action +
                    ", date=" + date +
                    "]"));

                Number amount = (Number)activityMongo.get("amount");

                Activity activity = new Activity(
                    UUID.randomUUID(),
                    activityMongo.getString("group"),
                    action,
                    amount != null ? amount.intValue() : 0
                );

                ActivityKey key = new ActivityKey(accId, date.getTime());

                activitiesRepo.save(key, activity);
            }
        }
    }
}
