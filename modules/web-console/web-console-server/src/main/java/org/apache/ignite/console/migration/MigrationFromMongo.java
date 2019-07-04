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
import com.mongodb.ConnectionString;
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
    @Value("${migration.mongo.db.url:}")
    private String mongoDbUrl;

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
        if (F.isEmpty(mongoDbUrl)) {
            log.info("MongoDB URL was not specified. Migration disabled.");

            return;
        }

        if (txMgr.doInTransaction(accRepo::hasUsers)) {
            log.warn("Database was already migrated. Consider to disable migration in application settings.");

            return;
        }

        log.info("Migration started...");

        ConnectionString connStr = new ConnectionString(mongoDbUrl);

        MongoClient mongoClient = MongoClients.create(connStr);

        try {
            mongoDb = mongoClient.getDatabase(connStr.getDatabase());

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
        MongoCollection<Document> accountsCol = mongoDb.getCollection("accounts");
        MongoCollection<Document> spacesCol = mongoDb.getCollection("spaces");

        log.info("Accounts to migrate: {}", accountsCol.countDocuments());

        try (MongoCursor<Document> cursor = accountsCol.find().iterator()) {
            while (cursor.hasNext()) {
                Document accMongo = cursor.next();

                ObjectId mongoAccId = accMongo.getObjectId("_id");
                String email = accMongo.getString("email");

                Document space = spacesCol.find(
                    Filters.and(
                        Filters.eq("owner", mongoAccId),
                        Filters.eq("demo", false)
                    )
                ).first();

                if (space == null) {
                    log.warn("Space not found [owner=" + mongoAccId + ", email=" + email + "]");

                    continue;
                }

                log.info("Migrating account [_id={}, email={}]", mongoAccId, email);

                txMgr.doInTransaction(() -> {
                    Account acc = createAccount(accMongo);

                    accRepo.create(acc);

                    migrateAccountObjects(accMongo, space, acc.getId());
                });
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
        MongoCollection<Document> notebooksCol = mongoDb.getCollection("notebooks");

        ObjectId spaceId = space.getObjectId("_id");

        long cnt = notebooksCol.countDocuments(Filters.eq("space", spaceId));

        log.info("Migrating notebooks: {}", cnt);

        try (MongoCursor<Document> cursor = notebooksCol.find(Filters.eq("space", spaceId)).iterator()) {
            while (cursor.hasNext()) {
                Document notebookMongo = cursor.next();

                log.info("Migrating notebook: [_id={}, name={}]", notebookMongo.getObjectId("_id"), notebookMongo.getString("name"));

                Notebook notebook = new Notebook();
                notebook.setId(UUID.randomUUID());
                notebook.setName(notebookMongo.getString("name"));

                notebook.setExpandedParagraphs(asPrimitives(notebookMongo, "expandedParagraphs"));

                List<Document> paragraphsMongo = (List<Document>)notebookMongo.get("paragraphs");

                Notebook.Paragraph[] paragraphs = paragraphsMongo
                    .stream()
                    .map(paragraphMongo -> {
                        Notebook.Paragraph paragraph = new Notebook.Paragraph();

                        paragraph.setName(paragraphMongo.getString("name"));
                        paragraph.setCacheName(paragraphMongo.getString("cacheName"));
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

        log.info("Migrating configurations: {}", cnt);

        ConfigurationKey accKey = new ConfigurationKey(accId, false);

        try (MongoCursor<Document> cursor = clusters.find(Filters.eq("space", space.getObjectId("_id"))).iterator()) {
            while (cursor.hasNext())
                migrateCluster(accKey, UUID.randomUUID(), cursor.next());
        }
    }

    /**
     * @param accKey Account key.
     * @param clusterId Cluster ID.
     * @param clusterMongo Mongo document with cluster.
     */
    protected void migrateCluster(ConfigurationKey accKey, UUID clusterId, Document clusterMongo) {
        log.info("Migrating cluster: [_id={}, name={}]",
            clusterMongo.getObjectId("_id"), clusterMongo.getString("name"));

        Map<ObjectId, UUID> cacheIds = new HashMap<>();

        List<ObjectId> cachesMongo = asListOfObjectIds(clusterMongo, "caches");

        if (!F.isEmpty(cachesMongo))
            cachesMongo.forEach(oid -> cacheIds.put(oid, UUID.randomUUID()));

        clusterMongo.put("caches", asStrings(cacheIds.values()));

        Map<ObjectId, UUID> mdlIds = new HashMap<>();

        List<ObjectId> modelsMongo = asListOfObjectIds(clusterMongo, "models");

        if (!F.isEmpty(modelsMongo))
            modelsMongo.forEach(oid -> mdlIds.put(oid, UUID.randomUUID()));

        clusterMongo.remove("space");
        clusterMongo.remove("igfss");

        clusterMongo.put("id", clusterId.toString());
        clusterMongo.put("models", asStrings(mdlIds.values()));

        JsonObject cfg = new JsonObject()
            .add("cluster", fromJson(mongoToJson(clusterMongo)));

        migrateCaches(cfg, cacheIds, mdlIds);

        migrateModels(cfg, mdlIds, cacheIds);

        cfgsRepo.saveAdvancedCluster(accKey, cfg);
    }

    /**
     * @param cfg Configuration.
     * @param cacheIds Links from cache Mongo ID to UUID.
     * @param mdlIds Links from model Mongo ID to UUID.
     */
    private void migrateCaches(
        JsonObject cfg,
        Map<ObjectId, UUID> cacheIds,
        Map<ObjectId, UUID> mdlIds
    ) {
        log.info("Migrating cluster caches: {}", cacheIds.size());

        MongoCollection<Document> cachesCol = mongoDb.getCollection("caches");

        JsonArray caches = new JsonArray();

        cacheIds.forEach((mongoId, cacheId) -> {
            try (MongoCursor<Document> cursor = cachesCol.find(Filters.eq("_id", mongoId)).iterator()) {
                while(cursor.hasNext()) {
                    Document cacheMongo = cursor.next();

                    String cacheName = cacheMongo.getString("name");

                    log.info("Migrating cache: [_id={}, name={}]", mongoId, cacheName);

                    cacheMongo.remove("clusters");

                    List<ObjectId> cacheDomains = asListOfObjectIds(cacheMongo, "domains");

                    cacheMongo.put("id", cacheId.toString());
                    cacheMongo.put("domains", mongoIdsToNewIds(cacheDomains, mdlIds));

                    caches.add(fromJson(mongoToJson(cacheMongo)));
                }
            }
        });

        cfg.add("caches", caches);
    }

    /**
     * @param cfg Configuration.
     * @param mdlIds Links from model Mongo ID to UUID.
     * @param cacheIds Links from cache Mongo ID to UUID.
     */
    private void migrateModels(
        JsonObject cfg,
        Map<ObjectId, UUID> mdlIds,
        Map<ObjectId, UUID> cacheIds
    ) {
        log.info("Migrating cluster models: {}", mdlIds.size());

        MongoCollection<Document> modelsCol = mongoDb.getCollection("domainmodels");

        JsonArray models = new JsonArray();

        mdlIds.forEach((mongoId, modelId) -> {
            try (MongoCursor<Document> cursor = modelsCol.find(Filters.eq("_id", mongoId)).iterator()) {
                while(cursor.hasNext()) {
                    Document mdlMongo = cursor.next();

                    log.info("Migrating model: [_id={}, keyType={}, valueType={}]",
                        mongoId,
                        mdlMongo.getString("keyType"),
                        mdlMongo.getString("valueType"));

                    mdlMongo.remove("clusters");

                    List<ObjectId> mdlCaches = asListOfObjectIds(mdlMongo, "caches");

                    mdlMongo.put("id", modelId.toString());
                    mdlMongo.put("caches",  mongoIdsToNewIds(mdlCaches, cacheIds));

                    models.add(fromJson(mongoToJson(mdlMongo)));
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
        MongoCollection<Document> activitiesCol = mongoDb.getCollection("activities");

        long cnt = activitiesCol.countDocuments(Filters.eq("owner", space.getObjectId("owner")));

        log.info("Migrating activities: {}", cnt);

        try (MongoCursor<Document> cursor = activitiesCol.find(Filters.eq("owner", space.getObjectId("owner"))).iterator()) {
            while (cursor.hasNext()) {
                Document activityMongo = cursor.next();

                String act = activityMongo.getString("action");

                if (act.contains("/igfs"))
                    continue;

                Date date = activityMongo.getDate("date");

                log.info("Migrating activity: [_id={}, action={}, date={}]",
                    activityMongo.getObjectId("_id"), act, date);

                Number amount = (Number)activityMongo.get("amount");

                Activity activity = new Activity(
                    UUID.randomUUID(),
                    accId,
                    activityMongo.getString("group"),
                    act,
                    amount != null ? amount.intValue() : 0
                );

                ActivityKey key = new ActivityKey(accId, date.getTime());

                activitiesRepo.save(key, activity);
            }
        }
    }
}
