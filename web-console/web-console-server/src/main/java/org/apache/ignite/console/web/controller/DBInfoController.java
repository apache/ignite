package org.apache.ignite.console.web.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import io.swagger.v3.oas.annotations.Operation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.DBInfoDto;
import org.apache.ignite.console.repositories.DBInfoRepository;
import org.apache.ignite.console.services.DataSourceInfoService;
import org.bson.Document;
import org.eclipse.jetty.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.LinkedCaseInsensitiveMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for datasources API.
 */
@RestController
@RequestMapping(path = "/api/v1/datasource")
public class DBInfoController {
    /** */
    private final DBInfoRepository datasourcesSrv;
    
    private final DataSourceInfoService dbInfoService;

    /**
     * @param datasourcesSrv Notebooks service.
     */
    @Autowired
    public DBInfoController(DBInfoRepository datasourcesSrv,DataSourceInfoService dbInfoService) {
        this.datasourcesSrv = datasourcesSrv;
		this.dbInfoService = dbInfoService;
    }
    
    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "get user's datasource.")
    @GetMapping(path = "/{datasourceId}")
    public ResponseEntity<DBInfoDto> get(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId
    ) {
    	DBInfoDto dto = datasourcesSrv.get(acc.getId(), datasourceId);
    	
        return ResponseEntity.ok(dto);
    }

    /**
     * @param acc Account.
     * @return Collection of datasources.
     */
    @Operation(summary = "Get user's datasources.")
    @GetMapping
    public ResponseEntity<Collection<DBInfoDto>> list(@AuthenticationPrincipal Account acc) {
        return ResponseEntity.ok(datasourcesSrv.list(acc.getId()));
    }
    
    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "get user's datasource databases.")
    @GetMapping(path = "/{datasourceId}/databases")
    public ResponseEntity<JsonArray> getDatabases(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId
    ) {
    	DBInfoDto dto = datasourcesSrv.get(acc.getId(), datasourceId);
    	if(!dto.getDb().equalsIgnoreCase("mongodb")) {
    		throw new NotImplementedException("Get collections only support mongodb");
    	}
    	String mongoUri = dto.getJdbcUrl();
    	
    	JsonArray list = new JsonArray();

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
        	
            ListDatabasesIterable<Document> collections = mongoClient.listDatabases();            
            collections.forEach((row)->{
            	list.add(new JsonObject(row));
            });
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
        return ResponseEntity.ok(list);
    }
    
    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "get user's datasource collections.")
    @GetMapping(path = "/{datasourceId}/{database}/collections")
    public ResponseEntity<JsonArray> getCollections(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId,
        @PathVariable("database") String databaseName,
        @RequestParam(value="columns",required=false) boolean columns
    ) {
    	DBInfoDto dto = datasourcesSrv.get(acc.getId(), datasourceId);
    	if(!dto.getDb().equalsIgnoreCase("mongodb")) {
    		throw new NotImplementedException("Get collections only support mongodb");
    	}
    	String mongoUri = dto.getJdbcUrl();
    	
    	JsonArray list = new JsonArray();

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
        	String schema = dto.getSchemaName();
        	if(StringUtil.isBlank(schema)) {
        		schema = "admin";
        	}
        	if(StringUtil.isBlank(databaseName)) {
        		databaseName = schema;
        	}
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            ListCollectionsIterable<Document> collections = database.listCollections();            
            collections.forEach((row)->{
            	JsonObject table = new JsonObject(row);            	
            	list.add(table);
            });
            
            if(columns) {
            	JsonArray samples = new JsonArray();
            	for(int pos =0;pos<list.size();pos++) {
            		JsonObject table = list.getJsonObject(pos);
                	String collectionName = table.getString("name");
    	            MongoCollection<Document> collection = database.getCollection(collectionName);
    	            
    	            collection.find().limit(5).forEach((row)->{
    	            	samples.add(new JsonObject(row));
    	            });
    	            
    	            table.put("columns", dbInfoService.fieldsInfo(samples));
    	            samples.clear();
                }
        	}
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
        return ResponseEntity.ok(list);
    }
    
    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     * @param collection meta collection, must have: "TABLE_SCHEMA","TABLE_NAME","TABLE_COMMENT","COLUMN_NAME","COLUMN_COMMENT","DATA_TYPE"
     */
    @Operation(summary = "get user's datasource collections from meta collection.")
    @GetMapping(path = "/{datasourceId}/meta_collection/{database}/{collection}")
    public ResponseEntity<JsonArray> getMetaCollections(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId,
        @PathVariable("database") String databaseName,
        @PathVariable("collection") String collectionName
        
    ) {
    	DBInfoDto dto = datasourcesSrv.get(acc.getId(), datasourceId);
    	if(!dto.getDb().equalsIgnoreCase("mongodb")) {
    		throw new NotImplementedException("Get collections only support mongodb");
    	}
    	String mongoUri = dto.getJdbcUrl();
    	
    	JsonArray list = new JsonArray();

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
        	String schema = dto.getSchemaName();
        	if(StringUtil.isBlank(schema)) {
        		schema = "admin";
        	}
        	if(StringUtil.isBlank(databaseName)) {
        		databaseName = schema;
        	}
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            JsonArray samples = new JsonArray();
        	MongoCollection<Document> collection = database.getCollection(collectionName);
            
        	Document sort = new Document("TABLE_SCHEMA",1);
        	sort.append("TABLE_NAME",1);
        	
            collection.find().sort(sort).forEach((row)->{
            	samples.add(new JsonObject(row));
            });
            
            
            JsonObject table = null;
            String lastTablename = "";
            for(int i=0;i<samples.size();i++) {
            	JsonObject row = samples.getJsonObject(i);
            	String name = row.getString("TABLE_NAME");
            	
            	JsonArray columns = null;
            	if(!name.equalsIgnoreCase(lastTablename) || table==null) {
            		table = new JsonObject(); 
                	table.put("schema", row.getString("TABLE_SCHEMA"));
                	table.put("name", name);
                	table.put("comment", row.getString("TABLE_COMMENT"));
                	
                	columns = new JsonArray();
                	table.put("columns", columns);
                	list.add(table);
            	}
            	else {
            		columns = table.getJsonArray("columns");
            	}
            	
            	JsonObject column = new JsonObject();
            	column.put("name", row.getString("COLUMN_NAME"));
            	column.put("typeName", row.getString("DATA_TYPE"));
            	column.put("comment", row.getString("COLUMN_COMMENT"));
            	column.put("type", 1111);
            	column.put("unsigned", false);
            	column.put("nullable", row.getBoolean("IS_NULLABLE",true));
            	String isKey = row.getString("COLUMN_KEY",""); // 
            	String is_generated = row.getString("is_generated","NO");
            	if(!isKey.isEmpty() || !is_generated.equals("NO")) {
            		column.put("key", true);
            		column.put("nullable", false);
            	}
            	else {
	            	String cname = column.getString("name").toLowerCase();
	            	if(cname.equals("id") || cname.equals("_id") || cname.equals("_key") || cname.equalsIgnoreCase(name+"_id")) {
	            		column.put("key", true);
	            		column.put("nullable", false);
	            	}
            	}
            	columns.add(column);
            	
            	lastTablename = name;
            }            
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
        return ResponseEntity.ok(list);
    }
    
    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "get user's datasource data.")
    @GetMapping(path = "/{datasourceId}/{database}/{collection}/samples")
    public ResponseEntity<JsonArray> getSamples(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId,
        @PathVariable("database") String databaseName,
        @PathVariable("collection") String collectionName,
        @RequestParam(value="limit",required=false) int limit
        
    ) {
    	DBInfoDto dto = datasourcesSrv.get(acc.getId(), datasourceId);
    	if(!dto.getDb().equalsIgnoreCase("mongodb")) {
    		throw new NotImplementedException("Get samples only support mongodb");
    	}
    	String mongoUri = dto.getJdbcUrl();
    	
    	JsonArray list = new JsonArray();

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
        	String schema = dto.getSchemaName();
        	if(StringUtil.isBlank(schema)) {
        		schema = "admin";
        	}
        	if(StringUtil.isBlank(databaseName)) {
        		databaseName = schema;
        	}
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            
            collection.find().limit(limit).forEach((row)->{
            	list.add(new JsonObject(row));
            });
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
        return ResponseEntity.ok(list);
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Save user's datasource.")
    @PutMapping(consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<UUID> save(@AuthenticationPrincipal Account acc, @RequestBody DBInfoDto datasource) {
        if(datasource.getId()==null) {
        	datasource.setId(UUID.randomUUID());
        	datasource.setAccId(acc.getId());
        }
        if(datasource.getAccId()==null) {        	
        	datasource.setAccId(acc.getId());
        }
    	datasourcesSrv.save(acc.getId(), datasource);

        return ResponseEntity.ok().body(datasource.getId());
    }

    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "Delete user's datasource.")
    @DeleteMapping(path = "/{datasourceId}")
    public ResponseEntity<Void> delete(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId
    ) {
        datasourcesSrv.delete(acc.getId(), datasourceId);

        return ResponseEntity.ok().build();
    }
}
