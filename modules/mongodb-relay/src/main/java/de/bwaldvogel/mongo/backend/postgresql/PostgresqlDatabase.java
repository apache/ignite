package de.bwaldvogel.mongo.backend.postgresql;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.postgresql.index.PostgresUniqueIndex;
import de.bwaldvogel.mongo.exception.MongoServerException;

/**
 *  todo@byron
 *  #使用pg bjson来存储mongo document， 
 *  #为提高查询性能
 * @author WBPC1158
 *
 */
public class PostgresqlDatabase extends AbstractMongoDatabase<Long> {

    private final PostgresqlBackend backend;

    public PostgresqlDatabase(String databaseName, PostgresqlBackend backend) {
        super(databaseName, backend);
        this.backend = backend;
        initializeNamespacesAndIndexes();
    }

    @Override
    public void drop() {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("DROP SCHEMA " + getSchemaName() + " CASCADE")
        ) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to drop " + this, e);
        }
    }

    private String getSchemaName() {
        return getSchemaName(getDatabaseName());
    }

    @Override
    protected long getFileSize() {
        return 0;
    }

    @Override
    protected long getStorageSize() {
        return 0;
    }

    @Override
    protected Index<Long> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
        PostgresUniqueIndex index = new PostgresUniqueIndex(backend, databaseName, collectionName, indexName, keys, sparse);
        index.initialize();
        return index;
    }
    
    @Override
    protected Index<Long> openOrCreateIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
    	 PostgresUniqueIndex index = new PostgresUniqueIndex(backend, databaseName, collectionName, indexName, keys, sparse, false);
         index.initialize();
         return index;
    }

    @Override
    public void dropCollection(String collectionName) {
        super.dropCollection(collectionName);
        String fullCollectionName = PostgresqlCollection.getQualifiedTablename(getDatabaseName(), collectionName);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("DROP TABLE " + fullCollectionName + "")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to drop collection " + collectionName, e);
        }
    }

    @Override
    protected MongoCollection<Long> openOrCreateCollection(String collectionName, String idField) {
        String tableName = PostgresqlCollection.getTablename(collectionName);
        String fullCollectionName = PostgresqlCollection.getQualifiedTablename(getDatabaseName(), collectionName);
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + fullCollectionName + "" +
            " (id serial," +
            "  data json," +
            " CONSTRAINT \"pk_" + tableName + "\" PRIMARY KEY (id)" +
            ")";
        String insertSql = "INSERT INTO " + getDatabaseName() + "._meta (collection_name, datasize) VALUES (?, 0) ON CONFLICT DO NOTHING";
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt1 = connection.prepareStatement(createTableSql);
             PreparedStatement stmt2 = connection.prepareStatement(insertSql)) {
            stmt1.executeUpdate();
            stmt2.setString(1, collectionName);
            stmt2.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to create or open collection " + collectionName, e);
        }

        return new PostgresqlCollection(this, collectionName, idField);
    }

    public PostgresqlBackend getBackend() {
        return backend;
    }

    static String getSchemaName(String databaseName) {
        if (!databaseName.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException("Illegal database name: " + databaseName);
        }
        return databaseName;
    }

}
