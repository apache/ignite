package de.bwaldvogel.mongo.backend.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class PostgresqlBackend extends AbstractMongoBackend {

    private final DataSource dataSource;

    public PostgresqlBackend(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    protected MongoDatabase openOrCreateDatabase(String databaseName) {
        String sql = "CREATE TABLE IF NOT EXISTS " + databaseName + "._meta" +
            " (collection_name text," +
            "  datasize bigint," +
            " CONSTRAINT pk_meta PRIMARY KEY (collection_name)" +
            ")";
        try (Connection connection = getConnection();
             PreparedStatement stmt1 = connection.prepareStatement("CREATE SCHEMA IF NOT EXISTS " + databaseName);
             PreparedStatement stmt2 = connection.prepareStatement(sql)
        ) {
            stmt1.executeUpdate();
            stmt2.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to open or create database", e);
        }

        return new PostgresqlDatabase(databaseName, this);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
