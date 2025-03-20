package org.apache.ignite.console.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class CsvToMongoImporter {
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java CsvToMongoImporter <file_path> <database_name> [mongo_uri]");
            System.exit(1);
        }

        String filePath = args[0];
        String dbName = args[1];
        String mongoUri = args.length > 2 ? args[2] : "mongodb://localhost:27017";

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            File inputFile = new File(filePath);

            if (filePath.toLowerCase().endsWith(".zip")) {
                processZipFile(inputFile, database);
            } else if (filePath.toLowerCase().endsWith(".csv")) {
                processCsvFile(inputFile, database);
            } else {
                System.err.println("Unsupported file format. Only .csv and .zip are supported.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processZipFile(File zipFile, MongoDatabase database) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".csv")) {
                    String collectionName = getCollectionNameFromZipEntry(entry);
                    processCsvStream(zipIn, database, collectionName);
                }
                zipIn.closeEntry();
            }
        }
    }

    private static void processCsvFile(File csvFile, MongoDatabase database) throws IOException {
        String collectionName = getCollectionNameFromFile(csvFile);
        try (FileInputStream fis = new FileInputStream(csvFile)) {
            processCsvStream(fis, database, collectionName);
        }
    }

    private static String getCollectionNameFromZipEntry(ZipEntry entry) {
        String fileName = entry.getName();
        // Remove directory path and file extension
        int lastSlash = fileName.lastIndexOf('/');
        if (lastSlash != -1) {
            fileName = fileName.substring(lastSlash + 1);
        }
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
    }

    private static String getCollectionNameFromFile(File file) {
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
    }

    private static void processCsvStream(InputStream inputStream, MongoDatabase database, String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.drop(); // Optional: Remove existing data

        try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            CSVParser parser = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim()
                    .parse(reader);

            List<Document> batch = new ArrayList<>(BATCH_SIZE);
            for (CSVRecord record : parser) {
                Document doc = new Document();
                parser.getHeaderNames().forEach(header -> 
                    doc.append(header, record.get(header))
                );
                batch.add(doc);

                if (batch.size() >= BATCH_SIZE) {
                    collection.insertMany(batch);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                collection.insertMany(batch);
            }
            System.out.println("Imported " + collectionName + " (" + parser.getRecordNumber() + " records)");
        } catch (IOException e) {
            System.err.println("Error processing collection " + collectionName + ": " + e.getMessage());
        }
    }
}