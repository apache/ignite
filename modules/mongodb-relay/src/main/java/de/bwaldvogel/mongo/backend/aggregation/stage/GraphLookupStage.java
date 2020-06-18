package de.bwaldvogel.mongo.backend.aggregation.stage;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.FailedToParseException;

public class GraphLookupStage extends AbstractLookupStage {

    private static final String START_WITH = "startWith";
    private static final String CONNECT_FROM_FIELD = "connectFromField";
    private static final String CONNECT_TO_FIELD = "connectToField";
    private static final String MAX_DEPTH = "maxDepth";
    private static final String DEPTH_FIELD = "depthField";

    private static final Set<String> CONFIGURATION_KEYS;
    static {
        CONFIGURATION_KEYS = new HashSet<>();
        CONFIGURATION_KEYS.add(FROM);
        CONFIGURATION_KEYS.add(START_WITH);
        CONFIGURATION_KEYS.add(CONNECT_FROM_FIELD);
        CONFIGURATION_KEYS.add(CONNECT_TO_FIELD);
        CONFIGURATION_KEYS.add(AS);
        CONFIGURATION_KEYS.add(MAX_DEPTH);
        CONFIGURATION_KEYS.add(DEPTH_FIELD);
    }

    private final String connectFromField;
    private final String connectToField;
    private final String asField;
    private final Integer maxDepth;
    private final String depthField;

    private final MongoCollection<?> collection;

    public GraphLookupStage(Document configuration, MongoDatabase mongoDatabase) {
        stageName = "$graphLookup";
        String from = readStringConfigurationProperty(configuration, FROM);
        collection = mongoDatabase.resolveCollection(from, false);
        readStringConfigurationProperty(configuration, FROM);
        readVariableConfigurationProperty(configuration, START_WITH);
        connectFromField = readStringConfigurationProperty(configuration, CONNECT_FROM_FIELD);
        connectToField = readStringConfigurationProperty(configuration, CONNECT_TO_FIELD);
        asField = readStringConfigurationProperty(configuration, AS);
        maxDepth = readOptionalIntegerConfigurationProperty(configuration, MAX_DEPTH);
        depthField = readOptionalStringConfigurationProperty(configuration, DEPTH_FIELD);
        ensureAllConfigurationPropertiesAreKnown(configuration, CONFIGURATION_KEYS);
    }

    Integer readOptionalIntegerConfigurationProperty(Document configuration, String name) {
        Object value = configuration.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        throw new FailedToParseException("'" + name + "' option to \" + stageName + \" must be a integer, but was type " + Utils.describeType(value));
    }

    String readOptionalStringConfigurationProperty(Document configuration, String name) {
        Object value = configuration.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw new FailedToParseException("'" + name + "' option to \" + stageName + \" must be a string, but was type " + Utils.describeType(value));
    }

    String readVariableConfigurationProperty(Document configuration, String name) {
        Object value = configuration.get(name);
        if (value == null) {
            throw new FailedToParseException("missing '" + name + "' option to $graphLookup stage specification: " + configuration);
        }
        if (value instanceof String) {
            String ret = (String) value;
            if (ret.startsWith("$")) {
                ret = ret.substring(1);
            }
            return ret;
        }
        throw new FailedToParseException("'" + name + "' option to $graphLookup must be a string, but was type " + Utils.describeType(value));
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::connectRemoteDocument);
    }

    private Document connectRemoteDocument(Document document) {
        Object value = document.get(connectFromField);
        List<Document> documentList = new ArrayList<>();
        findLinkedDocuments(0, documentList, value);
        Document result = document.clone();
        result.put(asField, documentList);
        return result;
    }

    private List<Document> findLinkedDocuments(long depth, final List<Document> linked, Object value) {

        if (maxDepth != null && depth > maxDepth) {
            return linked;
        }

        if (value instanceof List) {
            return ((List<?>) value).stream()
                .flatMap(item -> findLinkedDocuments(depth + 1, linked, item).stream())
                .collect(toList());
        }
        Document query = new Document(connectToField, value);

        List<Document> newlyLinkedDocuments = collection.handleQueryAsStream(query).collect(toList());
        for (Document newDocument : newlyLinkedDocuments) {
            Object newValue = newDocument.get(connectFromField);
            if (depthField != null) {
                newDocument.put(depthField, depth);
            }
            linked.add(0, newDocument);
            findLinkedDocuments(depth + 1, linked, newValue);
        }
        return linked;
    }

}
