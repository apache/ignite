package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;

public class LookupWithPipelineStage extends AbstractLookupStage {

    private static final String LET_FIELD = "let";
    public static final String PIPELINE_FIELD = "pipeline";

    private static final Set<String> CONFIGURATION_KEYS;

    static {
        CONFIGURATION_KEYS = new HashSet<>();
        CONFIGURATION_KEYS.add(FROM);
        CONFIGURATION_KEYS.add(LET_FIELD);
        CONFIGURATION_KEYS.add(PIPELINE_FIELD);
        CONFIGURATION_KEYS.add(AS);
    }

    private final MongoDatabase mongoDatabase;
    private final MongoCollection<?> collection;
    private final Document let;
    private final Object pipeline;
    private final String as;

    public LookupWithPipelineStage(Document configuration, MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        String from = readStringConfigurationProperty(configuration, FROM);
        collection = mongoDatabase.resolveCollection(from, false);
        let = readOptionalDocumentArgument(configuration, LET_FIELD);
        pipeline = configuration.get(PIPELINE_FIELD);
        as = readStringConfigurationProperty(configuration, AS);
        ensureAllConfigurationPropertiesAreKnown(configuration, CONFIGURATION_KEYS);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::joinDocuments);
    }

    private Document joinDocuments(Document document) {
        Aggregation aggregation = Aggregation.fromPipeline(pipeline, mongoDatabase, collection);
        aggregation.setVariables(evaluateVariables(document));
        List<Document> documents = aggregation.computeResult();
        Document result = document.clone();
        result.put(as, documents);
        return result;
    }

    private Map<String, Object> evaluateVariables(Document document) {
        Map<String, Object> variables = new Document();
        for (Entry<String, Object> entry : let.entrySet()) {
            Object expression = entry.getValue();
            Object value = Expression.evaluateDocument(expression, document);
            variables.put("$" + entry.getKey(), value);
        }
        return variables;
    }

}
