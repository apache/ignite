package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Set;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.FailedToParseException;

abstract class AbstractLookupStage implements AggregationStage {

    protected String stageName = "$lookup";
    static final String FROM = "from";
    static final String AS = "as";

    String readStringConfigurationProperty(Document configuration, String name) {
        Object value = configuration.get(name);
        if (value == null) {
            throw new FailedToParseException("missing '" + name + "' option to " + stageName + " stage specification: " + configuration);
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw new FailedToParseException("'" + name + "' option to " + stageName + " must be a string, but was type " + Utils.describeType(value));
    }

    Document readOptionalDocumentArgument(Document configuration, String name) {
        Object value = configuration.get(name);
        if (value == null) {
            return new Document();
        }
        if (value instanceof Document) {
            return (Document) value;
        }
        throw new FailedToParseException(stageName + " argument '" + name + ": " + Json.toJsonValue(value) + "' must be an object, is type " + Utils.describeType(value));
    }

    void ensureAllConfigurationPropertiesAreKnown(Document configuration, Set<String> configurationKeys) {
        for (String name : configuration.keySet()) {
            if (!configurationKeys.contains(name)) {
                String message = "unknown argument to " + stageName + ": " + name;
                throw new FailedToParseException(message);
            }
        }
    }

}
