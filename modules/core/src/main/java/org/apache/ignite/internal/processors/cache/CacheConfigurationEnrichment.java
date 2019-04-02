package org.apache.ignite.internal.processors.cache;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.IgniteException;

public class CacheConfigurationEnrichment implements Serializable {
    /** Field name -> field serialized value. */
    private final Map<String, byte[]> enrichFields;

    /** Field name -> Field value class name. */
    private final Map<String, String> fieldClassNames;

    public CacheConfigurationEnrichment(Map<String, byte[]> enrichFields, Map<String, String> fieldClassNames) {
        this.enrichFields = enrichFields;
        this.fieldClassNames = fieldClassNames;
    }

    public Object getFieldValue(String fieldName) {
        byte[] serializedVal = enrichFields.get(fieldName);

/*
        if (serializedVal == null)
            throw new IgniteException("Field " + fieldName + " doesn't exist in enrichment.");
*/

        ByteArrayInputStream bis = new ByteArrayInputStream(serializedVal);

        try (ObjectInputStream is = new ObjectInputStream(bis)) {
            return is.readObject();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to deserialize field " + fieldName);
        }
    }

    public String getFieldClassName(String fieldName) {
        String fieldClassName = fieldClassNames.get(fieldName);

/*
        if (fieldClassName == null)
            throw new IgniteException("Field " + fieldName + " doesn't exist in enrichment.");
*/

        return fieldClassName;
    }
}
