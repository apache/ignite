package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnsetStageTest {

    @Test
    void testUnset() throws Exception {
        assertThat(unset("field1",
            json("_id: 1, field1: 'value1'"),
            json("_id: 2, field1: 'value1', field2: 'value2'"),
            json("_id: 3, field2: 'value2'"),
            json("_id: 4")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2, field2: 'value2'"),
                json("_id: 3, field2: 'value2'"),
                json("_id: 4")
            );
    }

    @Test
    void testUnsetMultipleFields() throws Exception {
        assertThat(unset(Arrays.asList("field1", "field2"),
            json("_id: 1, field1: 'value1'"),
            json("_id: 2, field1: 'value1', field2: 'value2'"),
            json("_id: 3, field2: 'value2', field3: 'value3'"),
            json("_id: 4, field3: 'value3'"),
            json("_id: 5")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3, field3: 'value3'"),
                json("_id: 4, field3: 'value3'"),
                json("_id: 5")
            );
    }

    @Test
    void testUnsetWithSubdocument() throws Exception {
        assertThat(unset("fields.field1",
            json("_id: 1, fields: { field1: 'value1' }"),
            json("_id: 2, fields: { field1: 'value1', field2: 'value2' }"),
            json("_id: 3, fields: { field2: 'value2' }"),
            json("_id: 4, fields: { }"),
            json("_id: 5")))
            .containsExactly(
                json("_id: 1, fields: { }"),
                json("_id: 2, fields: { field2: 'value2' }"),
                json("_id: 3, fields: { field2: 'value2' }"),
                json("_id: 4, fields: { }"),
                json("_id: 5")
            );
    }

    @Test
    void testIllegalUnset() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnsetStage(""))
            .withMessage("[Error 40352] Invalid $project :: caused by :: FieldPath cannot be constructed with empty string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnsetStage(Arrays.asList(123, 456)))
            .withMessage("[Error 31120] $unset specification must be a string or an array containing only string values");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnsetStage(123))
            .withMessage("[Error 31120] $unset specification must be a string or an array containing only string values");
    }

    private List<Document> unset(Object input, Document... documents) {
        return new UnsetStage(input).apply(Stream.of(documents)).collect(Collectors.toList());
    }

}
