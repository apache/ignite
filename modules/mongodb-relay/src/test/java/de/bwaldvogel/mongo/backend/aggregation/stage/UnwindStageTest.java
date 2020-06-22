package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnwindStageTest {

    @Test
    void testUnwind() throws Exception {
        assertThat(unwind("$field",
            json("_id: 1, field: [1, 2, 3]"),
            json("_id: 2, field: [2, 3]"),
            json("_id: 3")))
            .containsExactly(
                json("_id: 1, field: 1"),
                json("_id: 1, field: 2"),
                json("_id: 1, field: 3"),
                json("_id: 2, field: 2"),
                json("_id: 2, field: 3")
            );

        assertThat(unwind(json("path: '$field'"), json("_id: 1, field: ['A', 'B']")))
            .containsExactly(
                json("_id: 1, field: 'A'"),
                json("_id: 1, field: 'B'")
            );
    }

    @Test
    void testUnwindWithSubdocument() throws Exception {
        assertThat(unwind("$field.values",
            json("_id: 1, field: {values: [1, 2, 3]}"),
            json("_id: 2, field: {values: [2, 3]}")))
            .containsExactly(
                json("_id: 1, field: {values: 1}"),
                json("_id: 1, field: {values: 2}"),
                json("_id: 1, field: {values: 3}"),
                json("_id: 2, field: {values: 2}"),
                json("_id: 2, field: {values: 3}")
            );

        assertThat(unwind("$field.values",
            json("_id: 1, field: {x: 1, values: [1, 2, 3]}"),
            json("_id: 2, field: {x: 2, values: [2, 3]}")))
            .containsExactly(
                json("_id: 1, field: {x: 1, values: 1}"),
                json("_id: 1, field: {x: 1, values: 2}"),
                json("_id: 1, field: {x: 1, values: 3}"),
                json("_id: 2, field: {x: 2, values: 2}"),
                json("_id: 2, field: {x: 2, values: 3}")
            );
    }

    private List<Document> unwind(Object input, Document... documents) {
        return new UnwindStage(input).apply(Stream.of(documents)).collect(Collectors.toList());
    }

    @Test
    void testIllegalParameter() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnwindStage("illegalField"))
            .withMessage("[Error 28818] path option to $unwind stage should be prefixed with a '$': illegalField");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnwindStage(null))
            .withMessage("[Error 15981] expected either a string or an object as specification for $unwind stage, got null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnwindStage(json("")))
            .withMessage("[Error 28812] no path specified to $unwind stage");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new UnwindStage(json("path: {}")))
            .withMessage("[Error 28808] expected a string as the path for $unwind stage, got object");
    }

}
