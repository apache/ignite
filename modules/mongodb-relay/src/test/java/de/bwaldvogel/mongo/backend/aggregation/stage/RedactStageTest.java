package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;

class RedactStageTest {

    @Test
    void testRedact() {
        assertThat(redact(
            json("_id: 1"),
            json("$cond: {if: {$eq: [\"$_id\", 1]}, then: \"$$KEEP\", else: \"$$PRUNE\"}"))
        ).contains(json("_id: 1"));

        assertThat(redact(
            json("_id: 1"),
            json("$cond: {if: {$eq: [\"$_id\", 0]}, then: \"$$PRUNE\", else: \"$$KEEP\"}"))
        ).contains(json("_id: 1"));

        assertThat(redact(
            json("_id: 1"),
            json("$cond: {if: {$eq: [\"$_id\", 1]}, then: \"$$PRUNE\", else: \"$$KEEP\"}"))
        ).isEmpty();

        assertThat(redact(
            json("_id: 1"),
            json("$cond: {if: {$eq: [\"$_id\", 0]}, then: \"$$KEEP\", else: \"$$PRUNE\"}"))
        ).isEmpty();
    }

    private static List<Document> redact(Document input, Document redact) {
        return new RedactStage(redact).apply(Stream.of(input)).collect(Collectors.toList());
    }
}
