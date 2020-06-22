package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class AddFieldsStageTest {

    @Test
    void testAddFields() throws Exception {
        assertThat(addFields(json("a: 'value'"), json("a: true"))).isEqualTo(json("a: true"));
        assertThat(addFields(json("_id: 1"), json("a: 10"))).isEqualTo(json("_id: 1, a: 10"));
        assertThat(addFields(json("_id: 1, a: 'value'"), json("b: '$a'"))).isEqualTo(json("_id: 1, a: 'value', b: 'value'"));
        assertThat(addFields(json("a: 123"), json("a: '$missing'"))).isEqualTo(json(""));
        assertThat(addFields(json("_id: 1"), json("b: '$a'"))).isEqualTo(json("_id: 1"));
        assertThat(addFields(json("_id: 1, a: 'value'"), json("_id: null"))).isEqualTo(json("_id: null, a: 'value'"));
        assertThat(addFields(json("_id: null, value: 25"), json("_id: 1, a: {b: '$value'}"))).isEqualTo(json("_id: 1, value: 25, a: {b: 25}"));
        assertThat(addFields(json("a: {b: 1}"), json("a: {c: '$a.b'}"))).isEqualTo(json("a: {b: 1, c: 1}"));
        assertThat(addFields(json("_id: 1"), json("_id: {b: 1, m: '$missing'}"))).isEqualTo(json("_id: {b: 1}"));
    }

    private static Document addFields(Document document, Document addFields) {
        return new AddFieldsStage(addFields).projectDocument(document);
    }

    @Test
    void testIllegalSpecification() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new AddFieldsStage(json("")))
            .withMessage("[Error 40177] Invalid $addFields :: caused by :: specification must have at least one field");
    }

}
