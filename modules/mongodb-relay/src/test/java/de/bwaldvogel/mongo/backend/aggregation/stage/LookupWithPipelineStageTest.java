package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.exception.MongoServerException;


public class LookupWithPipelineStageTest extends AbstractLookupStageTest {

    @Test
    void testUnexpectedConfigurationField() {
        assertThatThrownBy(() -> buildLookupStage("from: 'authors', pipeline: [], as: 'author', unknown: 'hello'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] unknown argument to $lookup: unknown");
    }

    private void buildLookupStage(String jsonDocument) {
        new LookupWithPipelineStage(json(jsonDocument), database);
    }

}
