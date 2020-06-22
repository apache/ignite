package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.TestUtils;
import de.bwaldvogel.mongo.bson.Document;

class GraphLookupStageTest extends AbstractLookupStageTest {

    @Mock
    @SuppressWarnings("rawtypes")
    private MongoCollection employeesCollection;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(database.resolveCollection("employees", false)).thenReturn(employeesCollection);
    }

    @Test
    void testGraphLookupObjectThatHasLinkedDocuments() {
        GraphLookupStage graphLookupStage = buildGraphLookupStage("from: 'employees', 'startWith': '$manager', 'connectFromField': 'manager', 'connectToField': 'name', 'as': 'managers', 'depthField': 'order'");
        configureEmployeesCollection("name: 'Dave'", "_id: 2, name: 'Dave', manager: 'Mike'");
        configureEmployeesCollection("name: 'Mike'", "_id: 1, name: 'Mike'");
        Document document = json("name: 'Bob', manager: 'Dave'");

        Stream<Document> result = graphLookupStage.apply(Stream.of(document));

        assertThat(result).containsExactly(
            json("name: 'Bob', manager: 'Dave', managers: [{_id: 1, name: 'Mike', 'order': NumberLong(1)}, {_id: 2, name: 'Dave', 'manager': 'Mike', 'order': NumberLong(0)}]")
        );
    }

    private GraphLookupStage buildGraphLookupStage(String jsonDocument) {
        return new GraphLookupStage(json(jsonDocument), database);
    }

    private void configureEmployeesCollection(String expectedJsonQuery, String... employees) {
        when(employeesCollection.handleQueryAsStream(json(expectedJsonQuery)))
            .thenAnswer(invocation -> Stream.of(employees).map(TestUtils::json));
    }

}
