package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.TestUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class LookupStageTest extends AbstractLookupStageTest {

    @Mock
    @SuppressWarnings("rawtypes")
    private MongoCollection authorsCollection;

    @Test
    void testMissingFromField() {
        assertThatThrownBy(() -> buildLookupStage("localField: 'authorId', foreignField: '_id', as: 'author'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] missing 'from' option to $lookup stage specification: " +
                "{\"localField\" : \"authorId\", \"foreignField\" : \"_id\", \"as\" : \"author\"}");
    }

    @Test
    void testMissingLocalFieldField() {
        assertThatThrownBy(() -> buildLookupStage("from: 'authors', foreignField: '_id', as: 'author'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] missing 'localField' option to $lookup stage specification: " +
                "{\"from\" : \"authors\", \"foreignField\" : \"_id\", \"as\" : \"author\"}");
    }

    @Test
    void testMissingForeignFieldField() {
        assertThatThrownBy(() -> buildLookupStage("from: 'authors', localField: 'authorId', as: 'author'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] missing 'foreignField' option to $lookup stage specification: " +
                "{\"from\" : \"authors\", \"localField\" : \"authorId\", \"as\" : \"author\"}");
    }

    @Test
    void testMissingAsField() {
        assertThatThrownBy(() -> buildLookupStage("from: 'authors', localField: 'authorId', foreignField: '_id'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] missing 'as' option to $lookup stage specification: " +
                "{\"from\" : \"authors\", \"localField\" : \"authorId\", \"foreignField\" : \"_id\"}");
    }

    @Test
    void testFromFieldWithWrongType() {
        assertThatThrownBy(() -> buildLookupStage("from: 1, localField: 'authorId', foreignField: '_id', as: 'author'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] 'from' option to $lookup must be a string, but was type int");
    }

    @Test
    void testUnexpectedConfigurationField() {
        assertThatThrownBy(() -> buildLookupStage("from: 'authors', localField: 'authorId', foreignField: '_id', as: 'author', unknown: 'hello'"))
            .isInstanceOf(MongoServerException.class)
            .hasMessage("[Error 9] unknown argument to $lookup: unknown");
    }

    @Test
    void testLookupObjectThatExists() {
        prepareAuthorsCollectionMock();

        LookupStage lookupStage = buildLookupStage("from: 'authors', 'localField': 'authorId', foreignField: '_id', as: 'author'");
        configureAuthorsCollection("_id: 3", "_id: 3, name: 'Uncle Bob'");
        Document document = json("title: 'Clean Code', authorId: 3");

        Stream<Document> result = lookupStage.apply(Stream.of(document));

        assertThat(result).containsExactly(
            json("title: 'Clean Code', authorId: 3, author: [{_id: 3, name: 'Uncle Bob'}]")
        );
    }

    @Test
    void testLookupObjectThatExistsMultipleTimes() {
        prepareAuthorsCollectionMock();

        LookupStage lookupStage = buildLookupStage("from: 'authors', 'localField': 'job', foreignField: 'job', as: 'seeAuthors'");
        configureAuthorsCollection("job: 'Developer'",
            "_id: 3, name: 'Uncle Bob', job: 'Developer'",
            "_id: 5, name: 'Alice', job: 'Developer'");
        Document document = json("_id: 1, title: 'Developing for dummies', job: 'Developer'");

        Stream<Document> result = lookupStage.apply(Stream.of(document));

        assertThat(result).containsExactly(
            json("_id: 1, title: 'Developing for dummies', job: 'Developer', " +
                "seeAuthors: [{_id: 3, name: 'Uncle Bob', job: 'Developer'}, {_id: 5, name: 'Alice', job: 'Developer'}]")
        );
    }

    @Test
    void testLookupObjectThatDoesNotExist() {
        prepareAuthorsCollectionMock();

        LookupStage lookupStage = buildLookupStage("from: 'authors', 'localField': 'job', foreignField: 'job', as: 'seeAuthors'");
        configureAuthorsCollection("job: 'Developer'");
        Document document = json("_id: 1, title: 'Developing for dummies', job: 'Developer'");

        Stream<Document> result = lookupStage.apply(Stream.of(document));

        assertThat(result).containsExactly(
            json("_id: 1, title: 'Developing for dummies', job: 'Developer', seeAuthors: []")
        );
    }

    @Test
    void testLookupObjectBasedOnAnArrayKey() {
        prepareAuthorsCollectionMock();

        LookupStage lookupStage = buildLookupStage("from: 'authors', 'localField': 'authorsIds', foreignField: '_id', as: 'authors'");
        configureAuthorsCollection("_id: 3", "_id: 3, name: 'Uncle Bob'");
        configureAuthorsCollection("_id: 20", "_id: 20, name: 'Martin Fowler'");
        Document document1 = json("title: 'Agile Manifesto', authorsIds: [3, 20, 22]");
        Document document2 = json("title: 'Clean Code', authorsIds: [3]");

        Stream<Document> result = lookupStage.apply(Stream.of(document1, document2));

        assertThat(result).containsExactly(
            json("title: 'Agile Manifesto', authorsIds: [3, 20, 22], authors: [{_id: 3, name: 'Uncle Bob'}, {_id: 20, name: 'Martin Fowler'}]"),
            json("title: 'Clean Code', authorsIds: [3], authors: [{_id: 3, name: 'Uncle Bob'}]")
        );
    }

    @Test
    public void testLookupWithNestedField() throws Exception {
        prepareAuthorsCollectionMock();

        LookupStage lookupStage = buildLookupStage("from: 'authors', 'localField': 'author.id', foreignField: '_id', as: 'author'");
        configureAuthorsCollection("_id: 3", "_id: 3, name: 'Uncle Bob'");
        Document document = json("title: 'Clean Code', author: { id: 3 }");

        Stream<Document> result = lookupStage.apply(Stream.of(document));

        assertThat(result).containsExactly(
            json("title: 'Clean Code', author: { id: 3 }, author: [{_id: 3, name: 'Uncle Bob'}]")
        );
    }

    private LookupStage buildLookupStage(String jsonDocument) {
        return new LookupStage(json(jsonDocument), database);
    }

    private void prepareAuthorsCollectionMock() {
        when(database.resolveCollection("authors", false)).thenReturn(authorsCollection);
    }

    private void configureAuthorsCollection(String expectedJsonQuery, String... authors) {
        when(authorsCollection.handleQueryAsStream(json(expectedJsonQuery)))
            .thenAnswer(invocation -> Stream.of(authors).map(TestUtils::json));
    }
}
