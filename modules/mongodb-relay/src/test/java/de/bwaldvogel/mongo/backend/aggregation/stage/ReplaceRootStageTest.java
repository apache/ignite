package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ReplaceRootStageTest {

    @Test
    void testReplaceRoot() throws Exception {
        assertThat(replaceRoot(json("newRoot: '$a'"), json("a: { b: { c: 1 } }"))).isEqualTo(json("b: { c: 1 }"));
        assertThat(replaceRoot(json("newRoot: '$a.b'"), json("a: { b: { c: 1 } }"))).isEqualTo(json("c: 1"));
        assertThat(replaceRoot(json("newRoot: { x: '$a.b' }"), json("a: { b: { c: 1 } }"))).isEqualTo(json("x: { c: 1 }"));
    }

    private static Document replaceRoot(Document replaceRoot, Document document) {
        return new ReplaceRootStage(replaceRoot).replaceRoot(document);
    }

    @Test
    void testIllegalReplaceRoot() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new ReplaceRootStage(json("")))
            .withMessage("[Error 40231] no newRoot specified for the $replaceRoot stage");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new ReplaceRootStage(json("newRoot: 1")).replaceRoot(json("a: { b: {} }")))
            .withMessage("[Error 40228] 'newRoot' expression must evaluate to an object, but resulting value was: 1. Type of resulting value: 'int'. Input document: {a: {b: {}}}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new ReplaceRootStage(json("newRoot: 'x'")).replaceRoot(json("a: { b: {} }")))
            .withMessage("[Error 40228] 'newRoot' expression must evaluate to an object, but resulting value was: \"x\". Type of resulting value: 'string'. Input document: {a: {b: {}}}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new ReplaceRootStage(json("newRoot: '$c'")).replaceRoot(json("a: { b: {} }")))
            .withMessage("[Error 40228] 'newRoot' expression must evaluate to an object, but resulting value was: null. Type of resulting value: 'missing'. Input document: {a: {b: {}}}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new ReplaceRootStage(json("newRoot: '$a.c'")).replaceRoot(json("a: { b: {} }")))
            .withMessage("[Error 40228] 'newRoot' expression must evaluate to an object, but resulting value was: null. Type of resulting value: 'missing'. Input document: {a: {b: {}}}");
    }

}
