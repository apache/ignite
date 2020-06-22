package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.FailedToOptimizePipelineError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ExpressionTest {

    @Test
    void testEvaluateSimpleValue() throws Exception {
        assertThat(Expression.evaluate(1, json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(null, json(""))).isNull();
        assertThat(Expression.evaluate("abc", json(""))).isEqualTo("abc");
        assertThat(Expression.evaluate("$a", json("a: 123"))).isEqualTo(123);
        assertThat(Expression.evaluate("$a", json(""))).isInstanceOf(Missing.class);
        assertThat(Expression.evaluate("$a", json("a: null"))).isNull();
        assertThat(Expression.evaluate(json("a: 1, b: 2"), json("a: -2"))).isEqualTo(json("a: 1, b: 2"));
    }

    @Test
    void testEvaluateAbs() throws Exception {
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -2"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -2.5"))).isEqualTo(2.5);
        assertThat(Expression.evaluate(json("$abs: ['$a']"), json("a: -2.5"))).isEqualTo(2.5);
        assertThat(Expression.evaluate(new Document("$abs", 123L), json(""))).isEqualTo(123);
        assertThat(Expression.evaluate(json("$abs: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -25"))).isEqualTo(25);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$abs: '$a', $ceil: '$b'"), json("")))
            .withMessage("[Error 15983] An object representing an expression must have exactly one field: {\"$abs\" : \"$a\", \"$ceil\" : \"$b\"}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$abs: 'abc'"), json("")))
            .withMessage("[Error 28765] $abs only supports numeric types, not string");
    }

    @Test
    void testEvaluateAdd() throws Exception {
        assertThat(Expression.evaluate(json("$add: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(12);
        assertThat(Expression.evaluate(json("$add: ['$doesNotExist', 5]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$add: [7.5, 3]"), json(""))).isEqualTo(10.5);
        assertThat(Expression.evaluate(json("$add: [1, 2, 3]"), json(""))).isEqualTo(6);
        assertThat(Expression.evaluate(json("$add: []"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$add: 17"), json(""))).isEqualTo(17);
        assertThat(Expression.evaluate(json("$add: [1, null, 2]"), json(""))).isNull();
        assertThat(Expression.evaluate(new Document("$add",
            Arrays.asList(Instant.ofEpochSecond(1000), Instant.ofEpochSecond(2000))), json("")))
            .isEqualTo(Instant.ofEpochSecond(3000));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: 'abc'"), json("")))
            .withMessage("[Error 16554] $add only supports numeric or date types, not string");
    }

    @Test
    void testEvaluateAnd() throws Exception {
        assertThat(Expression.evaluate(json("$and: [1, 'green']"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: []"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [[null], [false], [0]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: 'abc'"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: '$value'"), json("value: true"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: '$value'"), json("value: false"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: true"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [{$gt: ['$qty', 100]}, {$lt: ['$qty', 250]}]"), json("qty: 150"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [{$gt: ['$qty', 100]}, {$lt: ['$qty', 250]}]"), json("qty: 300"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: false"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: null"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: [null, true]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: [0, true]"), json(""))).isEqualTo(false);
    }

    @Test
    void testEvaluateAnyElementTrue() throws Exception {
        assertThat(Expression.evaluate(json("$anyElementTrue: [[true, false]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: ['$items']"), json("items: [false, true]"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: [[[false]]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: ['$items']"), json("items: [false, false]"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$anyElementTrue: [[null, false, 0]]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$anyElementTrue: [[]]"), json(""))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: null"), json("")))
            .withMessage("[Error 17041] $anyElementTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: [null]"), json("")))
            .withMessage("[Error 17041] $anyElementTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: 'abc'"), json("")))
            .withMessage("[Error 17041] $anyElementTrue's argument must be an array, but is string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $anyElementTrue takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateAllElementsTrue() throws Exception {
        assertThat(Expression.evaluate(json("$allElementsTrue: [[true, 1, 'someString']]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [[[false]]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [[]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: ['$items']"), json("items: [true]"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [[null, false, 0]]"), json(""))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: null"), json("")))
            .withMessage("[Error 17040] $allElementsTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: [null]"), json("")))
            .withMessage("[Error 17040] $allElementsTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: 'abc'"), json("")))
            .withMessage("[Error 17040] $allElementsTrue's argument must be an array, but is string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $allElementsTrue takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateArrayElemAt() throws Exception {
        assertThat(Expression.evaluate(json("$arrayElemAt: [[1, 2, 3], 0]"), json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$arrayElemAt: [[1, 2, 3], 1.0]"), json(""))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$arrayElemAt: [[1, 2, 3], -2]"), json(""))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$arrayElemAt: [[1, 2, 3], 15]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$arrayElemAt: ['$items', 1]"), json("items: ['a', 'b', 'c']"))).isEqualTo("b");
        assertThat(Expression.evaluate(json("$arrayElemAt: ['$items', '$pos']"), json("items: ['a', 'b', 'c'], pos: -1"))).isEqualTo("c");
        assertThat(Expression.evaluate(json("$arrayElemAt: ['$items', '$pos']"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: null"), json("")))
            .withMessage("[Error 16020] Expression $arrayElemAt takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: [1, 2, 3]"), json("")))
            .withMessage("[Error 16020] Expression $arrayElemAt takes exactly 2 arguments. 3 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: ['a', 'b']"), json("")))
            .withMessage("[Error 28689] $arrayElemAt's first argument must be an array, but is string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: [['a', 'b'], 'b']"), json("")))
            .withMessage("[Error 28690] $arrayElemAt's second argument must be a numeric value, but is string");
    }

    @Test
    void testEvaluateArrayToObject() throws Exception {
        assertThat(Expression.evaluate(json("$arrayToObject: {$literal: [['item', 'abc123'], ['qty', 25]]}"), json("")))
            .isEqualTo(json("item: 'abc123', qty: 25"));

        assertThat(Expression.evaluate(json("$arrayToObject: {$literal: [{k: 'item', v: 'abc123'}, {k: 'qty', v: 25}]}"), json("")))
            .isEqualTo(json("item: 'abc123', qty: 25"));

        assertThat(Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [['k', 'v']]")))
            .isEqualTo(json("k: 'v'"));

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: 'str'"), json("")))
            .withMessage("[Error 40386] Failed to optimize pipeline :: caused by :: $arrayToObject requires an array input, found: string");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [1, 2]")))
            .withMessage("[Error 40398] Failed to optimize pipeline :: caused by :: Unrecognised input type format for $arrayToObject: int");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [[1, 2, 3]]")))
            .withMessage("[Error 40397] Failed to optimize pipeline :: caused by :: $arrayToObject requires an array of size 2 arrays,found array of size: 3");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [[1, 2]]")))
            .withMessage("[Error 40395] Failed to optimize pipeline :: caused by :: $arrayToObject requires an array of key-value pairs, where the key must be of type string. Found key type: int");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: 1")))
            .withMessage("[Error 40386] Failed to optimize pipeline :: caused by :: $arrayToObject requires an array input, found: int");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [{}]")))
            .withMessage("[Error 40392] Failed to optimize pipeline :: caused by :: $arrayToObject requires an object keys of 'k' and 'v'. Found incorrect number of keys:0");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [{k: 1, v: 2}]")))
            .withMessage("[Error 40394] Failed to optimize pipeline :: caused by :: $arrayToObject requires an object with keys 'k' and 'v', where the value of 'k' must be of type string. Found type: int");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayToObject: '$kv'"), json("kv: [{k: 'key', z: 2}]")))
            .withMessage("[Error 40393] Failed to optimize pipeline :: caused by :: $arrayToObject requires an object with keys 'k' and 'v'. Missing either or both keys from: {k: \"key\", z: 2}");
    }

    @Test
    void testEvaluateCeil() throws Exception {
        assertThat(Expression.evaluate(json("$ceil: '$a'"), json("a: 2.5"))).isEqualTo(3.0);
        assertThat(Expression.evaluate(json("$ceil: 42"), json(""))).isEqualTo(42.0);
        assertThat(Expression.evaluate(json("$ceil: [5.4]"), json(""))).isEqualTo(6.0);
        assertThat(Expression.evaluate(json("$ceil: ['$a']"), json("a: 9.9"))).isEqualTo(10.0);
        assertThat(Expression.evaluate(json("$ceil: 42.3"), json(""))).isEqualTo(43.0);
        assertThat(Expression.evaluate(new Document("$ceil", (double) Long.MAX_VALUE), json(""))).isEqualTo(9.223372036854776E18);
        assertThat(Expression.evaluate(new Document("$ceil", (double) Long.MIN_VALUE), json(""))).isEqualTo(-9.223372036854776E18);
        assertThat(Expression.evaluate(json("$ceil: null"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ceil: 'abc'"), json("")))
            .withMessage("[Error 28765] $ceil only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ceil: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $ceil takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateCmp() throws Exception {
        assertThat(Expression.evaluate(json("$cmp: [20, 10]"), json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: [20, 20]"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$cmp: [10, 20]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$cmp: ['$a', '$b']"), json("a: 10, b: 5"))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: ['b', 'a']"), json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: ['a', 'b']"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$cmp: ['$qty', 250]"), json("qty: 500"))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: ['$qty', 250]"), json("qty: 100"))).isEqualTo(-1);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cmp: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $cmp takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cmp: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $cmp takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateConcat() throws Exception {
        assertThat(Expression.evaluate(json("$concat: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$concat: ['A', 'B', 'C']"), json(""))).isEqualTo("ABC");
        assertThat(Expression.evaluate(json("$concat: ['$a', '-', '$b']"), json("a: 'A', b: 'B'"))).isEqualTo("A-B");
        assertThat(Expression.evaluate(json("$concat: ['$a', '$b']"), json("b: 'B'"))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concat: 1"), json("")))
            .withMessage("[Error 16702] $concat only supports strings, not int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concat: '$a'"), json("a: ['abc', 'def']")))
            .withMessage("[Error 16702] $concat only supports strings, not array");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concat: [1]"), json("")))
            .withMessage("[Error 16702] $concat only supports strings, not int");
    }

    @Test
    void testEvaluateConcatArrays() throws Exception {
        assertThat(Expression.evaluate(json("$concatArrays: null"), json(""))).isNull();

        assertThat(Expression.evaluate(json("$concatArrays: [['hello', ' '], ['world']]"), json("")))
            .isEqualTo(Arrays.asList("hello", " ", "world"));

        assertThat(Expression.evaluate(json("$concatArrays: [['hello', ' '], [['world'], 'again']]"), json("")))
            .isEqualTo(Arrays.asList("hello", " ", Collections.singletonList("world"), "again"));

        assertThat(Expression.evaluate(json("$concatArrays: ['$a', '$b']"), json("a: [1, 2], b: [3, 4]")))
            .isEqualTo(Arrays.asList(1, 2, 3, 4));

        assertThat(Expression.evaluate(json("$concatArrays: ['$a', '$b']"), json("a: [1, 2]")))
            .isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concatArrays: 1"), json("")))
            .withMessage("[Error 28664] $concatArrays only supports arrays, not int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concatArrays: [1]"), json("")))
            .withMessage("[Error 28664] $concatArrays only supports arrays, not int");
    }

    @Test
    void testEvaluateCond() throws Exception {
        assertThat(Expression.evaluate(json("$cond: {if: {$gte: ['$qty', 250]}, then: 30, else: 20}"), json("qty: 100")))
            .isEqualTo(20);

        assertThat(Expression.evaluate(json("$cond: [{$gte: ['$qty', 250]}, 30, 20]"), json("qty: 300")))
            .isEqualTo(30);

        assertThat(Expression.evaluate(json("$cond: {if: {$gte: ['$qty', 250]}, then: '$qty', else: 20}"), json("qty: 300")))
            .isEqualTo(300);

        assertThat(Expression.evaluate(json("$cond: {if: {$gte: ['$qty', 250]}, then: 10, else: '$qty'}"), json("qty: 200")))
            .isEqualTo(200);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: null"), json("")))
            .withMessage("[Error 16020] Expression $cond takes exactly 3 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: [1, 2, 3, 4]"), json("")))
            .withMessage("[Error 16020] Expression $cond takes exactly 3 arguments. 4 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {}"), json("")))
            .withMessage("[Error 17080] Missing 'if' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {then: 1, else: 1}"), json("")))
            .withMessage("[Error 17080] Missing 'if' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {if: 1, else: 1}"), json("")))
            .withMessage("[Error 17080] Missing 'then' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {if: 1, then: 1}"), json("")))
            .withMessage("[Error 17080] Missing 'else' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {if: 1, then: 1, else: 1, foo: 1}"), json("")))
            .withMessage("[Error 17083] Unrecognized parameter to $cond: foo");
    }

    @Test
    void testEvaluateEq() throws Exception {
        assertThat(Expression.evaluate(json("$eq: [20, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: [20, 10]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$eq: [null, null]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: ['$a', '$b']"), json("a: 10, b: 10"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: ['$qty', 250]"), json("qty: 250"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: ['$qty', 250]"), json("qty: 100"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$eq: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $eq takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$eq: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $eq takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateMap() throws Exception {
        assertThat((Collection<Object>) Expression.evaluate(
            json("$map: {input: '$quizzes', as: 'grade', in: {$add: ['$$grade', 2]}}"),
            json("quizzes: [5, 6, 7]")))
            .containsExactly(7, 8, 9);

        assertThat((Collection<Object>) Expression.evaluate(
            json("$map: {input: '$quizzes', as: 'grade', in: {$add: ['$$grade', 2]}}"),
            json("quizzes: []")))
            .isEmpty();

        assertThat(Expression.evaluate(json("$map: {input: '$q', in: '$this'}"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: 'a'"), json("")))
            .withMessage("[Error 16878] $map only supports an object as its argument");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {input: [1, 2, 3], in: true}"), json("$this: 1")))
            .withMessage("Document already contains '$this'");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {input: 'a', in: null}"), json("")))
            .withMessage("[Error 16883] input to $map must be an array not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {}"), json("")))
            .withMessage("[Error 16882] Missing 'input' parameter to $map");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {input: null}"), json("")))
            .withMessage("[Error 16882] Missing 'in' parameter to $map");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {input: 1, in: 1, foo: 1}"), json("")))
            .withMessage("[Error 16879] Unrecognized parameter to $map: foo");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {input: [], as: [], in: 1}"), json("")))
            .withMessage("[Error 16866] empty variable names are not allowed");
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$map: {input: [], as: '', in: 1}"), json("")))
            .withMessage("[Error 16866] empty variable names are not allowed");
    }

    @Test
    void testEvaluateReduce() throws Exception {
        assertThat((Integer) Expression.evaluate(
            json("$reduce: {input: '$quizzes', initialValue: 0, in: {$add: ['$$this', '$$value']}}"),
            json("quizzes: [5, 6, 7]")))
            .isEqualTo(18);

        final Document expectedDocument = new Document();
        expectedDocument.put("sum", 15);
        expectedDocument.put("product", 48);
        assertThat(Expression.evaluate(
            json("$reduce: {input: '$quizzes', initialValue: { sum: 5, product: 2 }, in: {sum: {$add : ['$$value.sum', '$$this']},product: {$multiply: [ '$$value.product', '$$this' ]}}}"),
            json("quizzes: [ 1, 2, 3, 4 ]")))
            .isEqualTo(expectedDocument);

        assertThat((Collection<Object>) Expression.evaluate(
            json("$reduce: {input: '$quizzes',initialValue: [ 1, 2 ],in: {$concatArrays : ['$$value', '$$this']}}"),
            json("quizzes: [ [ 3, 4 ], [ 5, 6 ] ]")))
            .containsExactly(1, 2, 3, 4, 5, 6);

        assertThat((Collection<Object>) Expression.evaluate(
            json("$reduce: {input: '$quizzes',initialValue: [],in: {$concatArrays : ['$$value', '$$this']}}"),
            json("quizzes: []")))
            .isEmpty();

        assertThat((Collection<Object>) Expression.evaluate(
            json("$reduce: {input: '$quizzes', initialValue: [ 1, 2 ], in: '$$this'}"),
            json("")))
            .isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: 'a'"), json("")))
            .withMessage("[Error 40075] $reduce only supports an object as its argument");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {input: [1, 2, 3], initialValue: null, in: '$this'}"), json("$this: 1")))
            .withMessage("Document already contains '$this'");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {input: [1, 2, 3], initialValue: null, in: '$value'}"), json("$value: 1")))
            .withMessage("Document already contains '$value'");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {input: 'a', initialValue: null, in: null}"), json("")))
            .withMessage("[Error 40080] input to $reduce must be an array not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {initialValue: null, in: '$value'}"), json("")))
            .withMessage("[Error 40079] Missing 'input' parameter to $reduce");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {input: [1, 2, 3], in: '$value'}"), json("")))
            .withMessage("[Error 40079] Missing 'initialValue' parameter to $reduce");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {input: [1, 2, 3], initialValue: null}"), json("")))
            .withMessage("[Error 40079] Missing 'in' parameter to $reduce");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reduce: {input: [1, 2, 3], initialValue: null, in: '$value', foo: 1}"), json("")))
            .withMessage("[Error 40076] Unrecognized parameter to $reduce: foo");
    }

    @Test
    void testEvaluateMergeObjects() throws Exception {
        assertThat(Expression.evaluate(json("$mergeObjects: [{a: 1}, null]"), json(""))).isEqualTo(json("a: 1"));
        assertThat(Expression.evaluate(json("$mergeObjects: [null, null]"), json(""))).isEqualTo(json(""));
        assertThat(Expression.evaluate(json("$mergeObjects: ['$a', '$b']"), json(""))).isEqualTo(json(""));
        assertThat(Expression.evaluate(json("$mergeObjects: ['$a', '$b']"), json("a: {x: 1}, b: {y: 2}"))).isEqualTo(json("x: 1, y: 2"));
        assertThat(Expression.evaluate(json("$mergeObjects: ['$a']"), json("a: {x: 1, y: 2}"))).isEqualTo(json("x: 1, y: 2"));
        assertThat(Expression.evaluate(json("$mergeObjects: ['$a', '$a.x']"), json("a: {x: {y: 2}}"))).isEqualTo(json("x: {y: 2}, y: 2"));

        assertThat(Expression.evaluate(json("$mergeObjects: [{a: 1}, {a: 2, b: 2}, {a: 3, c: 3}]"), json("")))
            .isEqualTo(json("a: 3, b: 2, c: 3"));

        assertThat(Expression.evaluate(json("$mergeObjects: [{a: 1}, {a: 2, b: 2}, {a: 3, b: null, c: 3}]"), json("")))
            .isEqualTo(json("a: 3, b: null, c: 3"));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$mergeObjects: [[], []]"), json("")))
            .withMessage("[Error 40400] $mergeObjects requires object inputs, but input [] is of type array");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$mergeObjects: 'x'"), json("")))
            .withMessage("[Error 40400] $mergeObjects requires object inputs, but input \"x\" is of type string");
    }

    @Test
    void testEvaluateMinute() throws Exception {
        assertThat(Expression.evaluate(json("$minute: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$minute: '$a'"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(10);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$minute: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateMod() throws Exception {
        assertThat(Expression.evaluate(json("$mod: [10, 2]"), json(""))).isEqualTo(0.0);
        assertThat(Expression.evaluate(json("$mod: [3, 2]"), json(""))).isEqualTo(1.0);
        assertThat(Expression.evaluate(json("$mod: [3.5, 3]"), json(""))).isEqualTo(0.5);
        assertThat(Expression.evaluate(json("$mod: ['$a', '$b']"), json("a: -10, b: 4"))).isEqualTo(-2.0);
        assertThat(Expression.evaluate(json("$mod: ['$a', '$b']"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$mod: [null, 2]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$mod: [2, null]"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$mod: ''"), json("")))
            .withMessage("[Error 16020] Expression $mod takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$mod: [1, 2, 3]"), json("")))
            .withMessage("[Error 16020] Expression $mod takes exactly 2 arguments. 3 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$mod: ['a', 'b']"), json("")))
            .withMessage("[Error 16611] $mod only supports numeric types, not string and string");
    }

    @Test
    void testEvaluateMonth() throws Exception {
        assertThat(Expression.evaluate(json("$month: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$month: '$a'"), new Document("a", toDate("2018-07-03T14:00:00Z")))).isEqualTo(7);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$month: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateMultiply() throws Exception {
        assertThat(Expression.evaluate(json("$multiply: ['$a', '$b']"), json("a: 8, b: 4"))).isEqualTo(32);
        assertThat(Expression.evaluate(json("$multiply: [4.5, 3]"), json(""))).isEqualTo(13.5);
        assertThat(Expression.evaluate(json("$multiply: [5, 3.0]"), json(""))).isEqualTo(15.0);
        assertThat(Expression.evaluate(json("$multiply: [null, 3]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$multiply: [null, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$multiply: [3, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$multiply: [3, 0]"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$multiply: [5000, 9000]"), json(""))).isEqualTo(45000000);
        assertThat(Expression.evaluate(json("$multiply: [50000, 90000]"), json(""))).isEqualTo(4500000000L);
        assertThat(Expression.evaluate(json("$multiply: [45000000000, 2]"), json(""))).isEqualTo(90000000000L);
        assertThat(Expression.evaluate(json("$multiply: [-45000000000, 2]"), json(""))).isEqualTo(-90000000000L);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$multiply: []"), json("")))
            .withMessage("[Error 16020] Expression $multiply takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$multiply: [1]"), json("")))
            .withMessage("[Error 16020] Expression $multiply takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$multiply: 123"), json("")))
            .withMessage("[Error 16020] Expression $multiply takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$multiply: ['a', 'b']"), json("")))
            .withMessage("[Error 16555] $multiply only supports numeric types, not string and string");
    }

    @Test
    void testEvaluateNe() throws Exception {
        assertThat(Expression.evaluate(json("$ne: [20, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$ne: [20, 10]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$ne: [20, 'a']"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$ne: ['$a', '$b']"), json("a: 10, b: 10"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$ne: ['$qty', 250]"), json("qty: 250"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$ne: ['$qty', 250]"), json("qty: 100"))).isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ne: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $ne takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ne: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $ne takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateNot() throws Exception {
        assertThat(Expression.evaluate(json("$not: false"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$not: true"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$not: 1"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$not: 0"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$not: [true]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$not: [[false]]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$not: [false]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$not: [null]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$not: [0]"), json(""))).isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$not: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $not takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateOr() throws Exception {
        assertThat(Expression.evaluate(json("$or: [1, 'green']"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: []"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$or: [[null], [false], [0]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: 'abc'"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: '$value'"), json("value: true"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: '$value'"), json("value: false"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$or: true"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: [{$gt: ['$qty', 100]}, {$lt: ['$qty', 250]}]"), json("qty: 150"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: [{$gt: ['$qty', 100]}, {$lt: ['$qty', 250]}]"), json("qty: 300"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: [{$gt: ['$qty', 400]}, {$lt: ['$qty', 100]}]"), json("qty: 300"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$or: false"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$or: null"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$or: [null, true]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: [0, true]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$or: [0, false]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$or: [0, 0]"), json(""))).isEqualTo(false);
    }

    @Test
    void testEvaluateObjectToArray() throws Exception {
        assertThat((List<Document>) Expression.evaluate(json("$objectToArray: '$v'"), json("v: {a: 1, b: 2}")))
            .containsExactly(
                json("{k: 'a', v: 1}"),
                json("{k: 'b', v: 2}")
            );

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$objectToArray: 1"), json("")))
            .withMessage("[Error 40390] $objectToArray requires a document input, found: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$objectToArray: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $objectToArray takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluatePow() throws Exception {
        assertThat(Expression.evaluate(json("$pow: ['$a', '$b']"), json("a: 8, b: 4"))).isEqualTo(4096.0);
        assertThat(Expression.evaluate(json("$pow: [4.5, 3]"), json(""))).isEqualTo(91.125);
        assertThat(Expression.evaluate(json("$pow: [null, 3]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$pow: [null, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$pow: [3, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$pow: [3, 0]"), json(""))).isEqualTo(1.0);
        assertThat(Expression.evaluate(json("$pow: [-5, 0.5]"), json(""))).isEqualTo(Double.NaN);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$pow: []"), json("")))
            .withMessage("[Error 16020] Expression $pow takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$pow: [1]"), json("")))
            .withMessage("[Error 16020] Expression $pow takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$pow: 123"), json("")))
            .withMessage("[Error 16020] Expression $pow takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$pow: ['a', 3]"), json("")))
            .withMessage("[Error 28762] $pow's base must be numeric, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$pow: [3, 'a']"), json("")))
            .withMessage("[Error 28763] $pow's exponent must be numeric, not string");
    }

    @Test
    void testEvaluateRange() throws Exception {
        assertThat(Expression.evaluate(json("$range: [0, 5]"), json(""))).isEqualTo(Arrays.asList(0, 1, 2, 3, 4));
        assertThat(Expression.evaluate(json("$range: [0, 10, 2]"), json(""))).isEqualTo(Arrays.asList(0, 2, 4, 6, 8));
        assertThat(Expression.evaluate(json("$range: [0, 1.0, 2]"), json(""))).isEqualTo(Collections.singletonList(0));
        assertThat(Expression.evaluate(json("$range: [0, 0, 1]"), json(""))).isEqualTo(Collections.emptyList());
        assertThat(Expression.evaluate(json("$range: [10, 0, -2]"), json(""))).isEqualTo(Arrays.asList(10, 8, 6, 4, 2));
        assertThat(Expression.evaluate(json("$range: [0, 10, -2]"), json(""))).isEqualTo(Collections.emptyList());

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: 'abc'"), json("")))
            .withMessage("[Error 28667] Expression $range takes at least 2 arguments, and at most 3, but 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: ['a', 'b']"), json("")))
            .withMessage("[Error 34443] $range requires a numeric starting value, found value of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 34443] $range requires a numeric starting value, found value of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: [0, 'b', 'c']"), json("")))
            .withMessage("[Error 34445] $range requires a numeric ending value, found value of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: [0, 0, 'c']"), json("")))
            .withMessage("[Error 34447] $range requires a numeric step value, found value of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: [0, 0, 0]"), json("")))
            .withMessage("[Error 34449] $range requires a non-zero step value");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: [0.5, 0, 1]"), json("")))
            .withMessage("[Error 34444] $range requires a starting value that can be represented as a 32-bit integer, found value: 0.5");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: [0, 1.5, 1]"), json("")))
            .withMessage("[Error 34446] $range requires a ending value that can be represented as a 32-bit integer, found value: 1.5");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$range: [0, 10, 0.5]"), json("")))
            .withMessage("[Error 34448] $range requires a step value that can be represented as a 32-bit integer, found value: 0.5");
    }

    @Test
    void testEvaluateReverseArray() throws Exception {
        assertThat(Expression.evaluate(json("$reverseArray: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$reverseArray: '$a'"), json(""))).isNull();

        assertThat(Expression.evaluate(json("$reverseArray: [[1, 2, 3]]"), json("")))
            .isEqualTo(Arrays.asList(3, 2, 1));

        assertThat(Expression.evaluate(json("$reverseArray: '$a'"), json("a: ['foo', 'bar']")))
            .isEqualTo(Arrays.asList("bar", "foo"));

        assertThat(Expression.evaluate(json("$reverseArray: ['$a']"), json("a: ['foo', 'bar']")))
            .isEqualTo(Arrays.asList("bar", "foo"));

        assertThat(Expression.evaluate(json("$reverseArray: [[]]"), json("")))
            .isEqualTo(Collections.emptyList());

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reverseArray: 1"), json("")))
            .withMessage("[Error 34435] The argument to $reverseArray must be an array, but was of type: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reverseArray: [1]"), json("")))
            .withMessage("[Error 34435] The argument to $reverseArray must be an array, but was of type: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$reverseArray: [[1, 2], [3, 4]]"), json("")))
            .withMessage("[Error 16020] Expression $reverseArray takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateGt() throws Exception {
        assertThat(Expression.evaluate(json("$gt: [20, 10]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: [20, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$gt: ['$a', '$b']"), json("a: 10, b: 5"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: ['b', 'a']"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: ['a', 'b']"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$gt: ['$qty', 250]"), json("qty: 500"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: ['$qty', 250]"), json("qty: 100"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gt: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $gt takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gt: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $gt takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateGte() throws Exception {
        assertThat(Expression.evaluate(json("$gte: [20, 10]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gte: [20, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gte: [20, 21]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$gte: ['$qty', 250]"), json("qty: 500"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gte: ['$qty', 250]"), json("qty: 100"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gte: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $gte takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gte: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $gte takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateHour() throws Exception {
        assertThat(Expression.evaluate(json("$hour: '$a'"), json(""))).isNull();
        int expectedHour = ZonedDateTime.ofInstant(Instant.parse("2018-07-03T14:10:00Z"), ZoneId.systemDefault()).toLocalTime().getHour();
        assertThat(Expression.evaluate(json("$hour: '$a'"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(expectedHour);
        assertThat(Expression.evaluate(json("$hour: {date: '$a'}"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(expectedHour);
        assertThat(Expression.evaluate(json("$hour: {date: '$a', timezone: 'UTC'}"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(14);
        assertThat(Expression.evaluate(json("$hour: {date: '$a', timezone: '$TZ'}"), new Document("a", toDate("2018-07-03T14:10:00Z")).append("TZ", "Europe/Berlin"))).isEqualTo(16);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$hour: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$hour: {}"), json("")))
            .withMessage("[Error 40539] missing 'date' argument to $hour, provided: {}");
    }

    @Test
    void testEvaluateLt() throws Exception {
        assertThat(Expression.evaluate(json("$lt: [10, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lt: [20, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$lt: ['$qty', 250]"), json("qty: 100"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lt: ['$qty', 250]"), json("qty: 500"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lt: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $lt takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lt: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $lt takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateLte() throws Exception {
        assertThat(Expression.evaluate(json("$lte: [10, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lte: [20, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lte: [21, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$lte: ['$qty', 250]"), json("qty: 100"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lte: ['$qty', 250]"), json("qty: 500"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lte: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $lte takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lte: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $lte takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateSecond() throws Exception {
        assertThat(Expression.evaluate(json("$second: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$second: '$a'"), new Document("a", toDate("2018-07-03T14:10:23Z")))).isEqualTo(23);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$second: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateSetDifference() throws Exception {
        assertThat(Expression.evaluate(json("$setDifference: [null, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$setDifference: [[], null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$setDifference: [null, []]"), json(""))).isNull();

        assertThat((Collection<Object>) Expression.evaluate(json("$setDifference: [['a', 'b', 'a'], ['b']]"), json("")))
            .containsExactly("a");

        assertThat((Collection<Object>) Expression.evaluate(json("$setDifference: [['a', 'b', 'a'], ['c', 'b', 'a']]"), json("")))
            .isEmpty();

        assertThat((Collection<Object>) Expression.evaluate(json("$setDifference: [['a', 'b'], [['a', 'b']]]"), json("")))
            .containsExactly("a", "b");

        assertThat((Collection<Object>) Expression.evaluate(json("$setDifference: [[1.0, 0, 2], [-0.0]]"), json("")))
            .containsExactly(1.0, 2);

        assertThat((Collection<Object>) Expression.evaluate(json("$setDifference: [[1.0, -0.0, 2], [1, 0]]"), json("")))
            .containsExactly(2);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setDifference: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16020] Expression $setDifference takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setDifference: [1, 2, 3]"), json("")))
            .withMessage("[Error 16020] Expression $setDifference takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    void testEvaluateSetEquals() throws Exception {
        assertThat(Expression.evaluate(json("$setEquals: [['a', 'b', 'a'], ['b']]"), json("")))
            .isEqualTo(false);

        assertThat(Expression.evaluate(json("$setEquals: [['a', 'b', 'a'], ['b', 'a']]"), json("")))
            .isEqualTo(true);

        assertThat(Expression.evaluate(json("$setEquals: ['$one', '$other']"), json("one: [1, 2], other: [2, 1]")))
            .isEqualTo(true);

        assertThat(Expression.evaluate(json("$setEquals: ['$one', '$other', [2]]"), json("one: [1, 2], other: [2, 1]")))
            .isEqualTo(false);

        assertThat(Expression.evaluate(json("$setEquals: ['$one', '$other', [2, 2, 1]]"), json("one: [1, 2], other: [2, 1]")))
            .isEqualTo(true);

        assertThat(Expression.evaluate(json("$setEquals: ['$one', '$other']"), json("one: [0, 2.0], other: [2, -0.0]")))
            .isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setEquals: []"), json("")))
            .withMessage("[Error 17045] $setEquals needs at least two arguments had: 0");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setEquals: [null]"), json("")))
            .withMessage("[Error 17045] $setEquals needs at least two arguments had: 1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setEquals: null"), json("")))
            .withMessage("[Error 17045] $setEquals needs at least two arguments had: 1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setEquals: [[]]"), json("")))
            .withMessage("[Error 17045] $setEquals needs at least two arguments had: 1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setEquals: [1, 2]"), json("")))
            .withMessage("[Error 17044] All operands of $setEquals must be arrays. One argument is of type: int");
    }

    @Test
    void testEvaluateSetIntersection() throws Exception {
        assertThat(Expression.evaluate(json("$setIntersection: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$setIntersection: '$field'"), json(""))).isNull();
        assertThat((Collection<?>) Expression.evaluate(json("$setIntersection: []"), json(""))).isEmpty();
        assertThat((Collection<?>) Expression.evaluate(json("$setIntersection: [[]]"), json(""))).isEmpty();
        assertThat(Expression.evaluate(json("$setIntersection: [null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$setIntersection: [['a'], null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$setIntersection: ['$a', null]"), json(""))).isNull();

        assertThat((Collection<Object>) Expression.evaluate(json("$setIntersection: [['a', 'b', 'a'], ['b']]"), json("")))
            .containsExactly("b");

        assertThat((Collection<Object>) Expression.evaluate(json("$setIntersection: [['a', 'b', 'a'], ['b', 'a']]"), json("")))
            .containsExactly("a", "b");

        assertThat((Collection<Object>) Expression.evaluate(json("$setIntersection: ['$one', '$other']"), json("one: [1, 2, 3], other: [2, 3, 5]")))
            .containsExactly(2, 3);

        assertThat((Collection<Object>) Expression.evaluate(json("$setIntersection: ['$one', '$other', [2]]"), json("one: [1, 2], other: [2, 1]")))
            .containsExactly(2);

        assertThat((Collection<Object>) Expression.evaluate(json("$setIntersection: ['$one', '$other']"), json("one: [1, 2.0], other: [2, 1]")))
            .containsExactly(1, 2.0);

        assertThat((Collection<Object>) Expression.evaluate(json("$setIntersection: ['$one', '$other', [2, 2, 1]]"), json("one: [], other: [2, 1]")))
            .isEmpty();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIntersection: [1, 2]"), json("")))
            .withMessage("[Error 17047] All operands of $setIntersection must be arrays. One argument is of type: int");
    }

    @Test
    void testEvaluateSetIsSubset() throws Exception {
        assertThat(Expression.evaluate(json("$setIsSubset: [['a', 'b', 'a'], ['b', 'a']]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$setIsSubset: [['a', 'b'], [['a', 'b']]]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$setIsSubset: ['$a', '$b']"), json("a: [1, 2], b: [1, 2, 3, 4]"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$setIsSubset: ['$a', '$b']"), json("a: [1.0, 2.0], b: [1, 2, 3, 4]"))).isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: null"), json("")))
            .withMessage("[Error 16020] Expression $setIsSubset takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: [null]"), json("")))
            .withMessage("[Error 16020] Expression $setIsSubset takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: [null, []]"), json("")))
            .withMessage("[Error 17046] both operands of $setIsSubset must be arrays. First argument is of type: null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: ['$doestNotExist', []]"), json("")))
            .withMessage("[Error 17046] both operands of $setIsSubset must be arrays. First argument is of type: missing");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: [[], '$doestNotExist']"), json("")))
            .withMessage("[Error 17042] both operands of $setIsSubset must be arrays. Second argument is of type: missing");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: [[], null]"), json("")))
            .withMessage("[Error 17042] both operands of $setIsSubset must be arrays. Second argument is of type: null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setIsSubset: [1, 2]"), json("")))
            .withMessage("[Error 17046] both operands of $setIsSubset must be arrays. First argument is of type: int");
    }

    @Test
    void testEvaluateSetUnion() throws Exception {
        assertThat(Expression.evaluate(json("$setUnion: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$setUnion: '$a'"), json(""))).isNull();

        assertThat((Collection<Object>) Expression.evaluate(json("$setUnion: [['a', 1], ['c', 'a']]"), json("")))
            .containsExactly(1, "a", "c");

        assertThat((Collection<Object>) Expression.evaluate(json("$setUnion: ['$a', '$b']"), json("a: [1, 2, 3], b: [3.0, 4]")))
            .containsExactly(1, 2, 3, 4);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setUnion: 1"), json("")))
            .withMessage("[Error 17043] All operands of $setUnion must be arrays. One argument is of type: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$setUnion: [1]"), json("")))
            .withMessage("[Error 17043] All operands of $setUnion must be arrays. One argument is of type: int");
    }

    @Test
    void testEvaluateSize() throws Exception {
        assertThat(Expression.evaluate(json("$size: [['$a', '$b']]"), json("a: 7, b: 5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$size: [[7.5, 3]]"), json(""))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$size: {$literal: [7.5, 3]}"), json(""))).isEqualTo(2);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$size: null"), json("")))
            .withMessage("[Error 17124] The argument to $size must be an array, but was of type: null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$size: 'abc'"), json("")))
            .withMessage("[Error 17124] The argument to $size must be an array, but was of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$size: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $size takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateSlice() throws Exception {
        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], 1, 1]"), json("")))
            .isEqualTo(Collections.singletonList(2));

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], 0]"), json("")))
            .isEqualTo(Collections.emptyList());

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], 2]"), json("")))
            .isEqualTo(Arrays.asList(1, 2));

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], 20]"), json("")))
            .isEqualTo(Arrays.asList(1, 2, 3));

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], -2]"), json("")))
            .isEqualTo(Arrays.asList(2, 3));

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], -20]"), json("")))
            .isEqualTo(Arrays.asList(1, 2, 3));

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], 0, 10]"), json("")))
            .isEqualTo(Arrays.asList(1, 2, 3));

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], 15, 2]"), json("")))
            .isEqualTo(Collections.emptyList());

        assertThat(Expression.evaluate(json("$slice: [[1, 2, 3], -15, 2]"), json("")))
            .isEqualTo(Arrays.asList(1, 2));

        assertThat(Expression.evaluate(json("$slice: [null, 0]"), json(""))).isNull();

        assertThat(Expression.evaluate(json("$slice: ['$a', 0]"), json(""))).isNull();

        assertThat(Expression.evaluate(json("$slice: ['$a', '$b', '$c']"), json("a: [1, 2, 3], b: 1, c: 1")))
            .isEqualTo(Collections.singletonList(2));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$slice: 'abc'"), json("")))
            .withMessage("[Error 28667] Expression $slice takes at least 2 arguments, and at most 3, but 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$slice: [1, 2, 3, 4]"), json("")))
            .withMessage("[Error 28667] Expression $slice takes at least 2 arguments, and at most 3, but 4 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$slice: [1, 0, 0]"), json("")))
            .withMessage("[Error 28724] First argument to $slice must be an array, but is of type: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$slice: [[], 'a', 0]"), json("")))
            .withMessage("[Error 28725] Second argument to $slice must be a numeric value, but is of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$slice: [[], 0, 'a']"), json("")))
            .withMessage("[Error 28725] Third argument to $slice must be numeric, but is of type: string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$slice: [[], 0, -1]"), json("")))
            .withMessage("[Error 28729] Third argument to $slice must be positive: -1");
    }

    @Test
    void testEvaluateSplit() throws Exception {
        assertThat((String[]) Expression.evaluate(json("$split: ['June-15-2013', '-']"), json(""))).containsExactly("June", "15", "2013");
        assertThat((String[]) Expression.evaluate(json("$split: ['$a', '$b']"), json("a: 'foo bar', b: ' '"))).containsExactly("foo", "bar");
        assertThat(Expression.evaluate(json("$split: [null, ' ']"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$split: ['$doesNotExist', ' ']"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$split: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $split takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$split: []"), json("")))
            .withMessage("[Error 16020] Expression $split takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$split: [1, 2, 3]"), json("")))
            .withMessage("[Error 16020] Expression $split takes exactly 2 arguments. 3 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$split: [25, ' ']"), json("")))
            .withMessage("[Error 40085] $split requires an expression that evaluates to a string as a first argument, found: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$split: ['foo', 10]"), json("")))
            .withMessage("[Error 40086] $split requires an expression that evaluates to a string as a second argument, found: int");
    }

    @Test
    void testEvaluateSubtract() throws Exception {
        assertThat(Expression.evaluate(json("$subtract: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$subtract: [7.5, 3]"), json(""))).isEqualTo(4.5);
        assertThat(Expression.evaluate(json("$subtract: [null, 3]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$subtract: [3, null]"), json(""))).isNull();

        // subtract two instants
        assertThat(Expression.evaluate(new Document("$subtract",
            Arrays.asList(Instant.ofEpochMilli(3000), Instant.ofEpochMilli(1000))), json("")))
            .isEqualTo(2000L);

        // subtract milliseconds from instant
        assertThat(Expression.evaluate(new Document("$subtract",
            Arrays.asList(Instant.ofEpochMilli(3000), 1000)), json("")))
            .isEqualTo(Instant.ofEpochMilli(2000));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: []"), json("")))
            .withMessage("[Error 16020] Expression $subtract takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: [1]"), json("")))
            .withMessage("[Error 16020] Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: 123"), json("")))
            .withMessage("[Error 16020] Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: ['a', 'b']"), json("")))
            .withMessage("[Error 16556] cant $subtract a string from a string");
    }

    @Test
    void testEvaluateSum() throws Exception {
        assertThat(Expression.evaluate(json("$sum: null"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: ''"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: 5"), json(""))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$sum: [[1, 2, 3]]"), json(""))).isEqualTo(6);
        assertThat(Expression.evaluate(json("$sum: [[1], [2]]"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: [1, 'foo', 2]"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$sum: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(12);
        assertThat(Expression.evaluate(json("$sum: []"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: '$values'"), json("values: [1, 2, 3]"))).isEqualTo(6);
    }

    @Test
    void testEvaluateToLower() throws Exception {
        assertThat(Expression.evaluate(json("$toLower: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$toLower: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$toLower: '$a'"), json("a: 'FOO'"))).isEqualTo("foo");
        assertThat(Expression.evaluate(json("$toLower: 1"), json(""))).isEqualTo("1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$toLower: [[1, 2]]"), json("")))
            .withMessage("[Error 16007] can't convert from BSON type array to String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$toLower: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $toLower takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateToUpper() throws Exception {
        assertThat(Expression.evaluate(json("$toUpper: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$toUpper: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$toUpper: '$a'"), json("a: 'foo'"))).isEqualTo("FOO");
        assertThat(Expression.evaluate(json("$toUpper: 1"), json(""))).isEqualTo("1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$toUpper: [[1, 2]]"), json("")))
            .withMessage("[Error 16007] can't convert from BSON type array to String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$toUpper: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $toUpper takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateTrunc() throws Exception {
        assertThat(Expression.evaluate(json("$trunc: '$a'"), json("a: 2.5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$trunc: 42"), json(""))).isEqualTo(42);
        assertThat(Expression.evaluate(json("$trunc: NaN"), json(""))).isEqualTo(Double.NaN);
        assertThat(Expression.evaluate(json("$trunc: [5.6]"), json(""))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$trunc: ['$a']"), json("a: 9.9"))).isEqualTo(9);
        assertThat(Expression.evaluate(json("$trunc: 42.3"), json(""))).isEqualTo(42);
        assertThat(Expression.evaluate(new Document("$trunc", (double) Long.MAX_VALUE), json(""))).isEqualTo(Long.MAX_VALUE);
        assertThat(Expression.evaluate(new Document("$trunc", (double) Long.MIN_VALUE), json(""))).isEqualTo(Long.MIN_VALUE);
        assertThat(Expression.evaluate(json("$trunc: null"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$trunc: 'abc'"), json("")))
            .withMessage("[Error 28765] $trunc only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$trunc: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $trunc takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateToString() throws Exception {
        assertThat(Expression.evaluate(json("$toString: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$toString: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$toString: '$a'"), json("a: 'foo'"))).isEqualTo("foo");
        assertThat(Expression.evaluate(json("$toString: 1"), json(""))).isEqualTo("1");
        assertThat(Expression.evaluate(json("$toString: 1.3"), json(""))).isEqualTo("1.3");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$toString: [[1, 2]]"), json("")))
            .withMessage("[Error 16007] can't convert from BSON type array to String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$toString: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $toString takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateSqrt() throws Exception {
        assertThat((double) Expression.evaluate(json("$sqrt: '$a'"), json("a: 2.5"))).isEqualTo(1.581, Offset.offset(0.001));
        assertThat(Expression.evaluate(json("$sqrt: 16"), json(""))).isEqualTo(4.0);
        assertThat(Expression.evaluate(json("$sqrt: [25]"), json(""))).isEqualTo(5.0);
        assertThat(Expression.evaluate(json("$sqrt: null"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$sqrt: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $sqrt takes exactly 1 arguments. 2 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$sqrt: 'abc'"), json("")))
            .withMessage("[Error 28765] $sqrt only supports numeric types, not string");
    }

    @Test
    void testEvaluateStrLenBytes() throws Exception {
        assertThat(Expression.evaluate(json("$strLenBytes: ''"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$strLenBytes: '$a'"), json("a: 'value'"))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$strLenBytes: 'cafétéria'"), json(""))).isEqualTo(11);
        assertThat(Expression.evaluate(json("$strLenBytes: '$a'"), json("a: '$€λA'"))).isEqualTo(7);
        assertThat(Expression.evaluate(json("$strLenBytes: '寿司'"), json(""))).isEqualTo(6);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$strLenBytes: null"), json("")))
            .withMessage("[Error 34471] $strLenBytes requires a string argument, found: null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$strLenBytes: 123"), json("")))
            .withMessage("[Error 34471] $strLenBytes requires a string argument, found: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$strLenBytes: '$a'"), json("")))
            .withMessage("[Error 34471] $strLenBytes requires a string argument, found: missing");
    }

    @Test
    void testEvaluateStrLenCP() throws Exception {
        assertThat(Expression.evaluate(json("$strLenCP: ''"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$strLenCP: '$a'"), json("a: 'value'"))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$strLenCP: 'cafétéria'"), json(""))).isEqualTo(9);
        assertThat(Expression.evaluate(json("$strLenCP: '$a'"), json("a: '$€λA'"))).isEqualTo(4);
        assertThat(Expression.evaluate(json("$strLenCP: '寿司'"), json(""))).isEqualTo(2);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$strLenCP: null"), json("")))
            .withMessage("[Error 34471] $strLenCP requires a string argument, found: null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$strLenCP: 123"), json("")))
            .withMessage("[Error 34471] $strLenCP requires a string argument, found: int");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$strLenCP: '$a'"), json("")))
            .withMessage("[Error 34471] $strLenCP requires a string argument, found: missing");
    }

    @Test
    void testEvaluateSubstrBytes() throws Exception {
        assertThat(Expression.evaluate(json("$substrBytes: ['', -1, -1]"), json(""))).isEqualTo("");
        assertThat(Expression.evaluate(json("$substrBytes: ['$a', 0, -1]"), json("a: 'value'"))).isEqualTo("value");
        assertThat(Expression.evaluate(json("$substrBytes: ['$a', 0, 5]"), json("a: 'cafétéria'"))).isEqualTo("café");
        assertThat(Expression.evaluate(json("$substrBytes: ['$a', 0, 5]"), json("a: 123"))).isEqualTo("123");
        assertThat(Expression.evaluate(json("$substrBytes: ['$a', 0, '$len']"), json("a: 'hello', len: 2"))).isEqualTo("he");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrBytes: null"), json("")))
            .withMessage("[Error 16020] Expression $substrBytes takes exactly 3 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrBytes: [123]"), json("")))
            .withMessage("[Error 16020] Expression $substrBytes takes exactly 3 arguments. 1 were passed in.");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrBytes: [123, 'abc', 'def']"), json("")))
            .withMessage("[Error 16034] Failed to optimize pipeline :: caused by :: $substrBytes:  starting index must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrBytes: [123, 0, 'def']"), json("")))
            .withMessage("[Error 16035] Failed to optimize pipeline :: caused by :: $substrBytes:  length must be a numeric type (is BSON type string)");
    }

    @Test
    void testEvaluateSubstrCP() throws Exception {
        assertThat(Expression.evaluate(json("$substrCP: ['', -1, -1]"), json(""))).isEqualTo("");
        assertThat(Expression.evaluate(json("$substrCP: ['$a', 0, -1]"), json("a: 'value'"))).isEqualTo("value");
        assertThat(Expression.evaluate(json("$substrCP: ['$a', 0, 5]"), json("a: 'cafétéria'"))).isEqualTo("cafét");
        assertThat(Expression.evaluate(json("$substrCP: ['$a', 0, 5]"), json("a: 123"))).isEqualTo("123");
        assertThat(Expression.evaluate(json("$substrCP: ['$a', 0, '$len']"), json("a: 'hello', len: 2"))).isEqualTo("he");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrCP: null"), json("")))
            .withMessage("[Error 16020] Expression $substrCP takes exactly 3 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrCP: [123]"), json("")))
            .withMessage("[Error 16020] Expression $substrCP takes exactly 3 arguments. 1 were passed in.");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrCP: [123, 'abc', 'def']"), json("")))
            .withMessage("[Error 34450] Failed to optimize pipeline :: caused by :: $substrCP: starting index must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(FailedToOptimizePipelineError.class)
            .isThrownBy(() -> Expression.evaluate(json("$substrCP: [123, 0, 'def']"), json("")))
            .withMessage("[Error 34452] Failed to optimize pipeline :: caused by :: $substrCP: length must be a numeric type (is BSON type string)");
    }

    @Test
    void testEvaluateYear() throws Exception {
        assertThat(Expression.evaluate(json("$year: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$year: '$a'"), new Document("a", toDate("2018-07-03T14:00:00Z")))).isEqualTo(2018);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$year: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateDayOfWeek() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), new Document("a", toDate("2018-11-08T22:00:00Z")))).isEqualTo(4);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfWeek: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateDayOfMonth() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), new Document("a", toDate("2018-11-08T22:00:00Z")))).isEqualTo(8);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfMonth: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateDayOfYear() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(34);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfYear: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testDateToString() throws Exception {
        Instant instant = Instant.parse("2011-12-19T10:15:20.250Z");

        assertThat(Expression.evaluate(json("$dateToString: {date: '$a'}"), json(""))).isNull();
        // default format
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a'}"), new Document("a", instant))).isEqualTo("2011-12-19T10:15:20.250Z");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', timezone: '+01:00'}"), new Document("a", instant))).isEqualTo("2011-12-19T11:15:20.250Z");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', onNull: '1970-01-01T00:00:00Z'}"), new Document("a", null))).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: 'foo'}"), new Document("a", instant))).isEqualTo("foo");

        // test different formats (https://docs.mongodb.com/manual/reference/operator/aggregation/dateToString/#format-specifiers)
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%d'}"), new Document("a", instant))).isEqualTo("19");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%G'}"), new Document("a", instant))).isEqualTo("2011");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%H'}"), new Document("a", instant))).isEqualTo("10");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%j'}"), new Document("a", instant))).isEqualTo("353");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%L'}"), new Document("a", instant))).isEqualTo("250");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%m'}"), new Document("a", instant))).isEqualTo("12");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%M'}"), new Document("a", instant))).isEqualTo("15");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%S'}"), new Document("a", instant))).isEqualTo("20");
        // assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%w'}"), new Document("a", instant))).isEqualTo("2");
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: '$a', format: '%w'}"), new Document("a", instant)))
            .withMessage("[Error 18536] Not yet supported format character '%w' in $dateToString format string");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%u'}"), new Document("a", instant))).isEqualTo("1");
        // assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%U'}"), new Document("a", instant))).isEqualTo("51");
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: '$a', format: '%U'}"), new Document("a", instant)))
            .withMessage("[Error 18536] Not yet supported format character '%U' in $dateToString format string");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%V'}"), new Document("a", instant))).isEqualTo("51");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%Y'}"), new Document("a", instant))).isEqualTo("2011");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%z'}"), new Document("a", instant))).isEqualTo("+0000");
        // assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%Z'}"), new Document("a", instant))).isEqualTo("+000");
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: '$a', format: '%Z'}"), new Document("a", instant)))
            .withMessage("[Error 18536] Not yet supported format character '%Z' in $dateToString format string");
        assertThat(Expression.evaluate(json("$dateToString: {date: '$a', format: '%%'}"), new Document("a", instant))).isEqualTo("%");
        // empty format specifier
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: '$a', format: '%'}"), new Document("a", instant)))
            .withMessage("[Error 18535] Unmatched '%' at end of $dateToString format string");
        // invalid format specifier
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: '$a', format: '% '}"), new Document("a", instant)))
            .withMessage("[Error 18536] Invalid format character '% ' in $dateToString format string");

        // validation errors
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: ''"), json("")))
            .withMessage("[Error 18629] $dateToString only supports an object as its argument");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {}"), json("")))
            .withMessage("[Error 18628] Missing 'date' parameter to $dateToString");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: null, foo: 1}"), json("")))
            .withMessage("[Error 18534] Unrecognized parameter to $dateToString: foo");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: null, format: 1}"), json("")))
            .withMessage("[Error 18533] $dateToString requires that 'format' be a string, found: int with value 1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: null, format: '', timezone: 'foo'}"), json("")))
            .withMessage("[Error 40485] $dateToString unrecognized time zone identifier: foo");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dateToString: {date: 'foo'}"), json("")))
            .withMessage("[Error 16006] can't convert from string to Date");
    }

    @Test
    void testEvaluateDivide() throws Exception {
        assertThat(Expression.evaluate(json("$divide: ['$a', '$b']"), json("a: 8, b: 4"))).isEqualTo(2.0);
        assertThat(Expression.evaluate(json("$divide: [4.5, 3]"), json(""))).isEqualTo(1.5);
        assertThat(Expression.evaluate(json("$divide: [null, 3]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$divide: [null, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$divide: [3, null]"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$divide: []"), json("")))
            .withMessage("[Error 16020] Expression $divide takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$divide: [1, 0]"), json("")))
            .withMessage("[Error 16608] can't $divide by zero");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$divide: [1]"), json("")))
            .withMessage("[Error 16020] Expression $divide takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$divide: 123"), json("")))
            .withMessage("[Error 16020] Expression $divide takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$divide: ['a', 'b']"), json("")))
            .withMessage("[Error 16609] $divide only supports numeric types, not string and string");
    }

    @Test
    void testEvaluateExp() throws Exception {
        assertThat(Expression.evaluate(json("$exp: 0"), json(""))).isEqualTo(1.0);
        assertThat(Expression.evaluate(json("$exp: [0]"), json(""))).isEqualTo(1.0);
        assertThat((double) Expression.evaluate(json("$exp: '$a'"), json("a: 2"))).isEqualTo(7.389, Offset.offset(0.001));
        assertThat((double) Expression.evaluate(json("$exp: '$a.b'"), json("a: {b: -2}"))).isEqualTo(0.135, Offset.offset(0.001));
        assertThat(Expression.evaluate(json("$exp: '$doesNotExist'"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$exp: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $exp takes exactly 1 arguments. 2 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$exp: ['a']"), json("")))
            .withMessage("[Error 28765] $exp only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$exp: 'a'"), json("")))
            .withMessage("[Error 28765] $exp only supports numeric types, not string");
    }

    @Test
    void testEvaluateFilter() throws Exception {
        assertThat(Expression.evaluate(json("$filter: {input: null, cond: null}"), json(""))).isNull();

        assertThat(Expression.evaluate(json("$filter: {input: [1, 2, 3, 4], as: 'value', cond: {$lte: ['$$value', 3]}}"), json("")))
            .isEqualTo(Arrays.asList(1, 2, 3));

        assertThat(Expression.evaluate(json("$filter: {input: [1, 2, 3, 4], cond: {$lte: ['$$this', 3]}}"), json("")))
            .isEqualTo(Arrays.asList(1, 2, 3));

        assertThat(Expression.evaluate(json("$filter: {input: [1, 2, 3, 4], cond: {$lt: ['$$this', '$$ROOT.thresh']}}"), json("thresh: 3")))
            .isEqualTo(Arrays.asList(1, 2));

        assertThat(Expression.evaluate(json("$filter: {input: [1, 2, 3], cond: 1}"), json("")))
            .isEqualTo(Arrays.asList(1, 2, 3));

        assertThat(Expression.evaluate(json("$filter: {input: '$doesNotExist', cond: 1}"), json("")))
            .isNull();

        assertThat(Expression.evaluate(json("$filter: {input: '$items', as: 'item', cond: {$gte: ['$$item.price', 10]}}"),
            json("items: [{item_id: 1, price: 110}, {item_id: 2, price: 5}, {item_id: 3, price: 50}]")))
            .isEqualTo(Arrays.asList(
                json("item_id: 1, price: 110"),
                json("item_id: 3, price: 50")
            ));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: 'a'"), json("")))
            .withMessage("[Error 28646] $filter only supports an object as its argument");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {input: [1, 2, 3], cond: true}"), json("$this: 1")))
            .withMessage("Document already contains '$this'");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {input: 'a', cond: null}"), json("")))
            .withMessage("[Error 28651] input to $filter must be an array not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {}"), json("")))
            .withMessage("[Error 28648] Missing 'input' parameter to $filter");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {input: null}"), json("")))
            .withMessage("[Error 28648] Missing 'cond' parameter to $filter");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {input: 1, cond: 1, foo: 1}"), json("")))
            .withMessage("[Error 28647] Unrecognized parameter to $filter: foo");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {input: [], as: [], cond: 1}"), json("")))
            .withMessage("[Error 16866] empty variable names are not allowed");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$filter: {input: [], as: '', cond: 1}"), json("")))
            .withMessage("[Error 16866] empty variable names are not allowed");
    }

    @Test
    void testEvaluateFloor() throws Exception {
        assertThat(Expression.evaluate(json("$floor: '$a'"), json("a: 2.5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$floor: 42"), json(""))).isEqualTo(42);
        assertThat(Expression.evaluate(json("$floor: NaN"), json(""))).isEqualTo(Double.NaN);
        assertThat(Expression.evaluate(json("$floor: [5.6]"), json(""))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$floor: ['$a']"), json("a: 9.9"))).isEqualTo(9);
        assertThat(Expression.evaluate(json("$floor: 42.3"), json(""))).isEqualTo(42);
        assertThat(Expression.evaluate(new Document("$floor", (double) Long.MAX_VALUE), json(""))).isEqualTo(Long.MAX_VALUE);
        assertThat(Expression.evaluate(new Document("$floor", (double) Long.MIN_VALUE), json(""))).isEqualTo(Long.MIN_VALUE);
        assertThat(Expression.evaluate(json("$floor: null"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$floor: 'abc'"), json("")))
            .withMessage("[Error 28765] $floor only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$floor: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $floor takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateIfNull() throws Exception {
        assertThat(Expression.evaluate(json("$ifNull: [17, 'Unspecified']"), json(""))).isEqualTo(17);
        assertThat(Expression.evaluate(json("$ifNull: [null, null]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$ifNull: ['$desc', 'Unspecified']"), json(""))).isEqualTo("Unspecified");
        assertThat(Expression.evaluate(json("$ifNull: ['$desc', 'Unspecified']"), json("desc: null"))).isEqualTo("Unspecified");
        assertThat(Expression.evaluate(json("$ifNull: ['$desc', 'Unspecified']"), json("desc: 'prod1'"))).isEqualTo("prod1");
        assertThat(Expression.evaluate(json("$ifNull: ['$desc', '$alt']"), json("alt: 'prod'"))).isEqualTo("prod");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ifNull: []"), json("")))
            .withMessage("[Error 16020] Expression $ifNull takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ifNull: ['abc']"), json("")))
            .withMessage("[Error 16020] Expression $ifNull takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ifNull: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $ifNull takes exactly 2 arguments. 1 were passed in.");
    }

    @Test
    void testEvaluateIn() throws Exception {
        assertThat(Expression.evaluate(json("$in: [2, [1, 2, 3]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$in: ['abc', ['xyz', 'abc']]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$in: [['a'], ['a']]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$in: [['a'], [['a']]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$in: ['bananas', '$fruits']"), json("fruits: ['apples', 'oranges']"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$in: ['bananas', '$fruits']"), json("fruits: ['apples', 'bananas', 'oranges']"))).isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$in: ['abc']"), json("")))
            .withMessage("[Error 16020] Expression $in takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$in: ['a', 'b']"), json("")))
            .withMessage("[Error 40081] $in requires an array as a second argument, found: string");
    }

    @Test
    void testEvaluateIndexOfArray() throws Exception {
        assertThat(Expression.evaluate(json("$indexOfArray: [['a', 'abc'], 'a']"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$indexOfArray: [['a', 'abc', 'de', ['de']], ['de']]"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$indexOfArray: [[1, 2], 5]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfArray: [[1, 2, 3], [1, 2]]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfArray: [[10, 9, 9, 8, 9], 9, 3]"), json(""))).isEqualTo(4);
        assertThat(Expression.evaluate(json("$indexOfArray: [['a', 'abc', 'b'], 'b', 0, 1]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfArray: [['a', 'abc', 'b'], 'b', 1, 0]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfArray: [['a', 'abc', 'b'], 'b', 20]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfArray: [[null, null, null], null]"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$indexOfArray: [null, 'foo']"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$indexOfArray: ['$items', 2]"), json("items: [3, 4, 5, 2]"))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$indexOfArray: ['$items', 2]"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: ['a']"), json("")))
            .withMessage("[Error 28667] Expression $indexOfArray takes at least 2 arguments, and at most 4, but 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: [['a'], 'a', 'illegalIndex']"), json("")))
            .withMessage("[Error 40096] $indexOfArray requires an integral starting index, found a value of type: string, with value: \"illegalIndex\"");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: [['a'], 'a', 0, 'illegalIndex']"), json("")))
            .withMessage("[Error 40096] $indexOfArray requires an integral ending index, found a value of type: string, with value: \"illegalIndex\"");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: [['a'], 'a', -1]"), json("")))
            .withMessage("[Error 40097] $indexOfArray requires a nonnegative starting index, found: -1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: [['a'], 'a', 0, -1]"), json("")))
            .withMessage("[Error 40097] $indexOfArray requires a nonnegative ending index, found: -1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: [1, 2, 3, 4, 5]"), json("")))
            .withMessage("[Error 28667] Expression $indexOfArray takes at least 2 arguments, and at most 4, but 5 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfArray: ['a', 'b']"), json("")))
            .withMessage("[Error 40090] $indexOfArray requires an array as a first argument, found: string");
    }

    @Test
    void testEvaluateIndexOfBytes() throws Exception {
        assertThat(Expression.evaluate(json("$indexOfBytes: ['cafeteria', 'e']"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['cafétéria', 'é']"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['cafétéria', 'e']"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['cafétéria', 't']"), json(""))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['foo.bar.fi', '.', 5]"), json(""))).isEqualTo(7);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', 0, 2]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', 12]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', 5, 2]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['vanilla', 'nilla', 3]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfBytes: [null, 'foo']"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$indexOfBytes: ['$text', 'world']"), json("text: 'hello world'"))).isEqualTo(6);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['$text', '$search']"), json("text: 'hello world', search: 'l'"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$indexOfBytes: ['$text', '$search']"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: []"), json("")))
            .withMessage("[Error 28667] Expression $indexOfBytes takes at least 2 arguments, and at most 4, but 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: [1, 2, 3, 4, 5]"), json("")))
            .withMessage("[Error 28667] Expression $indexOfBytes takes at least 2 arguments, and at most 4, but 5 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: [[], 'll']"), json("")))
            .withMessage("[Error 40091] $indexOfBytes requires a string as the first argument, found: array");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: ['foo', ['x']]"), json("")))
            .withMessage("[Error 40092] $indexOfBytes requires a string as the second argument, found: array");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', -1]"), json("")))
            .withMessage("[Error 40097] $indexOfBytes requires a nonnegative starting index, found: -1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', 0, -1]"), json("")))
            .withMessage("[Error 40097] $indexOfBytes requires a nonnegative ending index, found: -1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', 'a']"), json("")))
            .withMessage("[Error 40096] $indexOfBytes requires an integral starting index, found a value of type: string, with value: \"a\"");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfBytes: ['vanilla', 'll', 0, 'b']"), json("")))
            .withMessage("[Error 40096] $indexOfBytes requires an integral ending index, found a value of type: string, with value: \"b\"");
    }

    @Test
    void testEvaluateIndexOfCP() throws Exception {
        assertThat(Expression.evaluate(json("$indexOfCP: ['cafeteria', 'e']"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$indexOfCP: ['cafétéria', 'é']"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$indexOfCP: ['cafétéria', 'e']"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfCP: ['cafétéria', 't']"), json(""))).isEqualTo(4);
        assertThat(Expression.evaluate(json("$indexOfCP: ['foo.bar.fi', '.', 5]"), json(""))).isEqualTo(7);
        assertThat(Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', 0, 2]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', 12]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', 5, 2]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfCP: ['vanilla', 'nilla', 3]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$indexOfCP: [null, 'foo']"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$indexOfCP: ['$text', 'world']"), json("text: 'hello world'"))).isEqualTo(6);
        assertThat(Expression.evaluate(json("$indexOfCP: ['$text', '$search']"), json("text: 'hello world', search: 'l'"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$indexOfCP: ['$text', '$search']"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: []"), json("")))
            .withMessage("[Error 28667] Expression $indexOfCP takes at least 2 arguments, and at most 4, but 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: [1, 2, 3, 4, 5]"), json("")))
            .withMessage("[Error 28667] Expression $indexOfCP takes at least 2 arguments, and at most 4, but 5 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: [[], 'll']"), json("")))
            .withMessage("[Error 40093] $indexOfCP requires a string as the first argument, found: array");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: ['foo', ['x']]"), json("")))
            .withMessage("[Error 40094] $indexOfCP requires a string as the second argument, found: array");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', -1]"), json("")))
            .withMessage("[Error 40097] $indexOfCP requires a nonnegative starting index, found: -1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', 0, -1]"), json("")))
            .withMessage("[Error 40097] $indexOfCP requires a nonnegative ending index, found: -1");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', 'a']"), json("")))
            .withMessage("[Error 40096] $indexOfCP requires an integral starting index, found a value of type: string, with value: \"a\"");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$indexOfCP: ['vanilla', 'll', 0, 'b']"), json("")))
            .withMessage("[Error 40096] $indexOfCP requires an integral ending index, found a value of type: string, with value: \"b\"");
    }

    @Test
    void testEvaluateIsArray() throws Exception {
        assertThat(Expression.evaluate(json("$isArray: ['hello']"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$isArray: [[2, 3]]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$isArray: 'foo'}"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$isArray: null}"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$isArray: '$value'}"), json("value: 'abc'"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$isArray: '$value'}"), json("value: ['abc']"))).isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$isArray: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $isArray takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    void testEvaluateLiteral() throws Exception {
        assertThat(Expression.evaluate(json("$literal: {$add: [2, 3]}"), json(""))).isEqualTo(json("$add: [2, 3]"));
        assertThat(Expression.evaluate(json("$literal: {$literal: 1}"), json(""))).isEqualTo(json("$literal: 1"));
    }

    @Test
    void testEvaluateLn() throws Exception {
        assertThat(Expression.evaluate(json("$ln: 1"), json(""))).isEqualTo(0.0);
        assertThat(Expression.evaluate(json("$ln: [1]"), json(""))).isEqualTo(0.0);
        assertThat((double) Expression.evaluate(json("$ln: '$a'"), json("a: 10"))).isEqualTo(2.302, Offset.offset(0.001));
        assertThat(Expression.evaluate(json("$ln: '$a.b'"), json("a: {b: -2}"))).isEqualTo(Double.NaN);
        assertThat(Expression.evaluate(json("$ln: '$doesNotExist'"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ln: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $ln takes exactly 1 arguments. 2 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ln: ['a']"), json("")))
            .withMessage("[Error 28765] $ln only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ln: 'a'"), json("")))
            .withMessage("[Error 28765] $ln only supports numeric types, not string");
    }

    @Test
    void testEvaluateLog() throws Exception {
        assertThat(Expression.evaluate(json("$log: 1"), json(""))).isEqualTo(0.0);
        assertThat(Expression.evaluate(json("$log: [1]"), json(""))).isEqualTo(0.0);
        assertThat((double) Expression.evaluate(json("$log: '$a'"), json("a: 10"))).isEqualTo(2.302, Offset.offset(0.001));
        assertThat(Expression.evaluate(json("$log: '$a.b'"), json("a: {b: -2}"))).isEqualTo(Double.NaN);
        assertThat(Expression.evaluate(json("$log: '$doesNotExist'"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$log: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $log takes exactly 1 arguments. 2 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$log: ['a']"), json("")))
            .withMessage("[Error 28765] $log only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$log: 'a'"), json("")))
            .withMessage("[Error 28765] $log only supports numeric types, not string");
    }

    @Test
    void testEvaluateLog10() throws Exception {
        assertThat(Expression.evaluate(json("$log10: 1"), json(""))).isEqualTo(0.0);
        assertThat(Expression.evaluate(json("$log10: 10"), json(""))).isEqualTo(1.0);
        assertThat(Expression.evaluate(json("$log10: 100"), json(""))).isEqualTo(2.0);
        assertThat(Expression.evaluate(json("$log10: 1000"), json(""))).isEqualTo(3.0);
        assertThat(Expression.evaluate(json("$log10: [1]"), json(""))).isEqualTo(0.0);
        assertThat((double) Expression.evaluate(json("$log10: '$a'"), json("a: 20"))).isEqualTo(1.301, Offset.offset(0.001));
        assertThat(Expression.evaluate(json("$log10: '$a.b'"), json("a: {b: -2}"))).isEqualTo(Double.NaN);
        assertThat(Expression.evaluate(json("$log10: '$doesNotExist'"), json(""))).isNull();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$log10: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $log10 takes exactly 1 arguments. 2 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$log10: ['a']"), json("")))
            .withMessage("[Error 28765] $log10 only supports numeric types, not string");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$log10: 'a'"), json("")))
            .withMessage("[Error 28765] $log10 only supports numeric types, not string");
    }

    @Test
    void testEvaluateIllegalExpression() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$foo: '$a'"), json("")))
            .withMessage("[Error 168] Unrecognized expression '$foo'");
    }

    @Test
    void testEvaluateDocument_SimpleExpression() throws Exception {
        Object evaluatedDocument = Expression.evaluateDocument(json("key: '$key2'"), json("key2: 123"));
        assertThat(evaluatedDocument).isEqualTo(json("key: 123"));
    }

    @Test
    void testEvaluateDocument_NullValue() throws Exception {
        Object evaluatedDocument = Expression.evaluateDocument(json("key: '$value'"), json("value: null"));
        assertThat(evaluatedDocument).isEqualTo(json("key: null"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/111
    @Test
    void testEvaluateDocument_MissingValue() throws Exception {
        Object evaluatedDocument = Expression.evaluateDocument(json("key: '$missing'"), json(""));
        assertThat(evaluatedDocument).isEqualTo(json(""));
    }

    private static Instant toDate(String instant) {
        return Instant.parse(instant);
    }

}
