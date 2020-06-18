package de.bwaldvogel.mongo.backend.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

public class PostgresqlUtilsTest {

    @Test
    public void testToDataKey() throws Exception {
        assertThat(PostgresqlUtils.toDataKey("foo")).isEqualTo("data ->> 'foo'");
        assertThat(PostgresqlUtils.toDataKey("foo.bar")).isEqualTo("data -> 'foo' ->> 'bar'");
        assertThat(PostgresqlUtils.toDataKey("foo.bar.bla")).isEqualTo("data -> 'foo' -> 'bar' ->> 'bla'");
    }

    @Test
    public void testToQueryValue() throws Exception {
        assertThat(PostgresqlUtils.toQueryValue(123)).isEqualTo("123");
        assertThat(PostgresqlUtils.toQueryValue(123.0)).isEqualTo("123");
        assertThat(PostgresqlUtils.toQueryValue(123.1)).isEqualTo("123.1");
        assertThat(PostgresqlUtils.toQueryValue("foobar")).isEqualTo("foobar");
        assertThat(PostgresqlUtils.toQueryValue("1.0")).isEqualTo("1.0");
        assertThat(PostgresqlUtils.toQueryValue(new LinkedHashMap<>(Collections.singletonMap("foo", "bar")))).isEqualTo("{\"foo\":\"bar\"}");
        assertThat(PostgresqlUtils.toQueryValue(Arrays.asList("foo", "bar"))).isEqualTo("[\"foo\",\"bar\"]");
        assertThat(PostgresqlUtils.toQueryValue(new ObjectId("foobarfoobar".getBytes(StandardCharsets.UTF_8)))).isEqualTo("{\"@class\":\"de.bwaldvogel.mongo.bson.ObjectId\",\"data\":\"Zm9vYmFyZm9vYmFy\"}");
        assertThat(PostgresqlUtils.toQueryValue(new Document("key", "value"))).isEqualTo("{\"@class\":\"de.bwaldvogel.mongo.bson.Document\",\"key\":\"value\"}");

        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> PostgresqlUtils.toQueryValue(null))
            .withMessage(null);
    }

}