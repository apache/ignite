package org.apache.ignite.console.services;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class FieldEnumeratorTest {
	
	DataSourceInfoService fieldEnumerator = new DataSourceInfoService();
	
    @Test
    void testEmptyDocument() {
        Map<String, Object> document = new HashMap<>();
        Set<String> result = fieldEnumerator.enumFields(document);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFlatObject() {
        Map<String, Object> document = Map.of(
            "name", "John",
            "age", 30,
            "email", "john@example.com"
        );

        List<String> expected = Arrays.asList("name", "age", "email");
        Set<String> actual = fieldEnumerator.enumFields(document);
        
        assertTrue(actual.containsAll(expected));
        assertEquals(expected.size(), actual.size());
    }

    @Test
    void testNestedObjects() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> address = Map.of(
            "street", "Main St",
            "city", "New York",
            "coordinates", Map.of("lat", 40.7128, "lng", -74.0060)
        );
        document.put("user", Map.of(
            "name", "Alice",
            "address", address
        ));

        List<String> expected = Arrays.asList(
            "user.name",
            "user.address.street",
            "user.address.city",
            "user.address.coordinates.lat",
            "user.address.coordinates.lng"
        );
        
        Set<String> actual = fieldEnumerator.enumFields(document);
        assertTrue(actual.containsAll(expected));
    }

    @Test
    void testArraysHandling() {
        Map<String, Object> document = Map.of(
            "products", Arrays.asList(
                Map.of("id", 1, "tags", Arrays.asList("a", "b")),
                Map.of("id", 2, "sizes", Arrays.asList("S", "M"))
            )
        );

        List<String> expected = Arrays.asList(
            "products.0.id",
            "products.0.tags.0",
            "products.0.tags.1",
            "products.1.id",
            "products.1.sizes.0",
            "products.1.sizes.1"
        );
        
        Set<String> actual = fieldEnumerator.enumFields(document);
        assertTrue(actual.containsAll(expected));
    }

    @Test
    void testMixedStructures() {
        Map<String, Object> document = new HashMap<>();
        document.put("a", Arrays.asList(
            Map.of("b", 1),
            Map.of("c", Arrays.asList(
                Map.of("d", 2)
            ))
        ));

        List<String> expected = Arrays.asList(
            "a.0.b",
            "a.1.c.0.d"
        );
        
        Set<String> actual = fieldEnumerator.enumFields(document);
        assertTrue(actual.containsAll(expected));
    }

    @Test
    void testDuplicatePaths() {
        Map<String, Object> document = Map.of(
            "user", Map.of(
                "name", "Bob",
                "contact", Map.of(
                    "email", "bob@test.com",
                    "phones", Arrays.asList(
                        Map.of("type", "mobile", "number", "123"),
                        Map.of("type", "home", "number", "456")
                    )
                )
            )
        );

        List<String> expected = Arrays.asList(
            "user.name",
            "user.contact.email",
            "user.contact.phones.0.type",
            "user.contact.phones.0.number",
            "user.contact.phones.1.type",
            "user.contact.phones.1.number"
        );
        
        Set<String> actual = fieldEnumerator.enumFields(document);
        assertEquals(expected.size(), actual.size());
        assertTrue(actual.containsAll(expected));
    }
}