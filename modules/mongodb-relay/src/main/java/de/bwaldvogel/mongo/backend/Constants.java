package de.bwaldvogel.mongo.backend;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public interface Constants {

    String ID_FIELD = "_id";

    int MAX_NS_LENGTH = 128;

    Set<String> REFERENCE_KEYS = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList("$ref", "$id")));

}
