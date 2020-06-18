package de.bwaldvogel.mongo.backend.postgresql;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

abstract class LegacyUUIDJsonMixIn {

    @JsonCreator
    LegacyUUIDJsonMixIn(@JsonProperty("uuid") UUID uuid) {}

}
