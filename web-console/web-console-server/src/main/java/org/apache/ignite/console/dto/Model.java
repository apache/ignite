/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.dto;

import java.util.UUID;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.context.support.MessageSourceAccessor;

import static java.lang.Boolean.FALSE;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * DTO for cluster model.
 */
public class Model extends DataObject {
    /** */
    private boolean hasIdx;

    /** */
    private String keyType;

    /** */
    private String valType;

    /**
     * @param json JSON data.
     * @return New instance of model DTO.
     */
    public static Model fromJson(JsonObject json) {
        UUID id = json.getUuid("id");
        MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        if (id == null)
            throw new IllegalStateException(messages.getMessage("err.model-id-not-found"));

        boolean generatePojo = FALSE.equals(json.getBoolean("generatePojo"));
        boolean missingDb = F.isEmpty(json.getString("databaseSchema")) && F.isEmpty(json.getString("databaseTable"));

        boolean hasIdx = !F.isEmpty(json.getJsonArray("keyFields")) ||
            "Annotations".equals(json.getString("queryMetadata")) && (generatePojo || missingDb);

        return new Model(
            id,
            hasIdx,
            json.getString("keyType"),
            json.getString("valueType"),
            toJson(json)
        );
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param hasIdx Model has at least one index.
     * @param keyType Key type name.
     * @param valType Value type name.
     * @param json JSON payload.
     */
    public Model(UUID id, boolean hasIdx, String keyType, String valType, String json) {
        super(id, json);

        this.hasIdx = hasIdx;
        this.keyType = keyType;
        this.valType = valType;
    }

    /**
     * @return {@code true} if model has at least one index.
     */
    public boolean hasIndex() {
        return hasIdx;
    }

    /**
     * @return Key type name.
     */
    public String keyType() {
        return keyType;
    }

    /**
     * @return Value type name.
     */
    public String valueType() {
        return valType;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .add("id", getId())
            .add("hasIndex", hasIdx)
            .add("keyType", keyType)
            .add("valueType", valType);
    }
}
