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

import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * DTO for cluster configuration.
 */
public class Cluster extends DataObject {
    /** */
    private String name;

    /** */
    private String discovery;

    /**
     * @param json JSON data.
     * @return New instance of cluster DTO.
     */
    public static Cluster fromJson(JsonObject json) {
        UUID id = json.getUuid("id");
        MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        if (id == null)
            throw new IllegalStateException(messages.getMessage("err.cluster-id-not-found"));

        String name = json.getString("name");

        if (F.isEmpty(name))
            throw new IllegalStateException(messages.getMessage("err.cluster-name-is-empty"));

        JsonObject discovery = json.getJsonObject("discovery");

        if (discovery == null)
            throw new IllegalStateException(messages.getMessage("err.cluster-discovery-not-found"));

        String discoveryKind = discovery.getString("kind");

        if (F.isEmpty(discoveryKind))
            throw new IllegalStateException(messages.getMessage("err.cluster-discovery-kind-not-found"));

        return new Cluster(
            id,
            name,
            discoveryKind,
            toJson(json)
        );
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param name Cluster name.
     * @param discovery Cluster discovery.
     * @param json JSON payload.
     */
    public Cluster(UUID id, String name, String discovery, String json) {
        super(id, json);

        this.name = name;
        this.discovery = discovery;
    }

    /**
     * @return name Cluster name.
     */
    public String name() {
        return name;
    }

    /**
     * @return name Cluster discovery.
     */
    public String discovery() {
        return discovery;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .add("id", getId())
            .add("name", name)
            .add("discovery", discovery);
    }
}
