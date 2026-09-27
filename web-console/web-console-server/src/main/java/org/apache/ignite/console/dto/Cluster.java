

package org.apache.ignite.console.dto;

import java.util.UUID;

import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.context.support.MessageSourceAccessor;

import io.vertx.core.json.JsonObject;

import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * DTO for cluster configuration.
 */
public class Cluster extends DataObject {
    /** */
    private String name;

    /** */
    private String discovery;

    private String comment;

    /**
     * @param json JSON data.
     * @return New instance of cluster DTO.
     */
    public static Cluster fromJson(JsonObject json) {
        UUID id = getUUID(json,"id");
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
            json.getString("comment"),
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
    public Cluster(UUID id, String name, String discovery, String comment, String json) {
        super(id, json);

        this.name = name;
        this.discovery = discovery;
        this.comment = comment;
    }

    /**
     * @return Cluster name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Cluster description.
     */
    public String comment() {
        return comment;
    }

    /**
     * @return Cluster discovery.
     */
    public String discovery() {
        return discovery;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .put("id", getId())
            .put("name", name)
            .put("comment", comment)
            .put("discovery", discovery);
    }
}
