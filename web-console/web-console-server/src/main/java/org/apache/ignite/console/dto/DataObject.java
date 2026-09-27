

package org.apache.ignite.console.dto;

import java.util.UUID;

import io.vertx.core.json.JsonObject;


/**
 * Abstract data object.
 */
public abstract class DataObject extends AbstractDto {
    /** */
    private String json;

    /**
     * Default constructor.
     */
    protected DataObject() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param json JSON encoded payload.
     */
    protected DataObject(UUID id, String json) {
        super(id);

        this.json = json;
    }

    /**
     * @return JSON encoded payload.
     */
    public String json() {
        return json;
    }

    /**
     * @param json JSON encoded payload.
     */
    public void json(String json) {
        this.json = json;
    }

    /**
     * @return JSON value suitable for short lists.
     */
    public abstract JsonObject shortView();
}
