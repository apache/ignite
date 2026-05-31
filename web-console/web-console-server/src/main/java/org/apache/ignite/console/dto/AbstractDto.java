

package org.apache.ignite.console.dto;

import java.util.UUID;

import io.vertx.core.json.JsonObject;

/**
 * Base class for DTO objects.
 */
public abstract class AbstractDto implements java.io.Serializable{
    /** */
    protected UUID id;

    /**
     * Default constructor.
     */
    protected AbstractDto() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     */
    protected AbstractDto(UUID id) {
        this.id = id;
    }
    
    /**
     * Full constructor.
     *
     * @param id ID.
     */
    protected AbstractDto(String id) {
        this.id = UUID.fromString(id);
    }


    /**
     * @return Object ID.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Object ID.
     */
    public void setId(UUID id) {
        this.id = id;
    }
    
    public static UUID getUUID(JsonObject json,String field) {
    	Object guid = json.getMap().get(field);
    	if(guid instanceof UUID) {
    		return (UUID)guid;
    	}
    	return UUID.fromString(guid.toString());
    }
}
