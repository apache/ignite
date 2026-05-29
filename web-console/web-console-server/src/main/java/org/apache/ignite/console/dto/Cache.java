

package org.apache.ignite.console.dto;

import java.util.UUID;

import io.vertx.core.json.JsonArray;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;

import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.context.support.MessageSourceAccessor;

import io.vertx.core.json.JsonObject;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * DTO for cluster cache.
 */
public class Cache extends DataObject {
    /** */
    private String name;

    private String comment;

    /** */
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private int backups;

    /**
     * @param json JSON data.
     * @return New instance of cache DTO.
     */
    public static Cache fromJson(JsonObject json) {
    	UUID id = getUUID(json,"id");
        MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        if (id == null)
            throw new IllegalStateException(messages.getMessage("err.cache-id-not-found"));

        String comment = null;
        JsonArray domains = json.getJsonArray("domains");
        if(!F.isEmpty(domains)){
            for(int i=0;i<domains.size();i++){
                UUID uuid = UUID.fromString(domains.getString(i));
                String tableComment = Model.commentsMap.get(uuid);
                if(tableComment!=null){
                    if(comment==null){
                        comment = tableComment;
                    }
                    else{
                        comment +="; " + tableComment;
                    }
                }
            }
        }

        return new Cache(
            id,
            json.getString("name"),
            comment,
            CacheMode.valueOf(json.getString("cacheMode", PARTITIONED.name())),
            CacheAtomicityMode.valueOf(json.getString("atomicityMode", ATOMIC.name())),
            json.getInteger("backups", 0),
            toJson(json)
        );
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param name Cache name.
     * @param json JSON payload.
     */
    public Cache(
        UUID id,
        String name,
        String comment,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        String json
    ) {
        super(id, json);

        this.name = name;
        this.comment = comment;
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.backups = backups;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode cacheMode() {
        return cacheMode;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @return Cache backups.
     */
    public int backups() {
        return backups;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .put("id", getId())
            .put("name", name)
            .put("comment", comment)
            .put("cacheMode", cacheMode)
            .put("atomicityMode", atomicityMode)
            .put("backups", backups);
    }
}
