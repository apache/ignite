
package org.apache.ignite.console.dto;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.igfs.IgfsMode;
import org.springframework.context.support.MessageSourceAccessor;
import java.util.UUID;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * DTO for cluster IGFS.
 */
public class IGFS extends DataObject {
    /** */
    private String name;

    /** */
    private IgfsMode defaultMode = FileSystemConfiguration.DFLT_MODE;
    /** */
    private boolean fragmentizerEnabled = FileSystemConfiguration.DFLT_FRAGMENTIZER_ENABLED;

    private int blockSize = FileSystemConfiguration.DFLT_BLOCK_SIZE;
    /** */
    private int backups = 0;

    /**
     * @param json JSON data.
     * @return New instance of cache DTO.
     */
    public static IGFS fromJson(JsonObject json) {
        UUID id = getUUID(json,"id");
        MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        if (id == null)
            throw new IllegalStateException(messages.getMessage("err.igfs-id-not-found"));

        return new IGFS(
            id,
            json.getString("name"),
            IgfsMode.valueOf(json.getString("defaultMode", IgfsMode.PRIMARY.name())),
            json.getBoolean("fragmentizerEnabled", true),
            json.getInteger("blockSize", FileSystemConfiguration.DFLT_BLOCK_SIZE),
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
    public IGFS(
        UUID id,
        String name,
        IgfsMode defaultMode,
        boolean fragmentizerEnabled,
        int blockSize,
        int backups,
        String json
    ) {
        super(id, json);

        this.name = name;
        this.defaultMode = defaultMode;
        this.fragmentizerEnabled = fragmentizerEnabled;
        this.backups = backups;
        this.blockSize = blockSize;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
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
            .put("defaultMode", defaultMode)
            .put("fragmentizerEnabled", fragmentizerEnabled)
            .put("blockSize", blockSize)
            .put("backups", backups);
    }
}
