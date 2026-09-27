

package org.apache.ignite.console.common;

import java.io.File;
import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.console.dto.DataObject;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestWrapper;
import org.springframework.util.StopWatch;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.springframework.boot.Banner.Mode.OFF;
import static org.springframework.security.web.authentication.switchuser.SwitchUserFilter.ROLE_PREVIOUS_ADMINISTRATOR;

/**
 * Utilities.
 */
public class Utils {
    /** */
    private static final JsonObject EMPTY_OBJ = new JsonObject();

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    public static String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

        return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param a First set.
     * @param b Second set.
     * @return Elements exists in a and not in b.
     */
    public static TreeSet<UUID> diff(TreeSet<UUID> a, TreeSet<UUID> b) {
        return a.stream().filter(item -> !b.contains(item)).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * @param json JSON object.
     * @param key Key with IDs.
     * @return Set of IDs.
     */
    public static TreeSet<UUID> idsFromJson(JsonObject json, String key) {
        TreeSet<UUID> res = new TreeSet<>();

        JsonArray ids = json.getJsonArray(key);

        if (ids != null)
            ids.forEach(item -> res.add(UUID.fromString(item.toString())));

        return res;
    }

    /**
     * @param json JSON to travers.
     * @param path Dot separated list of properties.
     * @return Tuple with unwind JSON and key to extract from it.
     */
    private static T2<JsonObject, String> xpath(JsonObject json, String path) {
        String[] keys = path.split("\\.");

        for (int i = 0; i < keys.length - 1; i++) {
            json = json.getJsonObject(keys[i]);

            if (json == null)
                json = EMPTY_OBJ;
        }

        String key = keys[keys.length - 1];

        return new T2<>(json, key);
    }

    /**
     * @param json JSON object.
     * @param path Dot separated list of properties.
     * @param def Default value.
     * @return the value or {@code def} if no entry present.
     */
    public static boolean boolParam(JsonObject json, String path, boolean def) {
        T2<JsonObject, String> t = xpath(json, path);

        return t.getKey().getBoolean(t.getValue(), def);
    }

    /**
     * @param data Collection of DTO objects.
     * @return JSON array.
     */
    public static JsonArray toJsonArray(Collection<? extends DataObject> data) {
        JsonArray res = new JsonArray();

        data.forEach(item -> res.add(fromJson(item.json())));

        return res;
    }

    /**
     * @return Current request origin.
     */
    public static String currentRequestOrigin() {
        return ServletUriComponentsBuilder
            .fromCurrentRequest()
            .replacePath(null)
            .build()
            .toString();
    }

    /**
     * Is switch user used.
     * @param req Request wrapper.
     * @return Switch user used flag.
     */
    public static boolean isBecomeUsed(SecurityContextHolderAwareRequestWrapper req) {
        return req.isUserInRole(ROLE_PREVIOUS_ADMINISTRATOR);
    }

    /**
     * @param auth Auth.
     * @param role Role.
     * @return Authority by name.
     */
    public static GrantedAuthority getAuthority(Authentication auth, String role) {
        return auth.getAuthorities().stream().filter(a -> role.equals(a.getAuthority())).findFirst().orElse(null);
    }

    /**
     * Static helper that can be used to run a {@link SpringApplication} from the specified source using default
     * settings.
     *
     * @param cls the source to load.
     * @param appName Application name.
     * @param args the application arguments (usually passed from a Java main method).
     * @return the running {@link ApplicationContext}
     */
    public static ConfigurableApplicationContext run(Class<?> cls, String appName, String... args) {
        SpringApplication app = new SpringApplication(cls);

        app.setBannerMode(OFF);
        app.setLogStartupInfo(false);

        System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");
        System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true)");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        ConfigurableApplicationContext ctx = app.run(args);

        stopWatch.stop();

        Logger log = LoggerFactory.getLogger(cls);

        try {
            File logsDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "log", false);

            log.info("Full log is available in {}", logsDir.getAbsolutePath());
        }
        catch (IgniteCheckedException ignored) {
            // No-op.
        }

        String port = ctx.getEnvironment().getProperty("local.server.port");

        log.info("{} started on TCP port {} in {} seconds", appName, port, stopWatch.getTotalTimeSeconds());

        return ctx;
    }
}
