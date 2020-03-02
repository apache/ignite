package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 *
 */
public class SecurityContextOfRestClientOnRemoteNodeTest extends JettyRestProcessorCommonSelfTest {
    /**
     *
     */
    private static final String CLNT = "clnt";

    /** Cache name for tests. */
    private static final String CACHE_NAME = "TEST_CACHE";

    public static final String KEY = "TestKey";

    public static final String VALUE = "TestValue";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TestSecurityData[] clientData = new TestSecurityData[] {
            new TestSecurityData(CLNT,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendTaskPermissions(GetValueTask.class.getName(), TASK_EXECUTE)
                    .appendCachePermissions(CACHE_NAME, CACHE_READ)
                    .build()
            )
        };

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(
                new TestSecurityPluginProvider("srv_" + igniteInstanceName, null, ALLOW_ALL, false, clientData)
            );
    }

    @Test
    public void test() throws Exception {
        grid(0).getOrCreateCache(CACHE_NAME).put(KEY, VALUE);

        grid(0).compute().localDeployTask(GetValueTask.class, GetValueTask.class.getClassLoader());

        String res = grid(0).compute(grid(0).cluster().forRemotes()).execute(GetValueTask.class, "");

        assertEquals(VALUE, res);

        Map<String, String> params = new LinkedHashMap<>();

        params.put("cmd", GridRestCommand.EXE.key());
        params.put("name", GetValueTask.class.getName());

        assertEquals(VALUE, jsonValue(content(params), "response", "result"));
    }

    @Override protected long getTestTimeout() {
        return 30_000;
    }

    @Override protected String signature() {
        return null;
    }

    @Override protected String restUrl() {
        return "http://" + LOC_HOST + ":" + restPort() + "/ignite?" + "ignite.login=" + CLNT + "&ignite.password=&";
    }

    private String jsonValue(String json, String... fields) throws IOException {
        JsonNode node = JSON_MAPPER.readTree(json);

        for(String s : fields)
            node = Objects.requireNonNull(node.get(s), "Field '" + s + "' is not found!");

        return node.asText();
    }

    public static class GetValueTask implements ComputeTask<String, String> {
        @IgniteInstanceResource
        private Ignite ignite;

        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable String arg) throws IgniteException {
            return Collections.singletonMap(new ComputeJob() {
                @Override public void cancel() {

                }

                @Override public Object execute() throws IgniteException {

                    return Ignition.localIgnite().cache(CACHE_NAME).get(KEY);
                }
            }, ignite.cluster().forRemotes().node());
        }

        @Nullable @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.stream().findFirst().get().getData();
        }

        @Override
        public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }
    }
}
