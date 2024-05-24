package mock;


import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.client.cluster.RemoteInfoResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class OpenSearchRestHighLevelClientTestCase extends OpenSearchRestTestCase {

    protected static final String CONFLICT_PIPELINE_ID = "conflict_pipeline";

    private static RestHighLevelClient restHighLevelClient;
    private static boolean async = false;
    
    public static XContentBuilder jsonBuilder() throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent());
    }


    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        IOUtils.close(restHighLevelClient);
        restHighLevelClient = null;
    }

    protected static RestHighLevelClient highLevelClient() {
        return restHighLevelClient;
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    protected static <Req, Resp> Resp execute(Req request, SyncMethod<Req, Resp> syncMethod, AsyncMethod<Req, Resp> asyncMethod)
        throws IOException {
        return execute(request, syncMethod, asyncMethod, RequestOptions.DEFAULT);
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    protected static <Req, Resp> Resp execute(
        Req request,
        SyncMethod<Req, Resp> syncMethod,
        AsyncMethod<Req, Resp> asyncMethod,
        RequestOptions options
    ) throws IOException {
        if (async == false) {
            return syncMethod.execute(request, options);
        } else {
            PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
            asyncMethod.execute(request, options, future);
            return future.actionGet();
        }
    }

    /**
     * Executes the provided request using either the sync method or its async
     * variant, both provided as functions. This variant is used when the call does
     * not have a request object (only headers and the request path).
     */
    protected static <Resp> Resp execute(
        SyncMethodNoRequest<Resp> syncMethodNoRequest,
        AsyncMethodNoRequest<Resp> asyncMethodNoRequest,
        RequestOptions requestOptions
    ) throws IOException {
        if (async == false) {
            return syncMethodNoRequest.execute(requestOptions);
        } else {
            PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
            asyncMethodNoRequest.execute(requestOptions, future);
            return future.actionGet();
        }
    }

    @FunctionalInterface
    protected interface SyncMethod<Request, Response> {
        Response execute(Request request, RequestOptions options) throws IOException;
    }

    @FunctionalInterface
    protected interface SyncMethodNoRequest<Response> {
        Response execute(RequestOptions options) throws IOException;
    }

    @FunctionalInterface
    protected interface AsyncMethod<Request, Response> {
        void execute(Request request, RequestOptions options, ActionListener<Response> listener);
    }

    @FunctionalInterface
    protected interface AsyncMethodNoRequest<Response> {
        void execute(RequestOptions options, ActionListener<Response> listener);
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, new SearchModule(Settings.EMPTY, !true, Collections.emptyList()).getNamedXContents());
        }
    }

    protected static XContentBuilder buildRandomXContentPipeline(XContentBuilder pipelineBuilder) throws IOException {
        pipelineBuilder.startObject();
        {
            pipelineBuilder.field(Pipeline.DESCRIPTION_KEY, "some random set of processors");
            pipelineBuilder.startArray(Pipeline.PROCESSORS_KEY);
            {
                pipelineBuilder.startObject().startObject("set");
                {
                    pipelineBuilder.field("field", "foo").field("value", "bar");
                }
                pipelineBuilder.endObject().endObject();
                pipelineBuilder.startObject().startObject("convert");
                {
                    pipelineBuilder.field("field", "rank").field("type", "integer");
                }
                pipelineBuilder.endObject().endObject();
            }
            pipelineBuilder.endArray();
        }
        pipelineBuilder.endObject();
        return pipelineBuilder;
    }

    protected static XContentBuilder buildRandomXContentPipeline() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder pipelineBuilder = XContentBuilder.builder(xContentType.xContent());
        return buildRandomXContentPipeline(pipelineBuilder);
    }

    private static XContentType randomFrom(XContentType[] values) {
		int i = new Random().nextInt(values.length);
		return values[i];
	}


	protected static void createFieldAddingPipleine(String id, String fieldName, String value) throws IOException {
        XContentBuilder pipeline = jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject("set")
            .field("field", fieldName)
            .field("value", value)
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        createPipeline(new PutPipelineRequest(id, BytesReference.bytes(pipeline), XContentType.JSON));
    }

    protected static void createPipeline(String pipelineId) throws IOException {
        XContentBuilder builder = buildRandomXContentPipeline();
        createPipeline(new PutPipelineRequest(pipelineId, BytesReference.bytes(builder), builder.contentType()));
    }

    protected static void createPipeline(PutPipelineRequest putPipelineRequest) throws IOException {
        assertTrue(
            execute(putPipelineRequest, highLevelClient().ingest()::putPipeline, highLevelClient().ingest()::putPipelineAsync)
                .isAcknowledged()
        );
    }

    protected static void clusterUpdateSettings(Settings persistentSettings, Settings transientSettings) throws IOException {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(persistentSettings);
        request.transientSettings(transientSettings);
        assertTrue(
            execute(request, highLevelClient().cluster()::putSettings, highLevelClient().cluster()::putSettingsAsync).isAcknowledged()
        );
    }

    protected void putConflictPipeline() throws IOException {
        final XContentBuilder pipelineBuilder = jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject("set")
            .field("field", "_version")
            .field("value", 1)
            .endObject()
            .endObject()
            .startObject()
            .startObject("set")
            .field("field", "_id")
            .field("value", "1")
            .endObject()
            .endObject()
            .endArray()
            .endObject();
        final PutPipelineRequest putPipelineRequest = new PutPipelineRequest(
            CONFLICT_PIPELINE_ID,
            BytesReference.bytes(pipelineBuilder),
            pipelineBuilder.contentType()
        );
        assertTrue(highLevelClient().ingest().putPipeline(putPipelineRequest, RequestOptions.DEFAULT).isAcknowledged());
    }

    @Override
    protected Settings restClientSettings() {
        final String user = Objects.requireNonNull(System.getProperty("tests.rest.cluster.username","ignite"));
        final String pass = Objects.requireNonNull(System.getProperty("tests.rest.cluster.password","ignite"));
        final String token = "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected Iterable<SearchHit> searchAll(String... indices) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indices);
        return searchAll(searchRequest);
    }

    protected Iterable<SearchHit> searchAll(SearchRequest searchRequest) throws IOException {
        refreshIndexes(searchRequest.indices());
        SearchResponse search = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        return search.getHits();
    }

    protected void refreshIndexes(String... indices) throws IOException {
        String joinedIndices = Arrays.stream(indices).collect(Collectors.joining(","));
        Response refreshResponse = client().performRequest(new Request("POST", "/" + joinedIndices + "/_refresh"));
        assertEquals(200, refreshResponse.getStatusLine().getStatusCode());
    }

    protected void createIndexWithMultipleShards(String index) throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest(index);
        int shards = 9;//randomIntBetween(8, 10);
        indexRequest.settings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0));
        highLevelClient().indices().create(indexRequest, RequestOptions.DEFAULT);
    }

    protected static void setupRemoteClusterConfig(String remoteClusterName) throws Exception {
        // Configure local cluster as remote cluster:
        // TODO: replace with nodes info highlevel rest client code when it is available:
        final Request request = new Request("GET", "/_nodes");
        Map<?, ?> nodesResponse = (Map<?, ?>) toMap(client().performRequest(request)).get("nodes");
        // Select node info of first node (we don't know the node id):
        nodesResponse = (Map<?, ?>) nodesResponse.get(nodesResponse.keySet().iterator().next());
        String transportAddress = (String) nodesResponse.get("transport_address");

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(singletonMap("cluster.remote." + remoteClusterName + ".seeds", transportAddress));
        ClusterUpdateSettingsResponse updateSettingsResponse = restHighLevelClient.cluster()
            .putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
        assertThat(updateSettingsResponse.isAcknowledged(), is(true));

        assertBusy(() -> {
            RemoteInfoResponse response = highLevelClient().cluster().remoteInfo(new RemoteInfoRequest(), RequestOptions.DEFAULT);
            assertThat(response, notNullValue());
            assertThat(response.getInfos().size(), greaterThan(0));
        });
    }

    protected static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

    protected static TaskId findTaskToRethrottle(String actionName, String description) throws IOException {
        long start = System.nanoTime();
        ListTasksRequest request = new ListTasksRequest();
        request.setActions(actionName);
        request.setDetailed(true);
        do {
            ListTasksResponse list = highLevelClient().tasks().list(request, RequestOptions.DEFAULT);
            list.rethrowFailures("Finding tasks to rethrottle");
            List<TaskGroup> taskGroups = list.getTaskGroups()
                .stream()
                .filter(taskGroup -> taskGroup.getTaskInfo().getDescription().equals(description))
                .collect(Collectors.toList());
            assertThat("tasks are left over from the last execution of this test", taskGroups, hasSize(lessThan(2)));
            if (0 == taskGroups.size()) {
                // The parent task hasn't started yet
                continue;
            }
            TaskGroup taskGroup = taskGroups.get(0);
            assertThat(taskGroup.getChildTasks(), empty());
            return taskGroup.getTaskInfo().getTaskId();
        } while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
        throw new AssertionError(
            "Couldn't find tasks to rethrottle. Here are the running tasks "
                + highLevelClient().tasks().list(request, RequestOptions.DEFAULT)
        );
    }

    protected static CheckedRunnable<Exception> checkTaskCompletionStatus(RestClient client, String taskId) {
        return () -> {
            Response response = client.performRequest(new Request("GET", "/_tasks/" + taskId));
            assertTrue((boolean) entityAsMap(response).get("completed"));
        };
    }
}
