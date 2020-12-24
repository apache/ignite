package org.apache.ignite.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import org.apache.ignite.cli.builtins.init.InitIgniteCommand;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.builtins.module.ModuleStorage;
import org.apache.ignite.cli.builtins.module.StandardModuleDefinition;
import org.apache.ignite.cli.builtins.node.NodeManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("ignite")
@ExtendWith(MockitoExtension.class)
public class IgniteCliInterfaceTest {

    ApplicationContext applicationContext;
    ByteArrayOutputStream err;
    ByteArrayOutputStream out;

    @Mock CliPathsConfigLoader cliPathsConfigLoader;

    @BeforeEach
    void setup() {
        applicationContext = ApplicationContext.run(Environment.TEST);
        applicationContext.registerSingleton(cliPathsConfigLoader);
        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();
    }

    CommandLine commandLine(ApplicationContext applicationContext) {
        CommandLine.IFactory factory = new CommandFactory(applicationContext);
        return new CommandLine(IgniteCliSpec.class, factory)
            .setErr(new PrintWriter(err, true))
            .setOut(new PrintWriter(out, true));
    }

    @DisplayName("init")
    @Nested
    class Init {

        @Test
        @DisplayName("init")
        void init() {
            var initIgniteCommand = mock(InitIgniteCommand.class);
            applicationContext.registerSingleton(InitIgniteCommand.class, initIgniteCommand);
            CommandLine cli = commandLine(applicationContext);
            assertEquals(0, cli.execute("init"));
            verify(initIgniteCommand).init(any(), any());
        }
    }

    @DisplayName("module")
    @Nested
    class Module {

        @Mock ModuleManager moduleManager;
        @Mock ModuleStorage moduleStorage;

        @BeforeEach
        void setUp() {
            applicationContext.registerSingleton(moduleManager);
            applicationContext.registerSingleton(moduleStorage);
        }

        @Test
        @DisplayName("add mvn:groupId:artifact:version")
        void add() {
            IgnitePaths paths = new IgnitePaths(Path.of("binDir"),
                Path.of("worksDir"), "version");
            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError()).thenReturn(paths);

            var exitCode =
                commandLine(applicationContext).execute("module add mvn:groupId:artifactId:version".split(" "));
            verify(moduleManager).addModule("mvn:groupId:artifactId:version", paths, Arrays.asList());
            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("add mvn:groupId:artifact:version --repo http://mvnrepo.com/repostiory")
        void addWithCustomRepo() throws MalformedURLException {
            doNothing().when(moduleManager).addModule(any(), any(), any());

            IgnitePaths paths = new IgnitePaths(Path.of("binDir"),
                Path.of("worksDir"), "version");
            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError()).thenReturn(paths);

            var exitCode =
                commandLine(applicationContext)
                    .execute("module add mvn:groupId:artifactId:version --repo http://mvnrepo.com/repostiory".split(" "));
            verify(moduleManager).addModule("mvn:groupId:artifactId:version", paths,
                Arrays.asList(new URL("http://mvnrepo.com/repostiory")));
            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("add test-module")
        void addBuiltinModule() {
            doNothing().when(moduleManager).addModule(any(), any(), any());

            IgnitePaths paths = new IgnitePaths(Path.of("binDir"),
                Path.of("worksDir"), "version");
            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError()).thenReturn(paths);

            var exitCode =
                commandLine(applicationContext).execute("module add test-module".split(" "));
            verify(moduleManager).addModule("test-module", paths, Collections.emptyList());
            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("remove builtin-module")
        void remove() {
            var moduleName = "builtin-module";
            when(moduleManager.removeModule(moduleName)).thenReturn(true);

            var exitCode =
                commandLine(applicationContext).execute("module remove builtin-module".split(" "));
            verify(moduleManager).removeModule(moduleName);
            assertEquals(0, exitCode);
            assertEquals("Module " + moduleName + " was removed successfully.\n", out.toString());
        }

        @Test
        @DisplayName("remove unknown-module")
        void removeUnknownModule() {
            var moduleName = "unknown-module";
            when(moduleManager.removeModule(moduleName)).thenReturn(false);

            var exitCode =
                commandLine(applicationContext).execute("module remove unknown-module".split(" "));
            verify(moduleManager).removeModule(moduleName);
            assertEquals(0, exitCode);
            assertEquals("Nothing to do: module " + moduleName + " is not yet added.\n", out.toString());
        }

        @Test
        @DisplayName("list")
        void list() {

            var module1 = new StandardModuleDefinition("module1", "description1", Collections.singletonList("artifact1"), Collections.singletonList("cli-artifact1"));
            var module2 = new StandardModuleDefinition("module2", "description2", Collections.singletonList("artifact2"), Collections.singletonList("cli-artifact2"));
            when(moduleManager.builtinModules()).thenReturn(Arrays.asList(module1, module2));

            var externalModule = new ModuleStorage.ModuleDefinition(
                "org.apache.ignite:snapshot:2.9.0",
                Collections.emptyList(),
                Collections.emptyList(),
                ModuleStorage.SourceType.Maven, "mvn:org.apache.ignite:snapshot:2.9.0");
            when(moduleStorage.listInstalled()).thenReturn(
                new ModuleStorage.ModuleDefinitionsRegistry(
                    Arrays.asList(
                        new ModuleStorage.ModuleDefinition(
                            module1.name,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            ModuleStorage.SourceType.Standard, ""), externalModule)));

            var exitCode =
                commandLine(applicationContext).execute("module list".split(" "));
            verify(moduleManager).builtinModules();
            assertEquals(0, exitCode);

            var expectedOutput = "Optional Ignite Modules\n" +
                "+---------+--------------+------------+\n" +
                "| Name    | Description  | Installed? |\n" +
                "+---------+--------------+------------+\n" +
                "| module1 | description1 | Yes        |\n" +
                "+---------+--------------+------------+\n" +
                "| module2 | description2 | No         |\n" +
                "+---------+--------------+------------+\n" +
                "\n" +
                "Additional Maven Dependencies\n" +
                "+-------------------+-------------+---------+\n" +
                "| Group ID          | Artifact ID | Version |\n" +
                "+-------------------+-------------+---------+\n" +
                "| org.apache.ignite | snapshot    | 2.9.0   |\n" +
                "+-------------------+-------------+---------+\n" +
                "Type ignite module remove <groupId>:<artifactId>:<version> to remove a dependency.\n";
            assertEquals(expectedOutput, out.toString());
        }
    }

    @Nested
    @DisplayName("node")
    class Node {

        @Mock NodeManager nodeManager;

        @BeforeEach
        void setUp() {
            applicationContext.registerSingleton(nodeManager);
        }

        @Test
        @DisplayName("start node1 --config conf.json")
        void start() {
           var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
           var nodeName = "node1";
           var node =
               new NodeManager.RunningNode(1, nodeName, Path.of("logfile"));
           when(nodeManager.start(any(), any(), any(), any()))
               .thenReturn(node);

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute(("node start " + nodeName + " --config conf.json").split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).start(nodeName, ignitePaths.workDir, ignitePaths.cliPidsDir(), Path.of("conf.json"));
            assertEquals("Starting a new Ignite node...\n\nNode is successfully started. To stop, type ignite node stop " + nodeName + "\n\n" +
                "+---------------+---------+\n" +
                "| Consistent ID | node1   |\n" +
                "+---------------+---------+\n" +
                "| PID           | 1       |\n" +
                "+---------------+---------+\n" +
                "| Log File      | logfile |\n" +
                "+---------------+---------+\n",
                out.toString());
        }

        @Test
        @DisplayName("stop node1")
        void stopRunning() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            var nodeName = "node1";
            when(nodeManager.stopWait(any(), any()))
                .thenReturn(true);

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute(("node stop " + nodeName).split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertEquals("Stopping locally running node with consistent ID " + nodeName + "... Done!\n",
                out.toString());
        }

        @Test
        @DisplayName("stop unknown-node")
        void stopUnknown() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            var nodeName = "unknown-node";
            when(nodeManager.stopWait(any(), any()))
                .thenReturn(false);

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute(("node stop " + nodeName).split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).stopWait(nodeName, ignitePaths.cliPidsDir());
            assertEquals("Stopping locally running node with consistent ID " + nodeName + "... Failed\n",
                out.toString());
        }

        @Test
        @DisplayName("list")
        void list() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            when(nodeManager.getRunningNodes(any(), any()))
                .thenReturn(Arrays.asList(
                    new NodeManager.RunningNode(1, "new1", Path.of("logFile1")),
                    new NodeManager.RunningNode(2, "new2", Path.of("logFile2"))
                ));

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute("node list".split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).getRunningNodes(ignitePaths.workDir, ignitePaths.cliPidsDir());
            assertEquals("Currently, there are 2 locally running nodes.\n\n" +
                "+---------------+-----+----------+\n" +
                "| Consistent ID | PID | Log File |\n" +
                "+---------------+-----+----------+\n" +
                "| new1          | 1   | logFile1 |\n" +
                "+---------------+-----+----------+\n" +
                "| new2          | 2   | logFile2 |\n" +
                "+---------------+-----+----------+\n",
                out.toString());
        }

        @Test
        @DisplayName("list")
        void listEmpty() {
            var ignitePaths = new IgnitePaths(Path.of(""), Path.of(""), "version");
            when(nodeManager.getRunningNodes(any(), any()))
                .thenReturn(Arrays.asList());

            when(cliPathsConfigLoader.loadIgnitePathsOrThrowError())
                .thenReturn(ignitePaths);

            var exitCode =
                commandLine(applicationContext).execute("node list".split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).getRunningNodes(ignitePaths.workDir, ignitePaths.cliPidsDir());
            assertEquals("Currently, there are no locally running nodes.\n\n" +
                "Use the ignite node start command to start a new node.\n", out.toString());
        }

        @Test
        @DisplayName("classpath")
        void classpath() throws IOException {
            when(nodeManager.classpathItems()).thenReturn(Arrays.asList("item1", "item2"));

            var exitCode = commandLine(applicationContext).execute("node classpath".split(" "));

            assertEquals(0, exitCode);
            verify(nodeManager).classpathItems();
            assertEquals("Current Ignite node classpath:\n    item1\n    item2\n", out.toString());
        }
    }

    @Nested
    @DisplayName("config")
    class Config {

        @Mock private HttpClient httpClient;
        @Mock private HttpResponse<String> response;

        @BeforeEach
        void setUp() {
            applicationContext.registerSingleton(httpClient);
        }

        @Test
        @DisplayName("get --node-endpoint localhost:8081")
        void get() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(response.body()).thenReturn("{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}");
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var exitCode =
                commandLine(applicationContext).execute("config get --node-endpoint localhost:8081".split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/") &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("{\n" +
                "  \"baseline\" : {\n" +
                "    \"autoAdjust\" : {\n" +
                "      \"enabled\" : true\n" +
                "    }\n" +
                "  }\n" +
                "}\n", out.toString());
        }

        @Test
        @DisplayName("get --node-endpoint localhost:8081 --selector local.baseline")
        void getSubtree() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(response.body()).thenReturn("{\"autoAdjust\":{\"enabled\":true}}");
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var exitCode =
                commandLine(applicationContext).execute(("config get --node-endpoint localhost:8081 " +
                    "--selector local.baseline").split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/local.baseline") &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("{\n" +
                "  \"autoAdjust\" : {\n" +
                "    \"enabled\" : true\n" +
                "  }\n" +
                "}\n", out.toString());
        }

        @Test
        @DisplayName("set --node-endpoint localhost:8081 local.baseline.autoAdjust.enabled=true")
        void setHocon() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var expectedSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            var exitCode =
                commandLine(applicationContext).execute(("config set --node-endpoint localhost:8081 " +
                    "local.baseline.autoAdjust.enabled=true"
                    ).split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/") &&
                    r.method().equals("POST") &&
                    // TODO: body matcher should be fixed to more appropriate
                    r.bodyPublisher().get().contentLength() == expectedSentContent.getBytes().length &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("Configuration was updated successfully.\n\n" +
                "Use the ignite config get command to view the updated configuration.\n", out.toString());
        }

        @Test
        @DisplayName("set --node-endpoint localhost:8081 {\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}")
        void setJson() throws IOException, InterruptedException {
            when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
            when(httpClient.<String>send(any(), any())).thenReturn(response);

            var expectedSentContent = "{\"local\":{\"baseline\":{\"autoAdjust\":{\"enabled\":true}}}}";

            var exitCode =
                commandLine(applicationContext).execute(("config set --node-endpoint localhost:8081 " +
                    "local.baseline.autoAdjust.enabled=true"
                ).split(" "));

            assertEquals(0, exitCode);
            verify(httpClient).send(
                argThat(r -> r.uri().toString().equals("http://localhost:8081/management/v1/configuration/") &&
                    r.method().equals("POST") &&
                    // TODO: body matcher should be fixed to more appropriate
                    r.bodyPublisher().get().contentLength() == expectedSentContent.getBytes().length &&
                    r.headers().firstValue("Content-Type").get().equals("application/json")),
                any());
            assertEquals("Configuration was updated successfully.\n\n" +
                "Use the ignite config get command to view the updated configuration.\n", out.toString());
        }
    }
}
