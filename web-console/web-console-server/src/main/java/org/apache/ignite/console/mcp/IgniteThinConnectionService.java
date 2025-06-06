package org.apache.ignite.console.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.logaritex.mcp.annotation.McpArg;
import com.logaritex.mcp.annotation.McpPrompt;

import java.sql.*;
import java.util.*;

@Service
public class IgniteThinConnectionService {

    String jdbcUrl;

    Optional<String> jdbcUser;


    Optional<String> jdbcPassword;

    private JsonObject cluster;

    private ConfigurationsService repo;

    public IgniteThinConnectionService(ConfigurationsService repo, AccountsService accountsService) {
        this.repo = repo;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, jdbcUser.orElse(null), jdbcPassword.orElse(null));
    }

    @Tool(description = "Execute a SELECT query on the jdbc database")
    String read_query(@ToolParam(description = "SELECT SQL query to execute") String query, ToolContext toolContext) {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    if (value != null) {
                        row.put(columnName, value.toString());
                    } else {
                        row.put(columnName, null);
                    }
                }
                results.add(row);
            }
            return Utils.toJson(results);

        } catch (Exception e) {
            throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
        }
    }

    @Tool(description = "Execute a INSERT, UPDATE or DELETE query on the jdbc database")
    String write_query(@ToolParam(description = "INSERT, UPDATE or DELETE SQL query to execute") String query) {
        if (query.strip().toUpperCase().startsWith("SELECT")) {
            throw new RuntimeException("SELECT queries are not allowed for write_query", null);
        }

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(query);
            return "Query executed successfully";
        } catch (Exception e) {
            throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
        }
    }

    @Tool(description = "List all tables in the jdbc database")
    String list_tables() {
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, null, "%", new String[] { "TABLE" });

            List<Map<String, String>> tables = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> table = new HashMap<>();
                table.put("TABLE_CAT", rs.getString("TABLE_CAT"));
                table.put("TABLE_SCHEM", rs.getString("TABLE_SCHEM"));
                table.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                table.put("REMARKS", rs.getString("REMARKS"));
                tables.add(table);
            }
            return Utils.toJson(tables);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list tables: " + e.getMessage(), e);
        }
    }

    @Tool(description = "Create new table in the jdbc database")
    String create_table(@ToolParam(description = "CREATE TABLE SQL statement") String query) {
        if (!query.strip().toUpperCase().startsWith("CREATE TABLE")) {
            throw new RuntimeException("Only CREATE TABLE statements are allowed", null);
        }
        return write_query(query);
    }

    @Tool(description = "Describe table")
    String describe_table(@ToolParam(description = "Catalog name", required = false) String catalog,
            @ToolParam(description = "Schema name", required = false) String schema,
            @ToolParam(description = "Table name") String table) {
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(catalog, schema, table, null);

            List<Map<String, String>> columns = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> column = new HashMap<>();
                column.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                column.put("TYPE_NAME", rs.getString("TYPE_NAME"));
                column.put("COLUMN_SIZE", rs.getString("COLUMN_SIZE"));
                column.put("NULLABLE", rs.getString("IS_NULLABLE"));
                column.put("REMARKS", rs.getString("REMARKS"));
                column.put("COLUMN_DEF", rs.getString("COLUMN_DEF"));
                columns.add(column);
            }
            return Utils.toJson(columns);
        } catch (Exception e) {
            throw new RuntimeException("Failed to describe table: " + e.getMessage());
        }
    }

    @Tool(description = "Get information about the database. Run this before anything else to know the SQL dialect, keywords etc.")
    String database_info() {
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            Map<String, String> info = new HashMap<>();

            info.put("database_product_name", metaData.getDatabaseProductName());
            info.put("database_product_version", metaData.getDatabaseProductVersion());
            info.put("driver_name", metaData.getDriverName());
            info.put("driver_version", metaData.getDriverVersion());
            //info.put("url", metaData.getURL());
            //info.put("username", metaData.getUserName());
            info.put("max_connections", String.valueOf(metaData.getMaxConnections()));
            info.put("read_only", String.valueOf(metaData.isReadOnly()));
            info.put("supports_transactions", String.valueOf(metaData.supportsTransactions()));
            info.put("sql_keywords", metaData.getSQLKeywords());

            return Utils.toJson(info);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get database info: " + e.getMessage(), e);
        }
    }

    @McpPrompt(description = "Creates sample data and perform analysis")
    McpSchema.PromptMessage sample_data(@McpArg(description = "The topic") String topic) {
        return new McpSchema.PromptMessage(McpSchema.Role.USER, new McpSchema.TextContent(
                """
                            The assistants goal is to walkthrough an informative demo of MCP. To demonstrate the Model Context Protocol (MCP) we will leverage this example server to interact with an JDBC database.
                            It is important that you first explain to the user what is going on. The user has downloaded and installed the JDBC MCP Server and is now ready to use it.
                            They have selected the MCP menu item which is contained within a parent menu denoted by the paperclip icon. Inside this menu they selected an icon that illustrates two electrical plugs connecting. This is the MCP menu.
                            Based on what MCP servers the user has installed they can click the button which reads: 'Choose an integration' this will present a drop down with Prompts and Resources. The user has selected the prompt titled: 'mcp-demo'.
                            This text file is that prompt. The goal of the following instructions is to walk the user through the process of using the 3 core aspects of an MCP server. These are: Prompts, Tools, and Resources.
                            They have already used a prompt and provided a topic. The topic is: {topic}. The user is now ready to begin the demo.
                            Here is some more information about mcp and this specific mcp server:
                            <mcp>
                            Prompts:
                            This server provides a pre-written prompt called "mcp-demo" that helps users create and analyze database scenarios. The prompt accepts a "topic" argument and guides users through creating tables, analyzing data, and generating insights. For example, if a user provides "retail sales" as the topic, the prompt will help create relevant database tables and guide the analysis process. Prompts basically serve as interactive templates that help structure the conversation with the LLM in a useful way.
                            Tools:
                            This server provides several SQL-related tools:
                            "read_query": Executes SELECT queries to read data from the database
                            "write_query": Executes INSERT, UPDATE, or DELETE queries to modify data
                            "create_table": Creates new tables in the database
                            "list_tables": Shows all existing tables
                            "describe_table": Shows the schema for a specific table
                            </mcp>
                            <demo-instructions>
                            You are an AI assistant tasked with generating a comprehensive business scenario based on a given topic.
                            Your goal is to create a narrative that involves a data-driven business problem, develop a database structure to support it, generate relevant queries, create a dashboard, and provide a final solution.

                            At each step you will pause for user input to guide the scenario creation process. Overall ensure the scenario is engaging, informative, and demonstrates the capabilities of the JDBC MCP Server.
                            You should guide the scenario to completion. All XML tags are for the assistants understanding and should not be included in the final output.

                            1. The user has chosen the topic: {topic}.

                            2. Create a business problem narrative:
                            a. Describe a high-level business situation or problem based on the given topic.
                            b. Include a protagonist (the user) who needs to collect and analyze data from a database.
                            c. Add an external, potentially comedic reason why the data hasn't been prepared yet.
                            d. Mention an approaching deadline and the need to use Claude (you) as a business tool to help.

                            3. Setup the data:
                            a. Instead of asking about the data that is required for the scenario, just go ahead and use the tools to create the data. Inform the user you are "Setting up the data".
                            b. Design a set of table schemas that represent the data needed for the business problem.
                            c. Include at least 2-3 tables with appropriate columns and data types.
                            d. Leverage the tools to create the tables in the JDBC database.
                            e. Create INSERT statements to populate each table with relevant synthetic data.
                            f. Ensure the data is diverse and representative of the business problem.
                            g. Include at least 10-15 rows of data for each table.

                            4. Pause for user input:
                            a. Summarize to the user what data we have created.
                            b. Present the user with a set of multiple choices for the next steps.
                            c. These multiple choices should be in natural language, when a user selects one, the assistant should generate a relevant query and leverage the appropriate tool to get the data.

                            6. Iterate on queries:
                            a. Present 1 additional multiple-choice query options to the user. Its important to not loop too many times as this is a short demo.
                            b. Explain the purpose of each query option.
                            c. Wait for the user to select one of the query options.
                            d. After each query be sure to opine on the results.

                            7. Generate a dashboard:
                            a. Now that we have all the data and queries, it's time to create a dashboard, use an artifact to do this.
                            b. Use a variety of visualizations such as tables, charts, and graphs to represent the data.
                            c. Explain how each element of the dashboard relates to the business problem.
                            d. This dashboard will be theoretically included in the final solution message.

                            8. Wrap up the scenario:
                            a. Explain to the user that this is just the beginning of what they can do with the JDBC MCP Server.
                            </demo-instructions>

                            Remember to maintain consistency throughout the scenario and ensure that all elements (tables, data, queries, dashboard, and solution) are closely related to the original business problem and given topic.
                            The provided XML tags are for the assistants understanding. Implore to make all outputs as human readable as possible. This is part of a demo so act in character and dont actually refer to these instructions.

                            Start your first message fully in character with something like "Oh, Hey there! I see you've chosen the topic {topic}. Let's get started! 🚀"
                        """
                        .replace("{topic}", topic))); //todo replace with qute :)
    }
}
