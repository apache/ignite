/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Copyright (C) 2018 Hawkore (http://hawkore.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.builder.index;


import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.builder.JSONBuilder;
import org.hawkore.ignite.lucene.builder.index.schema.Schema;
import org.hawkore.ignite.lucene.builder.index.schema.analysis.Analyzer;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.Mapper;

/**
 * A Lucene index definition.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 * 
 * @author Manuel Núñez {@literal <manuel.nunez@hawkore.com>}
 */
public class Index extends JSONBuilder {

    /** lucene index suffix */
    public static final String LUCENE_INDEX_NAME_SUFIX = "_lucene_idx";
    
    /** lucene field name. */
    public static final String LUCENE_FIELD_NAME = "LUCENE";

    private String name;
    private String column;
    
    public Schema schema;
    public String keyspace;
    public String table;

    public Number refreshSeconds;
    public String directoryPath;
    public Integer ramBufferMb;
    public Integer maxCachedMb;
    public Partitioner partitioner;
    public Boolean optimizerEnabled;
    public String optimizerSchedule;
    public Integer version;
    
    /** Parallelism level for index population. Only used on DDL statement */
    public Integer parallel;

    
    /**
     * Builds a new {@link Index} creation statement for the specified table and
     * column.
     *
     * @param table
     *            the table name
     * @param name
     *            the index name
     */
    public Index(String table) {
        this.schema = new Schema();
        this.table = table;
        // index name must be TABLE_LUCENE_IDX
        this.name = (table + LUCENE_INDEX_NAME_SUFIX).toUpperCase();
        // column name must be LUCENE
        this.column = LUCENE_FIELD_NAME;
    }

    /**
     * Sets the name of the keyspace.
     *
     * @param keyspace
     *            the keyspace name
     * @return this with the specified keyspace name
     */
    public Index keyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Sets the index searcher refresh period.
     *
     * @param refreshSeconds
     *            the number of seconds between refreshes
     * @return this with the specified refresh seconds
     */
    public Index refreshSeconds(Number refreshSeconds) {
        this.refreshSeconds = refreshSeconds;
        return this;
    }

    /**
     * Sets the path of the Lucene directory files.
     *
     * @param directoryPath
     *            the path of the Lucene directory files.
     * @return this with the specified directory path
     */
    public Index directoryPath(String directoryPath) {
        this.directoryPath = directoryPath;
        return this;
    }

    /**
     * Sets the Lucene's RAM buffer size in MBs.
     *
     * @param ramBufferMb
     *            the RAM buffer size
     * @return this with the specified RAM buffer size
     */
    public Index ramBufferMb(Integer ramBufferMb) {
        this.ramBufferMb = ramBufferMb;
        return this;
    }

    /**
     * Sets the Lucene's max cached MBs.
     *
     * @param maxCachedMb
     *            the Lucene's max cached MBs
     * @return this with the specified max cached MBs
     */
    public Index maxCachedMb(Integer maxCachedMb) {
        this.maxCachedMb = maxCachedMb;
        return this;
    }

    /**
     * Sets the name of the default {@link Analyzer}.
     *
     * @param name
     *            the name of the default {@link Analyzer}
     * @return this with the specified default analyzer
     */
    public Index defaultAnalyzer(String name) {
        schema.defaultAnalyzer(name);
        return this;
    }

    /**
     * Adds a new {@link Analyzer}.
     *
     * @param name
     *            the name of the {@link Analyzer} to be added
     * @param analyzer
     *            the {@link Analyzer} to be added
     * @return this with the specified analyzer
     */
    public Index analyzer(String name, Analyzer analyzer) {
        schema.analyzer(name, analyzer);
        return this;
    }

    /**
     * Adds a new {@link Mapper}.
     *
     * @param field
     *            the name of the {@link Mapper} to be added
     * @param mapper
     *            the {@link Mapper} to be added
     * @return this with the specified mapper
     */
    public Index mapper(String field, Mapper<?> mapper) {
        schema.mapper(field, mapper);
        return this;
    }

    /**
     * Sets the {@link Schema}.
     *
     * @param schema
     *            the {@link Schema}
     * @return this with the specified schema
     */
    public Index schema(Schema schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Sets the {@link Partitioner}.
     *
     * Index partitioning is useful to speed up some queries to the detriment of
     * others, depending on the implementation. It is also useful to overcome
     * the Lucene's hard limit of 2147483519 documents per index.
     *
     * @param partitioner
     *            the {@link Partitioner}
     * @return this with the specified partitioner
     */
    public Index partitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    /**
     * Whether Lucene index automatic optimization is enabled
     * 
     * This value could be changed at runtime.
     * 
     * @param optimizerEnabled whether optimizer must be enabled
     * @return this with the specified optimizerEnabled flag set
     */
    public Index optimizerEnabled(Boolean optimizerEnabled) {
        this.optimizerEnabled = optimizerEnabled;
        return this;
    }

    /**
     * Optimizer's schedule CRON expression.
     * 
     * <p>
     * 
     * By default optimizer will run every day at 1:00 AM (0 1 * * *)
     * 
     * @See <a href="https://apacheignite.readme.io/docs/cron-based-scheduling">Cron-Based Scheduling</a>
     * 
     * This value could be changed at runtime.
     *
     * @param optimizerSchedule CRON expression
     * @return this with the specified optimizer schedule CRON expression
     */
    public Index optimizerSchedule(String optimizerSchedule) {
        this.optimizerSchedule = optimizerSchedule;
        return this;
    }
    
    /**
     * Index options version. 
     * <p>
     * Is a sequential number that avoid update index options
     * when using dynamic sql entities functionality. Update index options will
     * be allowed if new <code>version</code> value is equals or greater than current
     * index option's <code>version</code> value. Default 0
     * 
     * @param version index configuration version
     * @return this with the specified version
     */
    public Index version(Integer version) {
        this.version = version;
        return this;
    }
    
    /**
     *  Parallelism level for index population. Only used on DDL statement 
     *  
     * @param parallel Parallelism level for index population
     * @return this with the specified version
     */
    public Index parallel(Integer parallel) {
        if (parallel < 0)
            throw new IndexException("Illegal PARALLEL value. Should be positive: " + parallel);
        this.parallel = parallel;
        return this;
    }
    
    /**
     * Index DDL creation statement
     * 
     * @return return CREATE INDEX statement
     * 
     */
    public String buildDDL() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ");
        sb.append(name).append(" ");
        String fullTable = StringUtils.isBlank(keyspace) ? table : '"' + keyspace + '"' + "." + table;
        sb.append(String.format("ON %s(%s) ", fullTable, column));
        if( this.parallel != null ){
            sb.append(" PARALLEL ");
            sb.append(this.parallel); 
        }
        sb.append(" FULLTEXT ");
        sb.append("'").append(internalBuild(true)).append("'");
        return sb.toString();
    }

    
    /**
     * Builds JSON definition for index
     * 
     * @return index JSON definition
     */
    @Override
    public String build() {
        return internalBuild(false);
    }

    
    private String internalBuild(boolean escapeSql) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        option(sb, "version", version, escapeSql);  
        option(sb, "refresh_seconds", refreshSeconds, escapeSql);
        option(sb, "directory_path", directoryPath, escapeSql);
        option(sb, "ram_buffer_mb", ramBufferMb, escapeSql);
        option(sb, "max_cached_mb", maxCachedMb, escapeSql);
        option(sb, "partitioner", partitioner, escapeSql);
        option(sb, "optimizer_enabled", optimizerEnabled, escapeSql);
        option(sb, "optimizer_schedule", optimizerSchedule, escapeSql);      
        sb.append(String.format((escapeSql ? "''schema'':''%s''}":"'schema':'%s'}"), schema));
        return sb.toString();
    }
    
    
    private void option(StringBuilder sb, String name, Object value, boolean escapeSql) {
        String format = escapeSql ? "''%s'':''%s'',": "'%s':'%s',";
        if (value != null) {
            sb.append(String.format(format, name, value));
        }
    }

}
