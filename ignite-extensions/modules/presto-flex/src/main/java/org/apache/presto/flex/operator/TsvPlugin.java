/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.presto.flex.operator;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.presto.flex.FileType.TXT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.presto.flex.FlexColumn;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import au.com.bytecode.opencsv.CSVParser;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;

public class TsvPlugin implements FilePlugin {
    private static final String DELIMITER = "\t";
    
    private CSVParser parser = new CSVParser(DELIMITER.charAt(0));
    private Splitter splitter = Splitter.on(DELIMITER).trimResults(CharMatcher.whitespace().or(CharMatcher.is('"')));

    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
    	
        ByteSource byteSource;
        try {
            byteSource = Resources.asByteSource(URI.create(table).toURL());
        }
        catch (IllegalArgumentException | MalformedURLException e) {
            throw new TableNotFoundException(new SchemaTableName(schema, table));
        }

        if (schema.equalsIgnoreCase(TXT.toString())) {
            return ImmutableList.of(new FlexColumn("value", VARCHAR));
        }

        List<FlexColumn> columnTypes = new LinkedList<>();
        try {
        	String line = byteSource.asCharSource(UTF_8).readFirstLine();
            List<String> fields = splitter.splitToList(line);          
            fields.forEach(field -> columnTypes.add(new FlexColumn(field, VARCHAR)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return columnTypes;

    }

    @Override
    public Iterator<String> getIterator(ByteSource byteSource,URI uri)
    {
        try {
            return byteSource.asCharSource(UTF_8).readLines().iterator();
        } catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get iterator");
        }
    }

    @Override
    public List<Object> splitToList(Iterator lines)
    {
    	Object line =  lines.next();
        //Splitter splitter = Splitter.on(DELIMITER).trimResults();
        //List<String> list = splitter.splitToList(line.toString());
        //return (List)list;        
        try {
			String[] row = parser.parseLineMulti(line.toString());
			return Arrays.asList(row);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to parse line "+ e.getMessage());
		}
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
