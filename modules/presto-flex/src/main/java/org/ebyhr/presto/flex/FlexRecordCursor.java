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
package org.ebyhr.presto.flex;

import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import com.google.common.io.Resources;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import org.ebyhr.presto.flex.operator.FilePlugin;
import org.ebyhr.presto.flex.operator.PluginFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class FlexRecordCursor
        implements RecordCursor
{
    private final List<FlexColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator lines;
    private final long totalBytes;
    private final FilePlugin plugin;

    private List<String> fields;

    public FlexRecordCursor(List<FlexColumnHandle> columnHandles, SchemaTableName schemaTableName)
    {
        this.columnHandles = columnHandles;
        this.plugin = PluginFactory.create(schemaTableName.getSchemaName());

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            FlexColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }


        URI uri = URI.create(schemaTableName.getTableName());
        ByteSource byteSource;
        try {
            byteSource = Resources.asByteSource(uri.toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e.getMessage());
        }


        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            lines = plugin.getIterator(byteSource);
            if (plugin.skipFirstLine()) {
                lines.next();
            }
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        fields = plugin.splitToList(lines);
        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
