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

import static io.prestosql.spi.type.VarcharType.VARCHAR;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.WeakHashMap;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.presto.flex.FlexColumn;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

public class ExcelPlugin implements FilePlugin {

    private static final DataFormatter DATA_FORMATTER = new DataFormatter();
    
    private static WeakHashMap<URI,Workbook> cache = new WeakHashMap<>();

    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        try {
            URI uri = URI.create(table);
            Workbook workbook = cache.get(uri);
            if(workbook==null) {
            	ByteSource byteSource = Resources.asByteSource(uri.toURL());
            	workbook = WorkbookFactory.create(byteSource.openStream());
            	cache.put(uri, workbook);
            }
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rows = sheet.iterator();
            List<FlexColumn> columnTypes = new LinkedList<>();
            Row row = rows.next();
            for(Cell cell: row) {
                String cellValue = DATA_FORMATTER.formatCellValue(cell);
                columnTypes.add(new FlexColumn(cellValue, VARCHAR));
            }
            return columnTypes;
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to operate %s file,because %s", table,e.getMessage()));
        }
    }

    @Override
    public Iterator getIterator(ByteSource byteSource,URI uri)
    {
        try {
        	Workbook workbook = cache.get(uri);
            if(workbook==null) {
            	workbook = WorkbookFactory.create(byteSource.openStream());
            	cache.put(uri, workbook);
            }
            
            Sheet sheet = workbook.getSheetAt(0);
            return sheet.iterator();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to operate s file");
        }
    }

    @Override
    public List<Object> splitToList(Iterator lines)
    {
        List<Object> values = new ArrayList<>();
        Row row = (Row) lines.next();
        for(Cell cell: row) {
            String cellValue = DATA_FORMATTER.formatCellValue(cell);
            values.add(cellValue);
        }
        return values;
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
