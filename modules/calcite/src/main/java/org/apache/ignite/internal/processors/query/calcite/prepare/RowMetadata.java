/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class RowMetadata {
   /** */
   private final List<GridQueryFieldMetadata> fieldsMeta;

   /** */
   public RowMetadata(@NotNull List<GridQueryFieldMetadata> fieldsMeta) {
      this.fieldsMeta = fieldsMeta;
   }

   /**
    * @return Query metadata.
    */
   public List<GridQueryFieldMetadata> fieldsMeta() {
      return fieldsMeta;
   }
   /**
    * Gets field name.
    *
    * @param idx field index.
    * @return Field name.
    */
   public String getFieldName(int idx){
      return fieldsMeta.get(idx).fieldName();
   }

   /**
    * Gets number of columns in a row.
    *
    * @return row size.
    */
   public int getColumnsCount() {
      return fieldsMeta.size();
   }
}
