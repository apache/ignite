package de.kp.works.ignite.mutate;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.IgniteColumn;
import de.kp.works.ignite.graph.ElementType;

public class IgnitePut extends IgniteMutation {

    public IgnitePut(Object id, ElementType elementType) {
        super(id);
        this.elementType = elementType;
        mutationType = IgniteMutationType.PUT;
    }

    public void addColumn(String colName, String colType, Object colValue) {
        columns.add(new IgniteColumn(colName, colType, colValue));
    }

}
