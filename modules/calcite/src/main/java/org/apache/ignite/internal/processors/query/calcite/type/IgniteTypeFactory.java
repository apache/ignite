/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.type;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Ignite type factory.
 */
public class IgniteTypeFactory extends JavaTypeFactoryImpl {
    /** */
    public IgniteTypeFactory() {
        super(IgniteTypeSystem.INSTANCE);
    }

    /**
     * @param typeSystem Type system.
     */
    public IgniteTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    /** {@inheritDoc} */
    @Override public RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        if (types.size() == 1 || allEquals(types))
            return F.first(types);

        return super.leastRestrictive(types);
    }

    /** */
    private boolean allEquals(List<RelDataType> types) {
        assert types.size() > 1;

        RelDataType first = F.first(types);
        for (int i = 1; i < types.size(); i++) {
            if (!Objects.equals(first, types.get(i)))
                return false;
        }

        return true;
    }
}
