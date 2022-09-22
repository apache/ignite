package de.kp.works.ignite.gremlin.exception;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.gremlin.IgniteElement;
import org.apache.tinkerpop.gremlin.structure.Element;

public class IgniteGraphNotFoundException extends IgniteGraphException {

    private static final long serialVersionUID = 562966954442798338L;

    private final IgniteElement element;

    public IgniteGraphNotFoundException(Element element, String reason) {
        super(reason);
        this.element = (IgniteElement) element;
    }

    public IgniteElement getElement() {
        return element;
    }
}
