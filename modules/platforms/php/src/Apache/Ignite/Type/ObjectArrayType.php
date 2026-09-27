<?php
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

namespace Apache\Ignite\Type;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\BinaryUtils;

/**
 * Class representing an array type of Ignite objects.
 *
 * It is described by ObjectType::OBJECT_ARRAY.
 */
class ObjectArrayType extends ObjectType
{
    private $elementType;
    
    /**
     * Public constructor.
     *
     * Optionally specifies Ignite type of elements in the array.
     *
     * If Ignite type of elements is not specified then during operations the Ignite client
     * tries to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param int|ObjectType|null $elementType Ignite type of the array element:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * 
     * @throws ClientException if error.
     */
    public function __construct($elementType = null)
    {
        parent::__construct(ObjectType::OBJECT_ARRAY);
        BinaryUtils::checkObjectType($elementType, 'elementType');
        $this->elementType = $elementType;
    }
    
    /**
     * Returns Ignite type of the array element.
     * 
     * @return int|ObjectType|null Ignite type of the array element:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getElementType()
    {
        return $this->elementType;
    }
}
