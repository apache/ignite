<?php
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

namespace Apache\Ignite\Type;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\BinaryUtils;

/**
 * Class representing an array type of GridGain objects.
 *
 * It is described by ObjectType::OBJECT_ARRAY.
 */
class ObjectArrayType extends ObjectType
{
    private $elementType;

    /**
     * Public constructor.
     *
     * Optionally specifies GridGain type of elements in the array.
     *
     * If GridGain type of elements is not specified then during operations the GridGain client
     * tries to make automatic mapping between PHP types and GridGain object types -
     * according to the mapping table defined in the description of the ObjectType class.
     *
     * @param int|ObjectType|null $elementType GridGain type of the array element:
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
     * Returns GridGain type of the array element.
     *
     * @return int|ObjectType|null GridGain type of the array element:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getElementType()
    {
        return $this->elementType;
    }
}
