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
 * Class representing a complex type of GridGain object.
 *
 * It corresponds to the ObjectType::COMPLEX_OBJECT GridGain type code.
 *
 * This class may be needed to help GridGain client to:
 *   - deserialize a GridGain complex object to a PHP object when reading data,
 *   - serialize a PHP object to a GridGain complex object when writing data.
 *
 * Note: only public properties of PHP objects can be serialized/deserialized.
 */
class ComplexObjectType extends ObjectType
{
    private $phpClassName;
    private $typeName;
    private $fieldTypes;

    /**
     * Public constructor.
     *
     * Creates a default representation of GridGain complex object type.
     * setPhpClassName(), setIgniteTypeName(), setFieldType() methods may be used
     * to change the default representation.
     */
    public function __construct()
    {
        parent::__construct(ObjectType::COMPLEX_OBJECT);
        $this->phpClassName = null;
        $this->typeName = null;
        $this->fieldTypes = [];
    }

    /**
     * Sets the name of the PHP class.
     *
     * Affects data reading operations only.
     *
     * The specified name will be used as PHP class name to instantiate a PHP object
     * the received GridGain complex object is deserialized to.
     *
     * By default (if the name is not specified), the GridGain complex type name of the received complex object
     * is used as PHP class name to instantiate a PHP object during deserialization.
     *
     * The PHP Class must have a constructor without parameters or with optional parameters only.
     *
     * @param string|null $phpClassName name of the PHP class or null (the name is not specified).
     *
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     */
    public function setPhpClassName(?string $phpClassName): ComplexObjectType
    {
        $this->phpClassName = $phpClassName;
        return $this;
    }

    /**
     * Gets the name of the PHP class.
     *
     * @return string|null name of the PHP class or null (the name is not specified).
     */
    public function getPhpClassName(): ?string
    {
        return $this->phpClassName;
    }

    /**
     * Sets the name of the GridGain complex type.
     *
     * Affects data writing operations only.
     *
     * The specified name will be used as the GridGain complex type name during the complex object's writing operations.
     *
     * By default (if the name is not specified), the GridGain complex type name of the serialized complex object
     * is taken from the name of the PHP class which instance is provided in a data writing operation.
     *
     * @param string|null $typeName name of the GridGain complex type or null (the name is not specified).
     *
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     */
    public function setIgniteTypeName(?string $typeName): ComplexObjectType
    {
        $this->typeName = $typeName;
        return $this;
    }

    /**
     * Gets the name of the GridGain complex type.
     *
     * @return string|null name of the GridGain complex type or null (the name is not specified).
     */
    public function getIgniteTypeName(): ?string
    {
        return $this->typeName;
    }

    /**
     * Specifies GridGain type of the indicated field in the complex type.
     *
     * Affects data writing operations only.
     *
     * During data serialization GridGain client will assume that the indicated field
     * has the GridGain type specified by this method.
     *
     * By default (if the type of the field is not specified),
     * GridGain client tries to make automatic mapping between PHP types and GridGain object types -
     * according to the mapping table defined in the description of the ObjectType class.
     *
     * @param string $fieldName name of the field.
     * @param int|ObjectType|null $fieldType GridGain type of the field:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     *
     * @throws ClientException if error.
     */
    public function setFieldType(string $fieldName, $fieldType): ComplexObjectType
    {
        BinaryUtils::checkObjectType($fieldType, 'fieldType');
        $this->fieldTypes[$fieldName] = $fieldType;
        return $this;
    }

    /**
     * Gets GridGain type of the indicated field in the complex type.
     *
     * @param string $fieldName name of the field.
     *
     * @return int|ObjectType|null GridGain type of the field:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getFieldType(string $fieldName)
    {
        if (array_key_exists($fieldName, $this->fieldTypes)) {
            return $this->fieldTypes[$fieldName];
        }
        return null;
    }
}
