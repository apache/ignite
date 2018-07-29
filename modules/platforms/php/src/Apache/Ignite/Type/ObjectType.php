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

/** 
 * Base class representing a type of Ignite object.
 * 
 * The class has no public constructor. Only subclasses may be instantiated.
 *
 * There are two groups of Ignite object types:
 *
 * - Primitive (simple) types. To fully describe such a type it is enough to specify
 * Ignite type code @ref PrimitiveTypeCodes only.
 *
 * - Non-primitive (composite) types. To fully describe such a type
 * Ignite type code @ref CompositeTypeCodes with additional information should be specified.
 * Eg. a kind of map or a kind of collection.
 *
 * This class helps the Ignite client to make a mapping between PHP types
 * and types used by Ignite.
 *
 * In many methods the Ignite client does not require to directly specify a type of Ignite object.
 * In this case the Ignite client tries to make automatic mapping between PHP types
 * and Ignite object types according to the following mapping tables:
 *
 * ----------------------------------------------------------------------------
 *
 * DEFAULT MAPPING FROM PHP type TO Ignite type code.
 *
 * This mapping is used when an application does not explicitly specify an Ignite type
 * for a field and is writing data to that field.
 *
 * <pre>
 * | PHP type                  | Ignite type code      |
 * | ------------------------- | ----------------------|
 * | boolean                   | BOOLEAN               |
 * | integer                   | INTEGER               |
 * | float                     | DOUBLE                |
 * | string                    | STRING                |
 * | Date                      | DATE                  |
 * | Time                      | TIME                  |
 * | Timestamp                 | TIMESTAMP             |
 * | array of boolean          | BOOLEAN_ARRAY / MAP   |
 * | array of integer          | INTEGER_ARRAY / MAP   |
 * | array of float            | DOUBLE_ARRAY / MAP    |
 * | array of string           | STRING_ARRAY / MAP    |
 * | array of Date             | DATE_ARRAY / MAP      |
 * | array of Time             | TIME_ARRAY / MAP      |
 * | array of Timestamp        | TIMESTAMP_ARRAY / MAP |
 * | Ds/Set                    | COLLECTION (HASH_SET) |
 * | Ds/Map                    | MAP (HASH_MAP)        |
 * </pre>
 *
 * Type of an array content is determined by the type of the first element of the array.
 * Empty array has no default mapping.
 *
 * All other PHP types have no default mapping.
 *
 * ----------------------------------------------------------------------------
 *
 * DEFAULT MAPPING FROM Ignite type code TO PHP type.
 *
 * This mapping is used when an application does not explicitly specify an Ignite type
 * for a field and is reading data from that field.
 *
 * <pre>
 * | Ignite type code             | PHP type                              |
 * | ---------------------------- | --------------------------------------|
 * | BYTE                         | integer                               |
 * | SHORT                        | integer                               |
 * | INTEGER                      | integer                               |
 * | LONG                         | integer                               |
 * | FLOAT                        | float                                 |
 * | DOUBLE                       | float                                 |
 * | BOOLEAN                      | boolean                               |
 * | STRING                       | string                                |
 * | CHAR                         | string (one character)                |
 * | DATE                         | Date                                  |
 * | TIME                         | Time                                  |
 * | TIMESTAMP                    | Timestamp                             |
 * | BYTE_ARRAY                   | array of integer                      |
 * | SHORT_ARRAY                  | array of integer                      |
 * | INTEGER_ARRAY                | array of integer                      |
 * | LONG_ARRAY                   | array of integer                      |
 * | FLOAT_ARRAY                  | array of float                        |
 * | DOUBLE_ARRAY                 | array of float                        |
 * | BOOLEAN_ARRAY                | array of boolean                      |
 * | STRING_ARRAY                 | array of string                       |
 * | CHAR_ARRAY                   | array of string (one character)       |
 * | DATE_ARRAY                   | array of Date                         |
 * | TIME_ARRAY                   | array of Time                         |
 * | TIMESTAMP_ARRAY              | array of Timestamp                    |
 * | COLLECTION (USER_COL)        | array                                 |
 * | COLLECTION (ARR_LIST)        | array                                 |
 * | COLLECTION (LINKED_LIST)     | array                                 |
 * | COLLECTION (SINGLETON_LIST)  | array                                 |
 * | COLLECTION (HASH_SET)        | Ds/Set                                |
 * | COLLECTION (LINKED_HASH_SET) | Ds/Set                                |
 * | COLLECTION (USER_SET)        | Ds/Set                                |
 * | MAP (HASH_MAP)               | Ds/Map                                |
 * | MAP (LINKED_HASH_MAP)        | Ds/Map                                |
 * | NULL                         | null                                  |
 * </pre>
 *
 * ----------------------------------------------------------------------------
 *
 * COMMENTS TO ALL TABLES
 *
 * PHP type - is a PHP primitive or a PHP object
 * (http://php.net/manual/en/language.types.intro.php)
 *
 * Ignite type code - is the type code of an Ignite primitive type (@ref PrimitiveTypeCodes)
 * or an Ignite composite type (@ref CompositeTypeCodes).
 *
 * ----------------------------------------------------------------------------
 * 
 */
abstract class ObjectType
{
    /** @name PrimitiveTypeCodes
     *  @anchor PrimitiveTypeCodes
     *  @{
     */
    
    /**
     * Single byte value. Can also represent small signed integer value.
     */
    const BYTE = 1;
    
    /**
     * 2-bytes long signed integer number.
     */
    const SHORT = 2;
    
    /**
     * 4-bytes long signed integer number.
     */
    const INTEGER = 3;
    
    /**
     * 8-bytes long signed integer number.
     */
    const LONG = 4;
    
    /**
     * 4-byte long floating-point number.
     */
    const FLOAT = 5;

    /**
     * 8-byte long floating-point number.
     */
    const DOUBLE = 6;
    
    /**
     * Single UTF-16 code unit.
     */
    const CHAR = 7;
    
    /**
     * Boolean value.
     */
    const BOOLEAN = 8;
    
    /**
     * String in UTF-8 encoding.
     */
    const STRING = 9;
    
    /**
     * A universally unique identifier (UUID) is a 128-bit number used to identify information in computer systems.
     */
    const UUID = 10;
    
    /**
     * Date, represented as a number of milliseconds elapsed since 00:00:00 1 Jan 1970 UTC.
     */
    const DATE = 11;
    
    /**
     * Array of bytes.
     */
    const BYTE_ARRAY = 12;
    
    /**
     * Array of short signed integer numbers.
     */
    const SHORT_ARRAY = 13;
    
    /**
     * Array of signed integer numbers.
     */
    const INTEGER_ARRAY = 14;
    
    /**
     * Array of long signed integer numbers.
     */
    const LONG_ARRAY = 15;
    
    /**
     * Array of floating point numbers.
     */
    const FLOAT_ARRAY = 16;
    
    /**
     * Array of floating point numbers with double precision.
     */
    const DOUBLE_ARRAY = 17;
    
    /**
     * Array of UTF-16 code units.
     */
    const CHAR_ARRAY = 18;
    
    /**
     * Array of boolean values.
     */
    const BOOLEAN_ARRAY = 19;
    
    /**
     * Array of UTF-8 string values.
     */
    const STRING_ARRAY = 20;
    
    /**
     * Array of UUIDs.
     */
    const UUID_ARRAY = 21;
    
    /**
     * Array of dates.
     */
    const DATE_ARRAY = 22;
    
    /**
     * Value of an enumerable type. For such types defined only a finite number of named values.
     */
    const ENUM = 28;
    
    /**
     * Array of enumerable type value.
     */
    const ENUM_ARRAY = 29;
    
    /**
     * Numeric value of any desired precision and scale.
     */
    const DECIMAL = 30;
    
    /**
     * Array of decimal values.
     */
    const DECIMAL_ARRAY = 31;
    
    /**
     * More precise than a Date data type. Except for a milliseconds since epoch, contains a nanoseconds
     * fraction of a last millisecond, which value could be in a range from 0 to 999999. 
     */
    const TIMESTAMP = 33;
    
    /**
     * Array of timestamp values.
     */
    const TIMESTAMP_ARRAY = 34;
    
    /**
     * Time, represented as a number of milliseconds elapsed since midnight, i.e. 00:00:00 UTC.
     */
    const TIME = 36;
    
    /**
     * Array of time values.
     */
    const TIME_ARRAY = 37;
    /** @} */ // end of PrimitiveTypeCodes

    /** @name CompositeTypeCodes
     *  @anchor CompositeTypeCodes
     *  @{
     */
    /**
     * Array of objects of any type.
     */
    const OBJECT_ARRAY = 23;
    
    /**
     * General collection type.
     */
    const COLLECTION = 24;
    
    /**
     * Map-like collection type. Contains pairs of key and value objects.
     */
    const MAP = 25;
    
    /**
     * Wrapped binary object type.
     */
    const BINARY_OBJECT = 27;
    
    /**
     * Wrapped enumerable type.
     */
    const BINARY_ENUM = 38;
    
    /**
     * null value.
     */
    const NULL = 101;

    /**
     * Complex object.
     */
    const COMPLEX_OBJECT = 103;
    /** @} */ // end of CompositeTypeCodes
    
    private $typeCode;
    
    /**
     * 
     * @param int $typeCode
     */
    protected function __construct(int $typeCode)
    {
        $this->typeCode = $typeCode;
    }
    
    /**
     * 
     * @return int
     */
    public function getTypeCode(): int
    {
        return $this->typeCode;
    }
}
