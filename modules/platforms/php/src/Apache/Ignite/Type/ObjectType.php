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
 * The class is abstract and has no public constructor. Only subclasses may be instantiated.
 *
 * There are two groups of Ignite object types:
 *
 * - Primitive (simple) types. To fully describe such a type:
 * it is enough to specify Ignite type code @ref PrimitiveTypeCodes only.
 *
 * - Non-primitive (composite) types. To fully describe such a type:
 * Ignite type code @ref CompositeTypeCodes with additional information
 * (eg. a kind of map or a kind of collection) should be specified.
 *
 * This class helps the Ignite client to make a mapping between PHP types
 * and types used by Ignite according to the following mapping tables:
 *
 * ----------------------------------------------------------------------------
 *
 * COMMENTS TO ALL TABLES
 * ----------------
 *
 * PHP type
 * ----------------
 * It is a PHP primitive or a PHP object: http://php.net/manual/en/language.types.intro.php
 *
 * Associative and indexed PHP arrays: http://php.net/manual/en/language.types.array.php
 *
 * Additional types:
 *   - Ds\\Set, Ds\\Map - PHP Data Structures extension classes: http://php.net/manual/en/book.ds.php
 *   - Brick\\Math\\BigDecimal - PHP class from the extension library
 *                               to work with arbitrary precision numbers: https://github.com/brick/math
 *   - Date, Time, Timestamp, EnumItem, BinaryObject - PHP classes introduced by the Ignite client.
 *
 * Ignite type code
 * ----------------
 * It is a type code of Ignite primitive type (@ref PrimitiveTypeCodes)
 * or Ignite composite type (@ref CompositeTypeCodes).
 *
 * ----------------------------------------------------------------------------
 *
 * MAPPING FROM IGNITE TYPE CODE TO PHP TYPE WHEN READING DATA
 * ----------------
 *
 * | Ignite type code             | PHP type                                      |
 * | ---------------------------- | ----------------------------------------------|
 * | BYTE                         | integer                                       |
 * | SHORT                        | integer                                       |
 * | INTEGER                      | integer                                       |
 * | LONG                         | float                                         |
 * | FLOAT                        | float                                         |
 * | DOUBLE                       | float                                         |
 * | DECIMAL                      | Brick\\Math\\BigDecimal                       |
 * | BOOLEAN                      | boolean                                       |
 * | STRING                       | string                                        |
 * | CHAR                         | string (one character)                        |
 * | UUID                         | indexed array of integers (16 items)          |
 * | DATE                         | Date                                          |
 * | TIME                         | Time                                          |
 * | TIMESTAMP                    | Timestamp                                     |
 * | ENUM                         | EnumItem                                      |
 * | COMPLEX_OBJECT               | BinaryObject or PHP object*                   |
 * | BYTE_ARRAY                   | indexed array of integer                      |
 * | SHORT_ARRAY                  | indexed array of integer                      |
 * | INTEGER_ARRAY                | indexed array of integer                      |
 * | LONG_ARRAY                   | indexed array of float                        |
 * | FLOAT_ARRAY                  | indexed array of float                        |
 * | DOUBLE_ARRAY                 | indexed array of float                        |
 * | DECIMAL_ARRAY                | indexed array of Brick\\Math\\BigDecimal      |
 * | BOOLEAN_ARRAY                | indexed array of boolean                      |
 * | STRING_ARRAY                 | indexed array of string                       |
 * | CHAR_ARRAY                   | indexed array of string (one character)       |
 * | UUID_ARRAY                   | indexed array of array of integers (16 items) |
 * | DATE_ARRAY                   | indexed array of Date                         |
 * | TIME_ARRAY                   | indexed array of Time                         |
 * | TIMESTAMP_ARRAY              | indexed array of Timestamp                    |
 * | ENUM_ARRAY                   | indexed array of EnumItem                     |
 * | OBJECT_ARRAY                 | indexed array                                 |
 * | COLLECTION (USER_COL)        | indexed array                                 |
 * | COLLECTION (ARR_LIST)        | indexed array                                 |
 * | COLLECTION (LINKED_LIST)     | indexed array                                 |
 * | COLLECTION (SINGLETON_LIST)  | indexed array                                 |
 * | COLLECTION (HASH_SET)        | Ds\\Set                                       |
 * | COLLECTION (LINKED_HASH_SET) | Ds\\Set                                       |
 * | COLLECTION (USER_SET)        | Ds\\Set                                       |
 * | MAP (HASH_MAP)               | Ds\\Map                                       |
 * | MAP (LINKED_HASH_MAP)        | Ds\\Map                                       |
 * | NULL                         | null**                                        |
 *
 * (*) If an application does not explicitly specify an Ignite type for a field and
 * COMPLEX_OBJECT is received, the Ignite client returns BinaryObject to the application.
 * If an application explicitly specifies ComplexObjectType for a field,
 * the Ignite client deserializes the received COMPLEX_OBJECT into PHP object
 * specified by the ComplexObjectType.
 *
 * (**) NULL cannot be specified as an Ignite type of a field but PHP null may be returned
 * as a value of a field.
 *
 * ----------------------------------------------------------------------------
 *
 * DEFAULT MAPPING FROM PHP TYPE TO IGNITE TYPE CODE WHEN WRITING DATA
 * ----------------
 *
 * This mapping is used when an application does not explicitly specify an Ignite type
 * for a field and it is writing data to that field.
 *
 * | PHP type                                    | Ignite type code      |
 * | ------------------------------------------- | ----------------------|
 * | boolean                                     | BOOLEAN               |
 * | integer                                     | INTEGER               |
 * | float                                       | DOUBLE                |
 * | string                                      | STRING                |
 * | Date                                        | DATE                  |
 * | Time                                        | TIME                  |
 * | Timestamp                                   | TIMESTAMP             |
 * | EnumItem                                    | ENUM                  |
 * | Brick\\Math\\BigDecimal                     | DECIMAL               |
 * | BinaryObject                                | COMPLEX_OBJECT        |
 * | any other PHP Object*                       | COMPLEX_OBJECT        |
 * | associative array of PHP supported type**   | MAP (HASH_MAP)        |
 * | indexed array of boolean***                 | BOOLEAN_ARRAY         |
 * | indexed array of integer                    | INTEGER_ARRAY         |
 * | indexed array of float                      | DOUBLE_ARRAY          |
 * | indexed array of string                     | STRING_ARRAY          |
 * | indexed array of Date                       | DATE_ARRAY            |
 * | indexed array of Time                       | TIME_ARRAY            |
 * | indexed array of Timestamp                  | TIMESTAMP_ARRAY       |
 * | indexed array of EnumItem                   | ENUM_ARRAY            |
 * | indexed array of Brick\\Math\\BigDecimal    | DECIMAL_ARRAY         |
 * | indexed array of BinaryObject               | OBJECT_ARRAY          |
 * | indexed array of any other PHP Object*      | OBJECT_ARRAY          |
 * | Ds\\Set                                     | COLLECTION (HASH_SET) |
 * | Ds\\Map                                     | MAP (HASH_MAP)        |
 *
 * All other PHP types have no default mapping to Ignite type codes.
 *
 * (*) Any other PHP Object - is any PHP class, not explicitly mentioned in the table.
 *
 * (**) PHP supported type - is any PHP type mentioned in the table.
 * 
 * (***) Type of an indexed array's value is determined by the value's type in the first element of the array.
 * Empty array has no default mapping.
 *
 * ----------------------------------------------------------------------------
 *
 * ALLOWED PHP TYPES WHEN WRITING DATA OF THE SPECIFIED IGNITE TYPE CODE
 * ----------------
 *
 * When an application explicitly specifies an Ignite type for a field
 * and it is writing data to that field, the following PHP types
 * are allowed for every concrete Ignite type code.
 *
 * | Specified Ignite type code   | Allowed PHP types                             |
 * | ---------------------------- | ----------------------------------------------|
 * | BYTE                         | integer                                       |
 * | SHORT                        | integer                                       |
 * | INTEGER                      | integer                                       |
 * | LONG                         | float, integer                                |
 * | FLOAT                        | float, integer                                |
 * | DOUBLE                       | float, integer                                |
 * | DECIMAL                      | Brick\\Math\\BigDecimal                       |
 * | BOOLEAN                      | boolean                                       |
 * | STRING                       | string                                        |
 * | CHAR                         | string (one character)                        |
 * | UUID                         | indexed array of integers (16 items)          |
 * | DATE                         | Date                                          |
 * | TIME                         | Time                                          |
 * | TIMESTAMP                    | Timestamp                                     |
 * | ENUM                         | EnumItem                                      |
 * | COMPLEX_OBJECT               | BinaryObject, any PHP object                  |
 * | BYTE_ARRAY*                  | indexed array of integer                      |
 * | SHORT_ARRAY                  | indexed array of integer                      |
 * | INTEGER_ARRAY                | indexed array of integer                      |
 * | LONG_ARRAY                   | indexed array of float/integer                |
 * | FLOAT_ARRAY                  | indexed array of float/integer                |
 * | DOUBLE_ARRAY                 | indexed array of float/integer                |
 * | DECIMAL_ARRAY                | indexed array of Brick\\Math\\BigDecimal      |
 * | BOOLEAN_ARRAY                | indexed array of boolean                      |
 * | STRING_ARRAY                 | indexed array of string                       |
 * | CHAR_ARRAY                   | indexed array of string (one character)       |
 * | UUID_ARRAY                   | indexed array of array of integers (16 items) |
 * | DATE_ARRAY                   | indexed array of Date                         |
 * | TIME_ARRAY                   | indexed array of Time                         |
 * | TIMESTAMP_ARRAY              | indexed array of Timestamp                    |
 * | ENUM_ARRAY                   | indexed array of EnumItem                     |
 * | OBJECT_ARRAY                 | indexed array                                 |
 * | COLLECTION (USER_COL)        | indexed array                                 |
 * | COLLECTION (ARR_LIST)        | indexed array                                 |
 * | COLLECTION (LINKED_LIST)     | indexed array                                 |
 * | COLLECTION (SINGLETON_LIST)  | indexed array                                 |
 * | COLLECTION (HASH_SET)        | Ds\\Set                                       |
 * | COLLECTION (LINKED_HASH_SET) | Ds\\Set                                       |
 * | COLLECTION (USER_SET)        | Ds\\Set                                       |
 * | MAP (HASH_MAP)               | Ds\\Map, associative array                    |
 * | MAP (LINKED_HASH_MAP)        | Ds\\Map, associative array                    |
 *
 * (*) For all *_ARRAY Ignite types an empty PHP indexed array is allowed.
 *
 * PHP null is allowed as value of a field (but not as a key/value in a cache)
 * or as a value of Array/Set/Map element for all Ignite types,
 * except BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, CHAR, BOOLEAN.
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
     * Gets Ignite type code of this Ignite object type.
     * 
     * @return int Ignite type code
     */
    public function getTypeCode(): int
    {
        return $this->typeCode;
    }

    //The class is abstract and has no public constructor. Only subclasses may be instantiated.
    protected function __construct(int $typeCode)
    {
        $this->typeCode = $typeCode;
    }
}
