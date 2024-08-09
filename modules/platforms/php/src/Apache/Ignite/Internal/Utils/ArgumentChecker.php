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

namespace Apache\Ignite\Internal\Utils;

use Apache\Ignite\Exception\ClientException;

class ArgumentChecker
{
    public static function notEmpty($arg, string $argName): void
    {
        if (empty($arg)) {
            ArgumentChecker::illegalArgument(sprintf('"%s" argument should not be empty', $argName));
        }
    }

    public static function notNull($arg, string $argName): void
    {
        if (is_null($arg)) {
            ArgumentChecker::illegalArgument(sprintf('"%s" argument should not be null', $argName));
        }
    }

    public static function hasType($arg, string $argName, bool $isArray, ...$types): void
    {
        if ($arg === null) {
            return;
        }
        if ($isArray && is_array($arg)) {
            foreach ($arg as $a) {
                ArgumentChecker::hasType($a, $argName, false, ...$types);
            }
        } else {
            foreach ($types as $type) {
                if ($arg instanceof $type) {
                    return;
                }
            }
            ArgumentChecker::illegalArgument(sprintf('"%s" argument has incorrect type', $argName));
        }
    }

    public static function hasValueFrom($arg, string $argName, bool $isArray, array $values): void
    {
        if ($isArray && is_array($arg)) {
            foreach ($arg as $a) {
                ArgumentChecker::hasValueFrom($a, $argName, false, $values);
            }
        } else {
            if (!in_array($arg, $values)) {
                ArgumentChecker::invalidValue($argName);
            }
        }
    }

    public static function invalidValue(string $argName): void
    {
        ArgumentChecker::illegalArgument(sprintf('"%s" argument has incorrect value', $argName));
    }

    public static function invalidArgument($arg, string $argName, string $typeName): void
    {
        if ($arg !== null) {
            ArgumentChecker::illegalArgument(sprintf('"%s" argument is invalid for %s', $argName, $typeName));
        }
    }

    public static function illegalArgument($message): void
    {
        throw new ClientException($message);
    }
}
