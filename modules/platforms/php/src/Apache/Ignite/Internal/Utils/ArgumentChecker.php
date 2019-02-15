<?php
/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
