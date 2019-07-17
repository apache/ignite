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

package org.apache.ignite.spark.impl.optimization

import org.apache.spark.sql.catalyst.expressions.{Expression, _}

/**
  * Object to support expressions to work with strings like `length` or `trim`.
  */
private[optimization] object StringExpressions extends SupportedExpressions {
    /** @inheritdoc */
    def apply(expr: Expression, checkChild: (Expression) ⇒ Boolean): Boolean = expr match {
        case Ascii(child) ⇒
            checkChild(child)

        case Length(child) ⇒
            checkChild(child)

        case Concat(children) ⇒
            children.forall(checkChild)

        case ConcatWs(children) ⇒
            children.forall(checkChild)

        case StringInstr(str, substr) ⇒
            checkChild(str) && checkChild(substr)

        case Lower(child) ⇒
            checkChild(child)

        case Upper(child) ⇒
            checkChild(child)

        case StringLocate(substr, str, start) ⇒
            checkChild(substr) && checkChild(str) && checkChild(start)

        case StringLPad(str, len, pad) ⇒
            checkChild(str) && checkChild(len) && checkChild(pad)

        case StringRPad(str, len, pad) ⇒
            checkChild(str) && checkChild(len) && checkChild(pad)

        case StringTrimLeft(child, None) ⇒
            checkChild(child)

        case StringTrimRight(child, None) ⇒
            checkChild(child)

        case StringTrim(child, None) ⇒
            checkChild(child)

        case StringTrimLeft(child, Some(trimStr)) ⇒
            checkChild(child) && checkChild(trimStr)

        case StringTrimRight(child,  Some(trimStr)) ⇒
            checkChild(child) && checkChild(trimStr)

        case StringTrim(child,  Some(trimStr)) ⇒
            checkChild(child) && checkChild(trimStr)

        case RegExpReplace(subject, regexp, rep) ⇒
            checkChild(subject) && checkChild(regexp) && checkChild(rep)

        case StringRepeat(str, times) ⇒
            checkChild(str) && checkChild(times)

        case SoundEx(child) ⇒
            checkChild(child)

        case StringSpace(child) ⇒
            checkChild(child)

        case Substring(str, pos, len) ⇒
            checkChild(str) && checkChild(pos) && checkChild(len)

        case Substring(str, pos, len) ⇒
            checkChild(str) && checkChild(pos) && checkChild(len)

        case StringTranslate(str, strMatch, strReplace) ⇒
            checkChild(str) && checkChild(strMatch) && checkChild(strReplace)

        case _ ⇒ false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case Ascii(child) ⇒
            Some(s"ASCII(${childToString(child)})")

        case Length(child) ⇒
            Some(s"CAST(LENGTH(${childToString(child)}) AS INTEGER)")

        case Concat(children) ⇒
            Some(s"CONCAT(${children.map(childToString(_)).mkString(", ")})")

        case ConcatWs(children) ⇒
            Some(s"CONCAT_WS(${children.map(childToString(_)).mkString(", ")})")

        case StringInstr(str, substr) ⇒
            Some(s"POSITION(${childToString(substr)}, ${childToString(str)})")

        case Lower(child) ⇒
            Some(s"LOWER(${childToString(child)})")

        case Upper(child) ⇒
            Some(s"UPPER(${childToString(child)})")

        case StringLocate(substr, str, start) ⇒
            Some(s"LOCATE(${childToString(substr)}, ${childToString(str)}, ${childToString(start)})")

        case StringLPad(str, len, pad) ⇒
            Some(s"LPAD(${childToString(str)}, ${childToString(len)}, ${childToString(pad)})")

        case StringRPad(str, len, pad) ⇒
            Some(s"RPAD(${childToString(str)}, ${childToString(len)}, ${childToString(pad)})")

        case StringTrimLeft(child, None) ⇒
            Some(s"LTRIM(${childToString(child)})")

        case StringTrimRight(child, None) ⇒
            Some(s"RTRIM(${childToString(child)})")

        case StringTrim(child, None) ⇒
            Some(s"TRIM(${childToString(child)})")

        case StringTrimLeft(child,  Some(trimStr)) ⇒
            Some(s"LTRIM(${childToString(child)}, ${childToString(trimStr)})")

        case StringTrimRight(child,  Some(trimStr)) ⇒
            Some(s"RTRIM(${childToString(child)}, ${childToString(trimStr)})")

        case StringTrim(child,  Some(trimStr)) ⇒
            Some(s"TRIM(${childToString(child)}, ${childToString(trimStr)})")

        case RegExpReplace(subject, regexp, rep) ⇒
            Some(s"REGEXP_REPLACE(${childToString(subject)}, ${childToString(regexp)}, ${childToString(rep)})")

        case StringRepeat(str, times) ⇒
            Some(s"REPEAT(${childToString(str)}, ${childToString(times)})")

        case SoundEx(child) ⇒
            Some(s"SOUND_EX(${childToString(child)})")

        case StringSpace(child) ⇒
            Some(s"SPACE(${childToString(child)})")

        case Substring(str, pos, len) ⇒
            Some(s"SUBSTR(${childToString(str)}, ${childToString(pos)}, ${childToString(len)})")

        case StringTranslate(str, strMatch, strReplace) ⇒
            Some(s"TRANSLATE(${childToString(str)}, ${childToString(strMatch)}, ${childToString(strReplace)})")

        case _ ⇒
            None
    }
}
