/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as i18n from '../../../i18n/i18n_ast';
import * as html from '../../../ml_parser/ast';
import * as o from '../../../output/output_ast';
export declare const TRANSLATION_PREFIX = "I18N_";
/** Name of the i18n attributes **/
export declare const I18N_ATTR = "i18n";
export declare const I18N_ATTR_PREFIX = "i18n-";
/** Prefix of var expressions used in ICUs */
export declare const I18N_ICU_VAR_PREFIX = "VAR_";
/** Prefix of ICU expressions for post processing */
export declare const I18N_ICU_MAPPING_PREFIX = "I18N_EXP_";
/** Placeholder wrapper for i18n expressions **/
export declare const I18N_PLACEHOLDER_SYMBOL = "\uFFFD";
export declare type I18nMeta = {
    id?: string;
    description?: string;
    meaning?: string;
};
export declare function isI18nAttribute(name: string): boolean;
export declare function isI18nRootNode(meta?: i18n.AST): meta is i18n.Message;
export declare function isSingleI18nIcu(meta?: i18n.AST): boolean;
export declare function hasI18nAttrs(element: html.Element): boolean;
export declare function metaFromI18nMessage(message: i18n.Message, id?: string | null): I18nMeta;
export declare function icuFromI18nMessage(message: i18n.Message): i18n.IcuPlaceholder;
export declare function wrapI18nPlaceholder(content: string | number, contextId?: number): string;
export declare function assembleI18nBoundString(strings: string[], bindingStartIndex?: number, contextId?: number): string;
export declare function getSeqNumberGenerator(startsAt?: number): () => number;
export declare function placeholdersToParams(placeholders: Map<string, string[]>): {
    [name: string]: o.Expression;
};
export declare function updatePlaceholderMap(map: Map<string, any[]>, name: string, ...values: any[]): void;
export declare function assembleBoundTextPlaceholders(meta: i18n.AST, bindingStartIndex?: number, contextId?: number): Map<string, any[]>;
export declare function findIndex(items: any[], callback: (item: any) => boolean): number;
/**
 * Parses i18n metas like:
 *  - "@@id",
 *  - "description[@@id]",
 *  - "meaning|description[@@id]"
 * and returns an object with parsed output.
 *
 * @param meta String that represents i18n meta
 * @returns Object with id, meaning and description fields
 */
export declare function parseI18nMeta(meta?: string): I18nMeta;
/**
 * Converts internal placeholder names to public-facing format
 * (for example to use in goog.getMsg call).
 * Example: `START_TAG_DIV_1` is converted to `startTagDiv_1`.
 *
 * @param name The placeholder name that should be formatted
 * @returns Formatted placeholder name
 */
export declare function formatI18nPlaceholderName(name: string, useCamelCase?: boolean): string;
/**
 * Generates a prefix for translation const name.
 *
 * @param extra Additional local prefix that should be injected into translation var name
 * @returns Complete translation const prefix
 */
export declare function getTranslationConstPrefix(extra: string): string;
/**
 * Generates translation declaration statements.
 *
 * @param variable Translation value reference
 * @param closureVar Variable for Closure `goog.getMsg` calls
 * @param message Text message to be translated
 * @param meta Object that contains meta information (id, meaning and description)
 * @param params Object with placeholders key-value pairs
 * @param transformFn Optional transformation (post processing) function reference
 * @returns Array of Statements that represent a given translation
 */
export declare function getTranslationDeclStmts(variable: o.ReadVarExpr, closureVar: o.ReadVarExpr, message: string, meta: I18nMeta, params?: {
    [name: string]: o.Expression;
}, transformFn?: (raw: o.ReadVarExpr) => o.Expression): o.Statement[];
