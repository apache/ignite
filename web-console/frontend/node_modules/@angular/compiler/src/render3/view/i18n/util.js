/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/render3/view/i18n/util", ["require", "exports", "tslib", "@angular/compiler/src/i18n/i18n_ast", "@angular/compiler/src/i18n/serializers/xmb", "@angular/compiler/src/output/map_util", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/render3/r3_identifiers"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var i18n = require("@angular/compiler/src/i18n/i18n_ast");
    var xmb_1 = require("@angular/compiler/src/i18n/serializers/xmb");
    var map_util_1 = require("@angular/compiler/src/output/map_util");
    var o = require("@angular/compiler/src/output/output_ast");
    var r3_identifiers_1 = require("@angular/compiler/src/render3/r3_identifiers");
    /* Closure variables holding messages must be named `MSG_[A-Z0-9]+` */
    var CLOSURE_TRANSLATION_PREFIX = 'MSG_';
    /* Prefix for non-`goog.getMsg` i18n-related vars */
    exports.TRANSLATION_PREFIX = 'I18N_';
    /** Closure uses `goog.getMsg(message)` to lookup translations */
    var GOOG_GET_MSG = 'goog.getMsg';
    /** Name of the global variable that is used to determine if we use Closure translations or not */
    var NG_I18N_CLOSURE_MODE = 'ngI18nClosureMode';
    /** I18n separators for metadata **/
    var I18N_MEANING_SEPARATOR = '|';
    var I18N_ID_SEPARATOR = '@@';
    /** Name of the i18n attributes **/
    exports.I18N_ATTR = 'i18n';
    exports.I18N_ATTR_PREFIX = 'i18n-';
    /** Prefix of var expressions used in ICUs */
    exports.I18N_ICU_VAR_PREFIX = 'VAR_';
    /** Prefix of ICU expressions for post processing */
    exports.I18N_ICU_MAPPING_PREFIX = 'I18N_EXP_';
    /** Placeholder wrapper for i18n expressions **/
    exports.I18N_PLACEHOLDER_SYMBOL = '�';
    function i18nTranslationToDeclStmt(variable, closureVar, message, meta, params) {
        var statements = [];
        // var I18N_X;
        statements.push(new o.DeclareVarStmt(variable.name, undefined, o.INFERRED_TYPE, null, variable.sourceSpan));
        var args = [o.literal(message)];
        if (params && Object.keys(params).length) {
            args.push(map_util_1.mapLiteral(params, true));
        }
        // Closure JSDoc comments
        var docStatements = i18nMetaToDocStmt(meta);
        var thenStatements = docStatements ? [docStatements] : [];
        var googFnCall = o.variable(GOOG_GET_MSG).callFn(args);
        // const MSG_... = goog.getMsg(..);
        thenStatements.push(closureVar.set(googFnCall).toConstDecl());
        // I18N_X = MSG_...;
        thenStatements.push(new o.ExpressionStatement(variable.set(closureVar)));
        var localizeFnCall = o.importExpr(r3_identifiers_1.Identifiers.i18nLocalize).callFn(args);
        // I18N_X = i18nLocalize(...);
        var elseStatements = [new o.ExpressionStatement(variable.set(localizeFnCall))];
        // if(ngI18nClosureMode) { ... } else { ... }
        statements.push(o.ifStmt(o.variable(NG_I18N_CLOSURE_MODE), thenStatements, elseStatements));
        return statements;
    }
    // Converts i18n meta information for a message (id, description, meaning)
    // to a JsDoc statement formatted as expected by the Closure compiler.
    function i18nMetaToDocStmt(meta) {
        var tags = [];
        if (meta.description) {
            tags.push({ tagName: "desc" /* Desc */, text: meta.description });
        }
        if (meta.meaning) {
            tags.push({ tagName: "meaning" /* Meaning */, text: meta.meaning });
        }
        return tags.length == 0 ? null : new o.JSDocCommentStmt(tags);
    }
    function isI18nAttribute(name) {
        return name === exports.I18N_ATTR || name.startsWith(exports.I18N_ATTR_PREFIX);
    }
    exports.isI18nAttribute = isI18nAttribute;
    function isI18nRootNode(meta) {
        return meta instanceof i18n.Message;
    }
    exports.isI18nRootNode = isI18nRootNode;
    function isSingleI18nIcu(meta) {
        return isI18nRootNode(meta) && meta.nodes.length === 1 && meta.nodes[0] instanceof i18n.Icu;
    }
    exports.isSingleI18nIcu = isSingleI18nIcu;
    function hasI18nAttrs(element) {
        return element.attrs.some(function (attr) { return isI18nAttribute(attr.name); });
    }
    exports.hasI18nAttrs = hasI18nAttrs;
    function metaFromI18nMessage(message, id) {
        if (id === void 0) { id = null; }
        return {
            id: typeof id === 'string' ? id : message.id || '',
            meaning: message.meaning || '',
            description: message.description || ''
        };
    }
    exports.metaFromI18nMessage = metaFromI18nMessage;
    function icuFromI18nMessage(message) {
        return message.nodes[0];
    }
    exports.icuFromI18nMessage = icuFromI18nMessage;
    function wrapI18nPlaceholder(content, contextId) {
        if (contextId === void 0) { contextId = 0; }
        var blockId = contextId > 0 ? ":" + contextId : '';
        return "" + exports.I18N_PLACEHOLDER_SYMBOL + content + blockId + exports.I18N_PLACEHOLDER_SYMBOL;
    }
    exports.wrapI18nPlaceholder = wrapI18nPlaceholder;
    function assembleI18nBoundString(strings, bindingStartIndex, contextId) {
        if (bindingStartIndex === void 0) { bindingStartIndex = 0; }
        if (contextId === void 0) { contextId = 0; }
        if (!strings.length)
            return '';
        var acc = '';
        var lastIdx = strings.length - 1;
        for (var i = 0; i < lastIdx; i++) {
            acc += "" + strings[i] + wrapI18nPlaceholder(bindingStartIndex + i, contextId);
        }
        acc += strings[lastIdx];
        return acc;
    }
    exports.assembleI18nBoundString = assembleI18nBoundString;
    function getSeqNumberGenerator(startsAt) {
        if (startsAt === void 0) { startsAt = 0; }
        var current = startsAt;
        return function () { return current++; };
    }
    exports.getSeqNumberGenerator = getSeqNumberGenerator;
    function placeholdersToParams(placeholders) {
        var params = {};
        placeholders.forEach(function (values, key) {
            params[key] = o.literal(values.length > 1 ? "[" + values.join('|') + "]" : values[0]);
        });
        return params;
    }
    exports.placeholdersToParams = placeholdersToParams;
    function updatePlaceholderMap(map, name) {
        var values = [];
        for (var _i = 2; _i < arguments.length; _i++) {
            values[_i - 2] = arguments[_i];
        }
        var current = map.get(name) || [];
        current.push.apply(current, tslib_1.__spread(values));
        map.set(name, current);
    }
    exports.updatePlaceholderMap = updatePlaceholderMap;
    function assembleBoundTextPlaceholders(meta, bindingStartIndex, contextId) {
        if (bindingStartIndex === void 0) { bindingStartIndex = 0; }
        if (contextId === void 0) { contextId = 0; }
        var startIdx = bindingStartIndex;
        var placeholders = new Map();
        var node = meta instanceof i18n.Message ? meta.nodes.find(function (node) { return node instanceof i18n.Container; }) : meta;
        if (node) {
            node
                .children
                .filter(function (child) { return child instanceof i18n.Placeholder; })
                .forEach(function (child, idx) {
                var content = wrapI18nPlaceholder(startIdx + idx, contextId);
                updatePlaceholderMap(placeholders, child.name, content);
            });
        }
        return placeholders;
    }
    exports.assembleBoundTextPlaceholders = assembleBoundTextPlaceholders;
    function findIndex(items, callback) {
        for (var i = 0; i < items.length; i++) {
            if (callback(items[i])) {
                return i;
            }
        }
        return -1;
    }
    exports.findIndex = findIndex;
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
    function parseI18nMeta(meta) {
        var _a, _b;
        var id;
        var meaning;
        var description;
        if (meta) {
            var idIndex = meta.indexOf(I18N_ID_SEPARATOR);
            var descIndex = meta.indexOf(I18N_MEANING_SEPARATOR);
            var meaningAndDesc = void 0;
            _a = tslib_1.__read((idIndex > -1) ? [meta.slice(0, idIndex), meta.slice(idIndex + 2)] : [meta, ''], 2), meaningAndDesc = _a[0], id = _a[1];
            _b = tslib_1.__read((descIndex > -1) ?
                [meaningAndDesc.slice(0, descIndex), meaningAndDesc.slice(descIndex + 1)] :
                ['', meaningAndDesc], 2), meaning = _b[0], description = _b[1];
        }
        return { id: id, meaning: meaning, description: description };
    }
    exports.parseI18nMeta = parseI18nMeta;
    /**
     * Converts internal placeholder names to public-facing format
     * (for example to use in goog.getMsg call).
     * Example: `START_TAG_DIV_1` is converted to `startTagDiv_1`.
     *
     * @param name The placeholder name that should be formatted
     * @returns Formatted placeholder name
     */
    function formatI18nPlaceholderName(name, useCamelCase) {
        if (useCamelCase === void 0) { useCamelCase = true; }
        var publicName = xmb_1.toPublicName(name);
        if (!useCamelCase) {
            return publicName;
        }
        var chunks = publicName.split('_');
        if (chunks.length === 1) {
            // if no "_" found - just lowercase the value
            return name.toLowerCase();
        }
        var postfix;
        // eject last element if it's a number
        if (/^\d+$/.test(chunks[chunks.length - 1])) {
            postfix = chunks.pop();
        }
        var raw = chunks.shift().toLowerCase();
        if (chunks.length) {
            raw += chunks.map(function (c) { return c.charAt(0).toUpperCase() + c.slice(1).toLowerCase(); }).join('');
        }
        return postfix ? raw + "_" + postfix : raw;
    }
    exports.formatI18nPlaceholderName = formatI18nPlaceholderName;
    /**
     * Generates a prefix for translation const name.
     *
     * @param extra Additional local prefix that should be injected into translation var name
     * @returns Complete translation const prefix
     */
    function getTranslationConstPrefix(extra) {
        return ("" + CLOSURE_TRANSLATION_PREFIX + extra).toUpperCase();
    }
    exports.getTranslationConstPrefix = getTranslationConstPrefix;
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
    function getTranslationDeclStmts(variable, closureVar, message, meta, params, transformFn) {
        if (params === void 0) { params = {}; }
        var statements = [];
        statements.push.apply(statements, tslib_1.__spread(i18nTranslationToDeclStmt(variable, closureVar, message, meta, params)));
        if (transformFn) {
            statements.push(new o.ExpressionStatement(variable.set(transformFn(variable))));
        }
        return statements;
    }
    exports.getTranslationDeclStmts = getTranslationDeclStmts;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3ZpZXcvaTE4bi91dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILDBEQUErQztJQUMvQyxrRUFBMkQ7SUFFM0Qsa0VBQW9EO0lBQ3BELDJEQUFnRDtJQUNoRCwrRUFBdUQ7SUFHdkQsc0VBQXNFO0lBQ3RFLElBQU0sMEJBQTBCLEdBQUcsTUFBTSxDQUFDO0lBRTFDLG9EQUFvRDtJQUN2QyxRQUFBLGtCQUFrQixHQUFHLE9BQU8sQ0FBQztJQUUxQyxpRUFBaUU7SUFDakUsSUFBTSxZQUFZLEdBQUcsYUFBYSxDQUFDO0lBRW5DLGtHQUFrRztJQUNsRyxJQUFNLG9CQUFvQixHQUFHLG1CQUFtQixDQUFDO0lBRWpELG9DQUFvQztJQUNwQyxJQUFNLHNCQUFzQixHQUFHLEdBQUcsQ0FBQztJQUNuQyxJQUFNLGlCQUFpQixHQUFHLElBQUksQ0FBQztJQUUvQixtQ0FBbUM7SUFDdEIsUUFBQSxTQUFTLEdBQUcsTUFBTSxDQUFDO0lBQ25CLFFBQUEsZ0JBQWdCLEdBQUcsT0FBTyxDQUFDO0lBRXhDLDZDQUE2QztJQUNoQyxRQUFBLG1CQUFtQixHQUFHLE1BQU0sQ0FBQztJQUUxQyxvREFBb0Q7SUFDdkMsUUFBQSx1QkFBdUIsR0FBRyxXQUFXLENBQUM7SUFFbkQsZ0RBQWdEO0lBQ25DLFFBQUEsdUJBQXVCLEdBQUcsR0FBRyxDQUFDO0lBUTNDLFNBQVMseUJBQXlCLENBQzlCLFFBQXVCLEVBQUUsVUFBeUIsRUFBRSxPQUFlLEVBQUUsSUFBYyxFQUNuRixNQUF1QztRQUN6QyxJQUFNLFVBQVUsR0FBa0IsRUFBRSxDQUFDO1FBQ3JDLGNBQWM7UUFDZCxVQUFVLENBQUMsSUFBSSxDQUNYLElBQUksQ0FBQyxDQUFDLGNBQWMsQ0FBQyxRQUFRLENB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QUNyRCxPQUFPLENBQUEsS0FBRywwQkFBMEIsR0FBRyxLQUFPLENBQUEsQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUMvRCxDQUFDO0lBRkQsOERBRUM7SUFFRDs7Ozs7Ozs7OztPQVVHO0lBQ0gsU0FBZ0IsdUJBQXVCLENBQ25DLFFBQXVCLEVBQUUsVUFBeUIsRUFBRSxPQUFlLEVBQUUsSUFBYyxFQUNuRixNQUEyQyxFQUMzQyxXQUFrRDtRQURsRCx1QkFBQSxFQUFBLFdBQTJDO1FBRTdDLElBQU0sVUFBVSxHQUFrQixFQUFFLENBQUM7UUFFckMsVUFBVSxDQUFDLElBQUksT0FBZixVQUFVLG1CQUFTLHlCQUF5QixDQUFDLFFBQVEsRUFBRSxVQUFVLEVBQUUsT0FBTyxFQUFFLElBQUksRUFBRSxNQUFNLENBQUMsR0FBRTtRQUUzRixJQUFJLFdBQVcsRUFBRTtZQUNmLFVBQVUsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsbUJBQW1CLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxXQUFXLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7U0FDakY7UUFFRCxPQUFPLFVBQVUsQ0FBQztJQUNwQixDQUFDO0lBYkQsMERBYUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi4vLi4vLi4vaTE4bi9pMThuX2FzdCc7XG5pbXBvcnQge3RvUHVibGljTmFtZX0gZnJvbSAnLi4vLi4vLi4vaTE4bi9zZXJpYWxpemVycy94bWInO1xuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuLi8uLi8uLi9tbF9wYXJzZXIvYXN0JztcbmltcG9ydCB7bWFwTGl0ZXJhbH0gZnJvbSAnLi4vLi4vLi4vb3V0cHV0L21hcF91dGlsJztcbmltcG9ydCAqIGFzIG8gZnJvbSAnLi4vLi4vLi4vb3V0cHV0L291dHB1dF9hc3QnO1xuaW1wb3J0IHtJZGVudGlmaWVycyBhcyBSM30gZnJvbSAnLi4vLi4vcjNfaWRlbnRpZmllcnMnO1xuXG5cbi8qIENsb3N1cmUgdmFyaWFibGVzIGhvbGRpbmcgbWVzc2FnZXMgbXVzdCBiZSBuYW1lZCBgTVNHX1tBLVowLTldK2AgKi9cbmNvbnN0IENMT1NVUkVfVFJBTlNMQVRJT05fUFJFRklYID0gJ01TR18nO1xuXG4vKiBQcmVmaXggZm9yIG5vbi1gZ29vZy5nZXRNc2dgIGkxOG4tcmVsYXRlZCB2YXJzICovXG5leHBvcnQgY29uc3QgVFJBTlNMQVRJT05fUFJFRklYID0gJ0kxOE5fJztcblxuLyoqIENsb3N1cmUgdXNlcyBgZ29vZy5nZXRNc2cobWVzc2FnZSlgIHRvIGxvb2t1cCB0cmFuc2xhdGlvbnMgKi9cbmNvbnN0IEdPT0dfR0VUX01TRyA9ICdnb29nLmdldE1zZyc7XG5cbi8qKiBOYW1lIG9mIHRoZSBnbG9iYWwgdmFyaWFibGUgdGhhdCBpcyB1c2VkIHRvIGRldGVybWluZSBpZiB3ZSB1c2UgQ2xvc3VyZSB0cmFuc2xhdGlvbnMgb3Igbm90ICovXG5jb25zdCBOR19JMThOX0NMT1NVUkVfTU9ERSA9ICduZ0kxOG5DbG9zdXJlTW9kZSc7XG5cbi8qKiBJMThuIHNlcGFyYXRvcnMgZm9yIG1ldGFkYXRhICoqL1xuY29uc3QgSTE4Tl9NRUFOSU5HX1NFUEFSQVRPUiA9ICd8JztcbmNvbnN0IEkxOE5fSURfU0VQQVJBVE9SID0gJ0BAJztcblxuLyoqIE5hbWUgb2YgdGhlIGkxOG4gYXR0cmlidXRlcyAqKi9cbmV4cG9ydCBjb25zdCBJMThOX0FUVFIgPSAnaTE4bic7XG5leHBvcnQgY29uc3QgSTE4Tl9BVFRSX1BSRUZJWCA9ICdpMThuLSc7XG5cbi8qKiBQcmVmaXggb2YgdmFyIGV4cHJlc3Npb25zIHVzZWQgaW4gSUNVcyAqL1xuZXhwb3J0IGNvbnN0IEkxOE5fSUNVX1ZBUl9QUkVGSVggPSAnVkFSXyc7XG5cbi8qKiBQcmVmaXggb2YgSUNVIGV4cHJlc3Npb25zIGZvciBwb3N0IHByb2Nlc3NpbmcgKi9cbmV4cG9ydCBjb25zdCBJMThOX0lDVV9NQVBQSU5HX1BSRUZJWCA9ICdJMThOX0VYUF8nO1xuXG4vKiogUGxhY2Vob2xkZXIgd3JhcHBlciBmb3IgaTE4biBleHByZXNzaW9ucyAqKi9cbmV4cG9ydCBjb25zdCBJMThOX1BMQUNFSE9MREVSX1NZTUJPTCA9ICfvv70nO1xuXG5leHBvcnQgdHlwZSBJMThuTWV0YSA9IHtcbiAgaWQ/OiBzdHJpbmcsXG4gIGRlc2NyaXB0aW9uPzogc3RyaW5nLFxuICBtZWFuaW5nPzogc3RyaW5nXG59O1xuXG5mdW5jdGlvbiBpMThuVHJhbnNsYXRpb25Ub0RlY2xTdG10KFxuICAgIHZhcmlhYmxlOiBvLlJlYWRWYXJFeHByLCBjbG9zdXJlVmFyOiBvLlJlYWRWYXJFeHByLCBtZXNzYWdlOiBzdHJpbmcsIG1ldGE6IEkxOG5NZXRhLFxuICAgIHBhcmFtcz86IHtbbmFtZTogc3RyaW5nXTogby5FeHByZXNzaW9ufSk6IG8uU3RhdGVtZW50W10ge1xuICBjb25zdCBzdGF0ZW1lbnRzOiBvLlN0YXRlbWVudFtdID0gW107XG4gIC8vIHZhciBJMThOX1g7XG4gIHN0YXRlbWVudHMucHVzaChcbiAgICAgIG5ldyBvLkRlY2xhcmVWYXJTdG10KHZhcmlhYmxlLm5hbWUgISwgdW5kZWZpbmVkLCBvLklORkVSUkVEX1RZUEUsIG51bGwsIHZhcmlhYmxlLnNvdXJjZVNwYW4pKTtcblxuICBjb25zdCBhcmdzID0gW28ubGl0ZXJhbChtZXNzYWdlKSBhcyBvLkV4cHJlc3Npb25dO1xuICBpZiAocGFyYW1zICYmIE9iamVjdC5rZXlzKHBhcmFtcykubGVuZ3RoKSB7XG4gICAgYXJncy5wdXNoKG1hcExpdGVyYWwocGFyYW1zLCB0cnVlKSk7XG4gIH1cblxuICAvLyBDbG9zdXJlIEpTRG9jIGNvbW1lbnRzXG4gIGNvbnN0IGRvY1N0YXRlbWVudHMgPSBpMThuTWV0YVRvRG9jU3RtdChtZXRhKTtcbiAgY29uc3QgdGhlblN0YXRlbWVudHM6IG8uU3RhdGVtZW50W10gPSBkb2NTdGF0ZW1lbnRzID8gW2RvY1N0YXRlbWVudHNdIDogW107XG4gIGNvbnN0IGdvb2dGbkNhbGwgPSBvLnZhcmlhYmxlKEdPT0dfR0VUX01TRykuY2FsbEZuKGFyZ3MpO1xuICAvLyBjb25zdCBNU0dfLi4uID0gZ29vZy5nZXRNc2coLi4pO1xuICB0aGVuU3RhdGVtZW50cy5wdXNoKGNsb3N1cmVWYXIuc2V0KGdvb2dGbkNhbGwpLnRvQ29uc3REZWNsKCkpO1xuICAvLyBJMThOX1ggPSBNU0dfLi4uO1xuICB0aGVuU3RhdGVtZW50cy5wdXNoKG5ldyBvLkV4cHJlc3Npb25TdGF0ZW1lbnQodmFyaWFibGUuc2V0KGNsb3N1cmVWYXIpKSk7XG4gIGNvbnN0IGxvY2FsaXplRm5DYWxsID0gby5pbXBvcnRFeHByKFIzLmkxOG5Mb2NhbGl6ZSkuY2FsbEZuKGFyZ3MpO1xuICAvLyBJMThOX1ggPSBpMThuTG9jYWxpemUoLi4uKTtcbiAgY29uc3QgZWxzZVN0YXRlbWVudHMgPSBbbmV3IG8uRXhwcmVzc2lvblN0YXRlbWVudCh2YXJpYWJsZS5zZXQobG9jYWxpemVGbkNhbGwpKV07XG4gIC8vIGlmKG5nSTE4bkNsb3N1cmVNb2RlKSB7IC4uLiB9IGVsc2UgeyAuLi4gfVxuICBzdGF0ZW1lbnRzLnB1c2goby5pZlN0bXQoby52YXJpYWJsZShOR19JMThOX0NMT1NVUkVfTU9ERSksIHRoZW5TdGF0ZW1lbnRzLCBlbHNlU3RhdGVtZW50cykpO1xuXG4gIHJldHVybiBzdGF0ZW1lbnRzO1xufVxuXG4vLyBDb252ZXJ0cyBpMThuIG1ldGEgaW5mb3JtYXRpb24gZm9yIGEgbWVzc2FnZSAoaWQsIGRlc2NyaXB0aW9uLCBtZWFuaW5nKVxuLy8gdG8gYSBKc0RvYyBzdGF0ZW1lbnQgZm9ybWF0dGVkIGFzIGV4cGVjdGVkIGJ5IHRoZSBDbG9zdXJlIGNvbXBpbGVyLlxuZnVuY3Rpb24gaTE4bk1ldGFUb0RvY1N0bXQobWV0YTogSTE4bk1ldGEpOiBvLkpTRG9jQ29tbWVudFN0bXR8bnVsbCB7XG4gIGNvbnN0IHRhZ3M6IG8uSlNEb2NUYWdbXSA9IFtdO1xuICBpZiAobWV0YS5kZXNjcmlwdGlvbikge1xuICAgIHRhZ3MucHVzaCh7dGFnTmFtZTogby5KU0RvY1RhZ05hbWUuRGVzYywgdGV4dDogbWV0YS5kZXNjcmlwdGlvbn0pO1xuICB9XG4gIGlmIChtZXRhLm1lYW5pbmcpIHtcbiAgICB0YWdzLnB1c2goe3RhZ05hbWU6IG8uSlNEb2NUYWdOYW1lLk1lYW5pbmcsIHRleHQ6IG1ldGEubWVhbmluZ30pO1xuICB9XG4gIHJldHVybiB0YWdzLmxlbmd0aCA9PSAwID8gbnVsbCA6IG5ldyBvLkpTRG9jQ29tbWVudFN0bXQodGFncyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0kxOG5BdHRyaWJ1dGUobmFtZTogc3RyaW5nKTogYm9vbGVhbiB7XG4gIHJldHVybiBuYW1lID09PSBJMThOX0FUVFIgfHwgbmFtZS5zdGFydHNXaXRoKEkxOE5fQVRUUl9QUkVGSVgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNJMThuUm9vdE5vZGUobWV0YT86IGkxOG4uQVNUKTogbWV0YSBpcyBpMThuLk1lc3NhZ2Uge1xuICByZXR1cm4gbWV0YSBpbnN0YW5jZW9mIGkxOG4uTWVzc2FnZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzU2luZ2xlSTE4bkljdShtZXRhPzogaTE4bi5BU1QpOiBib29sZWFuIHtcbiAgcmV0dXJuIGlzSTE4blJvb3ROb2RlKG1ldGEpICYmIG1ldGEubm9kZXMubGVuZ3RoID09PSAxICYmIG1ldGEubm9kZXNbMF0gaW5zdGFuY2VvZiBpMThuLkljdTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGhhc0kxOG5BdHRycyhlbGVtZW50OiBodG1sLkVsZW1lbnQpOiBib29sZWFuIHtcbiAgcmV0dXJuIGVsZW1lbnQuYXR0cnMuc29tZSgoYXR0cjogaHRtbC5BdHRyaWJ1dGUpID0+IGlzSTE4bkF0dHJpYnV0ZShhdHRyLm5hbWUpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG1ldGFGcm9tSTE4bk1lc3NhZ2UobWVzc2FnZTogaTE4bi5NZXNzYWdlLCBpZDogc3RyaW5nIHwgbnVsbCA9IG51bGwpOiBJMThuTWV0YSB7XG4gIHJldHVybiB7XG4gICAgaWQ6IHR5cGVvZiBpZCA9PT0gJ3N0cmluZycgPyBpZCA6IG1lc3NhZ2UuaWQgfHwgJycsXG4gICAgbWVhbmluZzogbWVzc2FnZS5tZWFuaW5nIHx8ICcnLFxuICAgIGRlc2NyaXB0aW9uOiBtZXNzYWdlLmRlc2NyaXB0aW9uIHx8ICcnXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpY3VGcm9tSTE4bk1lc3NhZ2UobWVzc2FnZTogaTE4bi5NZXNzYWdlKSB7XG4gIHJldHVybiBtZXNzYWdlLm5vZGVzWzBdIGFzIGkxOG4uSWN1UGxhY2Vob2xkZXI7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB3cmFwSTE4blBsYWNlaG9sZGVyKGNvbnRlbnQ6IHN0cmluZyB8IG51bWJlciwgY29udGV4dElkOiBudW1iZXIgPSAwKTogc3RyaW5nIHtcbiAgY29uc3QgYmxvY2tJZCA9IGNvbnRleHRJZCA+IDAgPyBgOiR7Y29udGV4dElkfWAgOiAnJztcbiAgcmV0dXJuIGAke0kxOE5fUExBQ0VIT0xERVJfU1lNQk9MfSR7Y29udGVudH0ke2Jsb2NrSWR9JHtJMThOX1BMQUNFSE9MREVSX1NZTUJPTH1gO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYXNzZW1ibGVJMThuQm91bmRTdHJpbmcoXG4gICAgc3RyaW5nczogc3RyaW5nW10sIGJpbmRpbmdTdGFydEluZGV4OiBudW1iZXIgPSAwLCBjb250ZXh0SWQ6IG51bWJlciA9IDApOiBzdHJpbmcge1xuICBpZiAoIXN0cmluZ3MubGVuZ3RoKSByZXR1cm4gJyc7XG4gIGxldCBhY2MgPSAnJztcbiAgY29uc3QgbGFzdElkeCA9IHN0cmluZ3MubGVuZ3RoIC0gMTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBsYXN0SWR4OyBpKyspIHtcbiAgICBhY2MgKz0gYCR7c3RyaW5nc1tpXX0ke3dyYXBJMThuUGxhY2Vob2xkZXIoYmluZGluZ1N0YXJ0SW5kZXggKyBpLCBjb250ZXh0SWQpfWA7XG4gIH1cbiAgYWNjICs9IHN0cmluZ3NbbGFzdElkeF07XG4gIHJldHVybiBhY2M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXFOdW1iZXJHZW5lcmF0b3Ioc3RhcnRzQXQ6IG51bWJlciA9IDApOiAoKSA9PiBudW1iZXIge1xuICBsZXQgY3VycmVudCA9IHN0YXJ0c0F0O1xuICByZXR1cm4gKCkgPT4gY3VycmVudCsrO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcGxhY2Vob2xkZXJzVG9QYXJhbXMocGxhY2Vob2xkZXJzOiBNYXA8c3RyaW5nLCBzdHJpbmdbXT4pOlxuICAgIHtbbmFtZTogc3RyaW5nXTogby5FeHByZXNzaW9ufSB7XG4gIGNvbnN0IHBhcmFtczoge1tuYW1lOiBzdHJpbmddOiBvLkV4cHJlc3Npb259ID0ge307XG4gIHBsYWNlaG9sZGVycy5mb3JFYWNoKCh2YWx1ZXM6IHN0cmluZ1tdLCBrZXk6IHN0cmluZykgPT4ge1xuICAgIHBhcmFtc1trZXldID0gby5saXRlcmFsKHZhbHVlcy5sZW5ndGggPiAxID8gYFske3ZhbHVlcy5qb2luKCd8Jyl9XWAgOiB2YWx1ZXNbMF0pO1xuICB9KTtcbiAgcmV0dXJuIHBhcmFtcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHVwZGF0ZVBsYWNlaG9sZGVyTWFwKG1hcDogTWFwPHN0cmluZywgYW55W10+LCBuYW1lOiBzdHJpbmcsIC4uLnZhbHVlczogYW55W10pIHtcbiAgY29uc3QgY3VycmVudCA9IG1hcC5nZXQobmFtZSkgfHwgW107XG4gIGN1cnJlbnQucHVzaCguLi52YWx1ZXMpO1xuICBtYXAuc2V0KG5hbWUsIGN1cnJlbnQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYXNzZW1ibGVCb3VuZFRleHRQbGFjZWhvbGRlcnMoXG4gICAgbWV0YTogaTE4bi5BU1QsIGJpbmRpbmdTdGFydEluZGV4OiBudW1iZXIgPSAwLCBjb250ZXh0SWQ6IG51bWJlciA9IDApOiBNYXA8c3RyaW5nLCBhbnlbXT4ge1xuICBjb25zdCBzdGFydElkeCA9IGJpbmRpbmdTdGFydEluZGV4O1xuICBjb25zdCBwbGFjZWhvbGRlcnMgPSBuZXcgTWFwPHN0cmluZywgYW55PigpO1xuICBjb25zdCBub2RlID1cbiAgICAgIG1ldGEgaW5zdGFuY2VvZiBpMThuLk1lc3NhZ2UgPyBtZXRhLm5vZGVzLmZpbmQobm9kZSA9PiBub2RlIGluc3RhbmNlb2YgaTE4bi5Db250YWluZXIpIDogbWV0YTtcbiAgaWYgKG5vZGUpIHtcbiAgICAobm9kZSBhcyBpMThuLkNvbnRhaW5lcilcbiAgICAgICAgLmNoaWxkcmVuXG4gICAgICAgIC5maWx0ZXIoKGNoaWxkOiBpMThuLk5vZGUpOiBjaGlsZCBpcyBpMThuLlBsYWNlaG9sZGVyID0+IGNoaWxkIGluc3RhbmNlb2YgaTE4bi5QbGFjZWhvbGRlcilcbiAgICAgICAgLmZvckVhY2goKGNoaWxkOiBpMThuLlBsYWNlaG9sZGVyLCBpZHg6IG51bWJlcikgPT4ge1xuICAgICAgICAgIGNvbnN0IGNvbnRlbnQgPSB3cmFwSTE4blBsYWNlaG9sZGVyKHN0YXJ0SWR4ICsgaWR4LCBjb250ZXh0SWQpO1xuICAgICAgICAgIHVwZGF0ZVBsYWNlaG9sZGVyTWFwKHBsYWNlaG9sZGVycywgY2hpbGQubmFtZSwgY29udGVudCk7XG4gICAgICAgIH0pO1xuICB9XG4gIHJldHVybiBwbGFjZWhvbGRlcnM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmaW5kSW5kZXgoaXRlbXM6IGFueVtdLCBjYWxsYmFjazogKGl0ZW06IGFueSkgPT4gYm9vbGVhbik6IG51bWJlciB7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgaXRlbXMubGVuZ3RoOyBpKyspIHtcbiAgICBpZiAoY2FsbGJhY2soaXRlbXNbaV0pKSB7XG4gICAgICByZXR1cm4gaTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIC0xO1xufVxuXG4vKipcbiAqIFBhcnNlcyBpMThuIG1ldGFzIGxpa2U6XG4gKiAgLSBcIkBAaWRcIixcbiAqICAtIFwiZGVzY3JpcHRpb25bQEBpZF1cIixcbiAqICAtIFwibWVhbmluZ3xkZXNjcmlwdGlvbltAQGlkXVwiXG4gKiBhbmQgcmV0dXJucyBhbiBvYmplY3Qgd2l0aCBwYXJzZWQgb3V0cHV0LlxuICpcbiAqIEBwYXJhbSBtZXRhIFN0cmluZyB0aGF0IHJlcHJlc2VudHMgaTE4biBtZXRhXG4gKiBAcmV0dXJucyBPYmplY3Qgd2l0aCBpZCwgbWVhbmluZyBhbmQgZGVzY3JpcHRpb24gZmllbGRzXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUkxOG5NZXRhKG1ldGE/OiBzdHJpbmcpOiBJMThuTWV0YSB7XG4gIGxldCBpZDogc3RyaW5nfHVuZGVmaW5lZDtcbiAgbGV0IG1lYW5pbmc6IHN0cmluZ3x1bmRlZmluZWQ7XG4gIGxldCBkZXNjcmlwdGlvbjogc3RyaW5nfHVuZGVmaW5lZDtcblxuICBpZiAobWV0YSkge1xuICAgIGNvbnN0IGlkSW5kZXggPSBtZXRhLmluZGV4T2YoSTE4Tl9JRF9TRVBBUkFUT1IpO1xuICAgIGNvbnN0IGRlc2NJbmRleCA9IG1ldGEuaW5kZXhPZihJMThOX01FQU5JTkdfU0VQQVJBVE9SKTtcbiAgICBsZXQgbWVhbmluZ0FuZERlc2M6IHN0cmluZztcbiAgICBbbWVhbmluZ0FuZERlc2MsIGlkXSA9XG4gICAgICAgIChpZEluZGV4ID4gLTEpID8gW21ldGEuc2xpY2UoMCwgaWRJbmRleCksIG1ldGEuc2xpY2UoaWRJbmRleCArIDIpXSA6IFttZXRhLCAnJ107XG4gICAgW21lYW5pbmcsIGRlc2NyaXB0aW9uXSA9IChkZXNjSW5kZXggPiAtMSkgP1xuICAgICAgICBbbWVhbmluZ0FuZERlc2Muc2xpY2UoMCwgZGVzY0luZGV4KSwgbWVhbmluZ0FuZERlc2Muc2xpY2UoZGVzY0luZGV4ICsgMSldIDpcbiAgICAgICAgWycnLCBtZWFuaW5nQW5kRGVzY107XG4gIH1cblxuICByZXR1cm4ge2lkLCBtZWFuaW5nLCBkZXNjcmlwdGlvbn07XG59XG5cbi8qKlxuICogQ29udmVydHMgaW50ZXJuYWwgcGxhY2Vob2xkZXIgbmFtZXMgdG8gcHVibGljLWZhY2luZyBmb3JtYXRcbiAqIChmb3IgZXhhbXBsZSB0byB1c2UgaW4gZ29vZy5nZXRNc2cgY2FsbCkuXG4gKiBFeGFtcGxlOiBgU1RBUlRfVEFHX0RJVl8xYCBpcyBjb252ZXJ0ZWQgdG8gYHN0YXJ0VGFnRGl2XzFgLlxuICpcbiAqIEBwYXJhbSBuYW1lIFRoZSBwbGFjZWhvbGRlciBuYW1lIHRoYXQgc2hvdWxkIGJlIGZvcm1hdHRlZFxuICogQHJldHVybnMgRm9ybWF0dGVkIHBsYWNlaG9sZGVyIG5hbWVcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdEkxOG5QbGFjZWhvbGRlck5hbWUobmFtZTogc3RyaW5nLCB1c2VDYW1lbENhc2U6IGJvb2xlYW4gPSB0cnVlKTogc3RyaW5nIHtcbiAgY29uc3QgcHVibGljTmFtZSA9IHRvUHVibGljTmFtZShuYW1lKTtcbiAgaWYgKCF1c2VDYW1lbENhc2UpIHtcbiAgICByZXR1cm4gcHVibGljTmFtZTtcbiAgfVxuICBjb25zdCBjaHVua3MgPSBwdWJsaWNOYW1lLnNwbGl0KCdfJyk7XG4gIGlmIChjaHVua3MubGVuZ3RoID09PSAxKSB7XG4gICAgLy8gaWYgbm8gXCJfXCIgZm91bmQgLSBqdXN0IGxvd2VyY2FzZSB0aGUgdmFsdWVcbiAgICByZXR1cm4gbmFtZS50b0xvd2VyQ2FzZSgpO1xuICB9XG4gIGxldCBwb3N0Zml4O1xuICAvLyBlamVjdCBsYXN0IGVsZW1lbnQgaWYgaXQncyBhIG51bWJlclxuICBpZiAoL15cXGQrJC8udGVzdChjaHVua3NbY2h1bmtzLmxlbmd0aCAtIDFdKSkge1xuICAgIHBvc3RmaXggPSBjaHVua3MucG9wKCk7XG4gIH1cbiAgbGV0IHJhdyA9IGNodW5rcy5zaGlmdCgpICEudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGNodW5rcy5sZW5ndGgpIHtcbiAgICByYXcgKz0gY2h1bmtzLm1hcChjID0+IGMuY2hhckF0KDApLnRvVXBwZXJDYXNlKCkgKyBjLnNsaWNlKDEpLnRvTG93ZXJDYXNlKCkpLmpvaW4oJycpO1xuICB9XG4gIHJldHVybiBwb3N0Zml4ID8gYCR7cmF3fV8ke3Bvc3RmaXh9YCA6IHJhdztcbn1cblxuLyoqXG4gKiBHZW5lcmF0ZXMgYSBwcmVmaXggZm9yIHRyYW5zbGF0aW9uIGNvbnN0IG5hbWUuXG4gKlxuICogQHBhcmFtIGV4dHJhIEFkZGl0aW9uYWwgbG9jYWwgcHJlZml4IHRoYXQgc2hvdWxkIGJlIGluamVjdGVkIGludG8gdHJhbnNsYXRpb24gdmFyIG5hbWVcbiAqIEByZXR1cm5zIENvbXBsZXRlIHRyYW5zbGF0aW9uIGNvbnN0IHByZWZpeFxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0VHJhbnNsYXRpb25Db25zdFByZWZpeChleHRyYTogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGAke0NMT1NVUkVfVFJBTlNMQVRJT05fUFJFRklYfSR7ZXh0cmF9YC50b1VwcGVyQ2FzZSgpO1xufVxuXG4vKipcbiAqIEdlbmVyYXRlcyB0cmFuc2xhdGlvbiBkZWNsYXJhdGlvbiBzdGF0ZW1lbnRzLlxuICpcbiAqIEBwYXJhbSB2YXJpYWJsZSBUcmFuc2xhdGlvbiB2YWx1ZSByZWZlcmVuY2VcbiAqIEBwYXJhbSBjbG9zdXJlVmFyIFZhcmlhYmxlIGZvciBDbG9zdXJlIGBnb29nLmdldE1zZ2AgY2FsbHNcbiAqIEBwYXJhbSBtZXNzYWdlIFRleHQgbWVzc2FnZSB0byBiZSB0cmFuc2xhdGVkXG4gKiBAcGFyYW0gbWV0YSBPYmplY3QgdGhhdCBjb250YWlucyBtZXRhIGluZm9ybWF0aW9uIChpZCwgbWVhbmluZyBhbmQgZGVzY3JpcHRpb24pXG4gKiBAcGFyYW0gcGFyYW1zIE9iamVjdCB3aXRoIHBsYWNlaG9sZGVycyBrZXktdmFsdWUgcGFpcnNcbiAqIEBwYXJhbSB0cmFuc2Zvcm1GbiBPcHRpb25hbCB0cmFuc2Zvcm1hdGlvbiAocG9zdCBwcm9jZXNzaW5nKSBmdW5jdGlvbiByZWZlcmVuY2VcbiAqIEByZXR1cm5zIEFycmF5IG9mIFN0YXRlbWVudHMgdGhhdCByZXByZXNlbnQgYSBnaXZlbiB0cmFuc2xhdGlvblxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0VHJhbnNsYXRpb25EZWNsU3RtdHMoXG4gICAgdmFyaWFibGU6IG8uUmVhZFZhckV4cHIsIGNsb3N1cmVWYXI6IG8uUmVhZFZhckV4cHIsIG1lc3NhZ2U6IHN0cmluZywgbWV0YTogSTE4bk1ldGEsXG4gICAgcGFyYW1zOiB7W25hbWU6IHN0cmluZ106IG8uRXhwcmVzc2lvbn0gPSB7fSxcbiAgICB0cmFuc2Zvcm1Gbj86IChyYXc6IG8uUmVhZFZhckV4cHIpID0+IG8uRXhwcmVzc2lvbik6IG8uU3RhdGVtZW50W10ge1xuICBjb25zdCBzdGF0ZW1lbnRzOiBvLlN0YXRlbWVudFtdID0gW107XG5cbiAgc3RhdGVtZW50cy5wdXNoKC4uLmkxOG5UcmFuc2xhdGlvblRvRGVjbFN0bXQodmFyaWFibGUsIGNsb3N1cmVWYXIsIG1lc3NhZ2UsIG1ldGEsIHBhcmFtcykpO1xuXG4gIGlmICh0cmFuc2Zvcm1Gbikge1xuICAgIHN0YXRlbWVudHMucHVzaChuZXcgby5FeHByZXNzaW9uU3RhdGVtZW50KHZhcmlhYmxlLnNldCh0cmFuc2Zvcm1Gbih2YXJpYWJsZSkpKSk7XG4gIH1cblxuICByZXR1cm4gc3RhdGVtZW50cztcbn1cbiJdfQ==