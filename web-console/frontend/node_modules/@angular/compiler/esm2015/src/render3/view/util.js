/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as o from '../../output/output_ast';
import { splitAtColon } from '../../util';
import * as t from '../r3_ast';
import { isI18nAttribute } from './i18n/util';
/**
 * Checks whether an object key contains potentially unsafe chars, thus the key should be wrapped in
 * quotes. Note: we do not wrap all keys into quotes, as it may have impact on minification and may
 * bot work in some cases when object keys are mangled by minifier.
 *
 * TODO(FW-1136): this is a temporary solution, we need to come up with a better way of working with
 * inputs that contain potentially unsafe chars.
 */
const UNSAFE_OBJECT_KEY_NAME_REGEXP = /[-.]/;
/** Name of the temporary to use during data binding */
export const TEMPORARY_NAME = '_t';
/** Name of the context parameter passed into a template function */
export const CONTEXT_NAME = 'ctx';
/** Name of the RenderFlag passed into a template function */
export const RENDER_FLAGS = 'rf';
/** The prefix reference variables */
export const REFERENCE_PREFIX = '_r';
/** The name of the implicit context reference */
export const IMPLICIT_REFERENCE = '$implicit';
/** Non bindable attribute name **/
export const NON_BINDABLE_ATTR = 'ngNonBindable';
/**
 * Creates an allocator for a temporary variable.
 *
 * A variable declaration is added to the statements the first time the allocator is invoked.
 */
export function temporaryAllocator(statements, name) {
    let temp = null;
    return () => {
        if (!temp) {
            statements.push(new o.DeclareVarStmt(TEMPORARY_NAME, undefined, o.DYNAMIC_TYPE));
            temp = o.variable(name);
        }
        return temp;
    };
}
export function unsupported(feature) {
    if (this) {
        throw new Error(`Builder ${this.constructor.name} doesn't support ${feature} yet`);
    }
    throw new Error(`Feature ${feature} is not supported yet`);
}
export function invalid(arg) {
    throw new Error(`Invalid state: Visitor ${this.constructor.name} doesn't handle ${arg.constructor.name}`);
}
export function asLiteral(value) {
    if (Array.isArray(value)) {
        return o.literalArr(value.map(asLiteral));
    }
    return o.literal(value, o.INFERRED_TYPE);
}
export function conditionallyCreateMapObjectLiteral(keys, keepDeclared) {
    if (Object.getOwnPropertyNames(keys).length > 0) {
        return mapToExpression(keys, keepDeclared);
    }
    return null;
}
function mapToExpression(map, keepDeclared) {
    return o.literalMap(Object.getOwnPropertyNames(map).map(key => {
        // canonical syntax: `dirProp: publicProp`
        // if there is no `:`, use dirProp = elProp
        const value = map[key];
        let declaredName;
        let publicName;
        let minifiedName;
        if (Array.isArray(value)) {
            [publicName, declaredName] = value;
        }
        else {
            [declaredName, publicName] = splitAtColon(key, [key, value]);
        }
        minifiedName = declaredName;
        return {
            key: minifiedName,
            // put quotes around keys that contain potentially unsafe characters
            quoted: UNSAFE_OBJECT_KEY_NAME_REGEXP.test(minifiedName),
            value: (keepDeclared && publicName !== declaredName) ?
                o.literalArr([asLiteral(publicName), asLiteral(declaredName)]) :
                asLiteral(publicName)
        };
    }));
}
/**
 *  Remove trailing null nodes as they are implied.
 */
export function trimTrailingNulls(parameters) {
    while (o.isNull(parameters[parameters.length - 1])) {
        parameters.pop();
    }
    return parameters;
}
export function getQueryPredicate(query, constantPool) {
    if (Array.isArray(query.predicate)) {
        let predicate = [];
        query.predicate.forEach((selector) => {
            // Each item in predicates array may contain strings with comma-separated refs
            // (for ex. 'ref, ref1, ..., refN'), thus we extract individual refs and store them
            // as separate array entities
            const selectors = selector.split(',').map(token => o.literal(token.trim()));
            predicate.push(...selectors);
        });
        return constantPool.getConstLiteral(o.literalArr(predicate), true);
    }
    else {
        return query.predicate;
    }
}
export function noop() { }
export class DefinitionMap {
    constructor() {
        this.values = [];
    }
    set(key, value) {
        if (value) {
            this.values.push({ key, value, quoted: false });
        }
    }
    toLiteralMap() { return o.literalMap(this.values); }
}
/**
 * Extract a map of properties to values for a given element or template node, which can be used
 * by the directive matching machinery.
 *
 * @param elOrTpl the element or template in question
 * @return an object set up for directive matching. For attributes on the element/template, this
 * object maps a property name to its (static) value. For any bindings, this map simply maps the
 * property name to an empty string.
 */
export function getAttrsForDirectiveMatching(elOrTpl) {
    const attributesMap = {};
    if (elOrTpl instanceof t.Template && elOrTpl.tagName !== 'ng-template') {
        elOrTpl.templateAttrs.forEach(a => attributesMap[a.name] = '');
    }
    else {
        elOrTpl.attributes.forEach(a => {
            if (!isI18nAttribute(a.name)) {
                attributesMap[a.name] = a.value;
            }
        });
        elOrTpl.inputs.forEach(i => { attributesMap[i.name] = ''; });
        elOrTpl.outputs.forEach(o => { attributesMap[o.name] = ''; });
    }
    return attributesMap;
}
/** Returns a call expression to a chained instruction, e.g. `property(params[0])(params[1])`. */
export function chainedInstruction(reference, calls, span) {
    let expression = o.importExpr(reference, null, span);
    if (calls.length > 0) {
        for (let i = 0; i < calls.length; i++) {
            expression = expression.callFn(calls[i], span);
        }
    }
    else {
        // Add a blank invocation, in case the `calls` array is empty.
        expression = expression.callFn([], span);
    }
    return expression;
}
/**
 * Gets the number of arguments expected to be passed to a generated instruction in the case of
 * interpolation instructions.
 * @param interpolation An interpolation ast
 */
export function getInterpolationArgsLength(interpolation) {
    const { expressions, strings } = interpolation;
    if (expressions.length === 1 && strings.length === 2 && strings[0] === '' && strings[1] === '') {
        // If the interpolation has one interpolated value, but the prefix and suffix are both empty
        // strings, we only pass one argument, to a special instruction like `propertyInterpolate` or
        // `textInterpolate`.
        return 1;
    }
    else {
        return expressions.length + strings.length;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3ZpZXcvdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFJSCxPQUFPLEtBQUssQ0FBQyxNQUFNLHlCQUF5QixDQUFDO0FBRTdDLE9BQU8sRUFBQyxZQUFZLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFDeEMsT0FBTyxLQUFLLENBQUMsTUFBTSxXQUFXLENBQUM7QUFHL0IsT0FBTyxFQUFDLGVBQWUsRUFBQyxNQUFNLGFBQWEsQ0FBQztBQUc1Qzs7Ozs7OztHQU9HO0FBQ0gsTUFBTSw2QkFBNkIsR0FBRyxNQUFNLENBQUM7QUFFN0MsdURBQXVEO0FBQ3ZELE1BQU0sQ0FBQyxNQUFNLGNBQWMsR0FBRyxJQUFJLENBQUM7QUFFbkMsb0VBQW9FO0FBQ3BFLE1BQU0sQ0FBQyxNQUFNLFlBQVksR0FBRyxLQUFLLENBQUM7QUFFbEMsNkRBQTZEO0FBQzdELE1BQU0sQ0FBQyxNQUFNLFlBQVksR0FBRyxJQUFJLENBQUM7QUFFakMscUNBQXFDO0FBQ3JDLE1BQU0sQ0FBQyxNQUFNLGdCQUFnQixHQUFHLElBQUksQ0FBQztBQUVyQyxpREFBaUQ7QUFDakQsTUFBTSxDQUFDLE1BQU0sa0JBQWtCLEdBQUcsV0FBVyxDQUFDO0FBRTlDLG1DQUFtQztBQUNuQyxNQUFNLENBQUMsTUFBTSxpQkFBaUIsR0FBRyxlQUFlLENBQUM7QUFFakQ7Ozs7R0FJRztBQUNILE1BQU0sVUFBVSxrQkFBa0IsQ0FBQyxVQUF5QixFQUFFLElBQVk7SUFDeEUsSUFBSSxJQUFJLEdBQXVCLElBQUksQ0FBQztJQUNwQyxPQUFPLEdBQUcsRUFBRTtRQUNWLElBQUksQ0FBQyxJQUFJLEVBQUU7WUFDVCxVQUFVLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLGNBQWMsQ0FBQyxjQUFjLEVBQUUsU0FBUyxF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Q0FDOUIsU0FBOEIsRUFBRSxLQUF1QixFQUFFLElBQTZCO0lBQ3hGLElBQUksVUFBVSxHQUFHLENBQUMsQ0FBQyxVQUFVLENBQUMsU0FBUyxFQUFFLElBQUksRUFBRSxJQUFJLENBQWlCLENBQUM7SUFFckUsSUFBSSxLQUFLLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtRQUNwQixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsS0FBSyxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTtZQUNyQyxVQUFVLEdBQUcsVUFBVSxDQUFDLE1BQU0sQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLEVBQUUsSUFBSSxDQUFDLENBQUM7U0FDaEQ7S0FDRjtTQUFNO1FBQ0wsOERBQThEO1FBQzlELFVBQVUsR0FBRyxVQUFVLENBQUMsTUFBTSxDQUFDLEVBQUUsRUFBRSxJQUFJLENBQUMsQ0FBQztLQUMxQztJQUVELE9BQU8sVUFBVSxDQUFDO0FBQ3BCLENBQUM7QUFFRDs7OztHQUlHO0FBQ0gsTUFBTSxVQUFVLDBCQUEwQixDQUFDLGFBQTRCO0lBQ3JFLE1BQU0sRUFBQyxXQUFXLEVBQUUsT0FBTyxFQUFDLEdBQUcsYUFBYSxDQUFDO0lBQzdDLElBQUksV0FBVyxDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksT0FBTyxDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksT0FBTyxDQUFDLENBQUMsQ0FBQyxLQUFLLEVBQUUsSUFBSSxPQUFPLENBQUMsQ0FBQyxDQUFDLEtBQUssRUFBRSxFQUFFO1FBQzlGLDRGQUE0RjtRQUM1Riw2RkFBNkY7UUFDN0YscUJBQXFCO1FBQ3JCLE9BQU8sQ0FBQyxDQUFDO0tBQ1Y7U0FBTTtRQUNMLE9BQU8sV0FBVyxDQUFDLE1BQU0sR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDO0tBQzVDO0FBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb25zdGFudFBvb2x9IGZyb20gJy4uLy4uL2NvbnN0YW50X3Bvb2wnO1xuaW1wb3J0IHtJbnRlcnBvbGF0aW9ufSBmcm9tICcuLi8uLi9leHByZXNzaW9uX3BhcnNlci9hc3QnO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuLi8uLi9vdXRwdXQvb3V0cHV0X2FzdCc7XG5pbXBvcnQge1BhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vLi4vcGFyc2VfdXRpbCc7XG5pbXBvcnQge3NwbGl0QXRDb2xvbn0gZnJvbSAnLi4vLi4vdXRpbCc7XG5pbXBvcnQgKiBhcyB0IGZyb20gJy4uL3IzX2FzdCc7XG5cbmltcG9ydCB7UjNRdWVyeU1ldGFkYXRhfSBmcm9tICcuL2FwaSc7XG5pbXBvcnQge2lzSTE4bkF0dHJpYnV0ZX0gZnJvbSAnLi9pMThuL3V0aWwnO1xuXG5cbi8qKlxuICogQ2hlY2tzIHdoZXRoZXIgYW4gb2JqZWN0IGtleSBjb250YWlucyBwb3RlbnRpYWxseSB1bnNhZmUgY2hhcnMsIHRodXMgdGhlIGtleSBzaG91bGQgYmUgd3JhcHBlZCBpblxuICogcXVvdGVzLiBOb3RlOiB3ZSBkbyBub3Qgd3JhcCBhbGwga2V5cyBpbnRvIHF1b3RlcywgYXMgaXQgbWF5IGhhdmUgaW1wYWN0IG9uIG1pbmlmaWNhdGlvbiBhbmQgbWF5XG4gKiBib3Qgd29yayBpbiBzb21lIGNhc2VzIHdoZW4gb2JqZWN0IGtleXMgYXJlIG1hbmdsZWQgYnkgbWluaWZpZXIuXG4gKlxuICogVE9ETyhGVy0xMTM2KTogdGhpcyBpcyBhIHRlbXBvcmFyeSBzb2x1dGlvbiwgd2UgbmVlZCB0byBjb21lIHVwIHdpdGggYSBiZXR0ZXIgd2F5IG9mIHdvcmtpbmcgd2l0aFxuICogaW5wdXRzIHRoYXQgY29udGFpbiBwb3RlbnRpYWxseSB1bnNhZmUgY2hhcnMuXG4gKi9cbmNvbnN0IFVOU0FGRV9PQkpFQ1RfS0VZX05BTUVfUkVHRVhQID0gL1stLl0vO1xuXG4vKiogTmFtZSBvZiB0aGUgdGVtcG9yYXJ5IHRvIHVzZSBkdXJpbmcgZGF0YSBiaW5kaW5nICovXG5leHBvcnQgY29uc3QgVEVNUE9SQVJZX05BTUUgPSAnX3QnO1xuXG4vKiogTmFtZSBvZiB0aGUgY29udGV4dCBwYXJhbWV0ZXIgcGFzc2VkIGludG8gYSB0ZW1wbGF0ZSBmdW5jdGlvbiAqL1xuZXhwb3J0IGNvbnN0IENPTlRFWFRfTkFNRSA9ICdjdHgnO1xuXG4vKiogTmFtZSBvZiB0aGUgUmVuZGVyRmxhZyBwYXNzZWQgaW50byBhIHRlbXBsYXRlIGZ1bmN0aW9uICovXG5leHBvcnQgY29uc3QgUkVOREVSX0ZMQUdTID0gJ3JmJztcblxuLyoqIFRoZSBwcmVmaXggcmVmZXJlbmNlIHZhcmlhYmxlcyAqL1xuZXhwb3J0IGNvbnN0IFJFRkVSRU5DRV9QUkVGSVggPSAnX3InO1xuXG4vKiogVGhlIG5hbWUgb2YgdGhlIGltcGxpY2l0IGNvbnRleHQgcmVmZXJlbmNlICovXG5leHBvcnQgY29uc3QgSU1QTElDSVRfUkVGRVJFTkNFID0gJyRpbXBsaWNpdCc7XG5cbi8qKiBOb24gYmluZGFibGUgYXR0cmlidXRlIG5hbWUgKiovXG5leHBvcnQgY29uc3QgTk9OX0JJTkRBQkxFX0FUVFIgPSAnbmdOb25CaW5kYWJsZSc7XG5cbi8qKlxuICogQ3JlYXRlcyBhbiBhbGxvY2F0b3IgZm9yIGEgdGVtcG9yYXJ5IHZhcmlhYmxlLlxuICpcbiAqIEEgdmFyaWFibGUgZGVjbGFyYXRpb24gaXMgYWRkZWQgdG8gdGhlIHN0YXRlbWVudHMgdGhlIGZpcnN0IHRpbWUgdGhlIGFsbG9jYXRvciBpcyBpbnZva2VkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gdGVtcG9yYXJ5QWxsb2NhdG9yKHN0YXRlbWVudHM6IG8uU3RhdGVtZW50W10sIG5hbWU6IHN0cmluZyk6ICgpID0+IG8uUmVhZFZhckV4cHIge1xuICBsZXQgdGVtcDogby5SZWFkVmFyRXhwcnxudWxsID0gbnVsbDtcbiAgcmV0dXJuICgpID0+IHtcbiAgICBpZiAoIXRlbXApIHtcbiAgICAgIHN0YXRlbWVudHMucHVzaChuZXcgby5EZWNsYXJlVmFyU3RtdChURU1QT1JBUllfTkFNRSwgdW5kZWZpbmVkLCBvLkRZTkFNSUNfVFlQRSkpO1xuICAgICAgdGVtcCA9IG8udmFyaWFibGUobmFtZSk7XG4gICAgfVxuICAgIHJldHVybiB0ZW1wO1xuICB9O1xufVxuXG5cbmV4cG9ydCBmdW5jdGlvbiB1bnN1cHBvcnRlZCh0aGlzOiB2b2lkfEZ1bmN0aW9uLCBmZWF0dXJlOiBzdHJpbmcpOiBuZXZlciB7XG4gIGlmICh0aGlzKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKGBCdWlsZGVyICR7dGhpcy5jb25zdHJ1Y3Rvci5uYW1lfSBkb2Vzbid0IHN1cHBvcnQgJHtmZWF0dXJlfSB5ZXRgKTtcbiAgfVxuICB0aHJvdyBuZXcgRXJyb3IoYEZlYXR1cmUgJHtmZWF0dXJlfSBpcyBub3Qgc3VwcG9ydGVkIHlldGApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaW52YWxpZDxUPih0aGlzOiB0LlZpc2l0b3IsIGFyZzogby5FeHByZXNzaW9uIHwgby5TdGF0ZW1lbnQgfCB0Lk5vZGUpOiBuZXZlciB7XG4gIHRocm93IG5ldyBFcnJvcihcbiAgICAgIGBJbnZhbGlkIHN0YXRlOiBWaXNpdG9yICR7dGhpcy5jb25zdHJ1Y3Rvci5uYW1lfSBkb2Vzbid0IGhhbmRsZSAke2FyZy5jb25zdHJ1Y3Rvci5uYW1lfWApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYXNMaXRlcmFsKHZhbHVlOiBhbnkpOiBvLkV4cHJlc3Npb24ge1xuICBpZiAoQXJyYXkuaXNBcnJheSh2YWx1ZSkpIHtcbiAgICByZXR1cm4gby5saXRlcmFsQXJyKHZhbHVlLm1hcChhc0xpdGVyYWwpKTtcbiAgfVxuICByZXR1cm4gby5saXRlcmFsKHZhbHVlLCBvLklORkVSUkVEX1RZUEUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY29uZGl0aW9uYWxseUNyZWF0ZU1hcE9iamVjdExpdGVyYWwoXG4gICAga2V5czoge1trZXk6IHN0cmluZ106IHN0cmluZyB8IHN0cmluZ1tdfSwga2VlcERlY2xhcmVkPzogYm9vbGVhbik6IG8uRXhwcmVzc2lvbnxudWxsIHtcbiAgaWYgKE9iamVjdC5nZXRPd25Qcm9wZXJ0eU5hbWVzKGtleXMpLmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gbWFwVG9FeHByZXNzaW9uKGtleXMsIGtlZXBEZWNsYXJlZCk7XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmZ1bmN0aW9uIG1hcFRvRXhwcmVzc2lvbihcbiAgICBtYXA6IHtba2V5OiBzdHJpbmddOiBzdHJpbmcgfCBzdHJpbmdbXX0sIGtlZXBEZWNsYXJlZD86IGJvb2xlYW4pOiBvLkV4cHJlc3Npb24ge1xuICByZXR1cm4gby5saXRlcmFsTWFwKE9iamVjdC5nZXRPd25Qcm9wZXJ0eU5hbWVzKG1hcCkubWFwKGtleSA9PiB7XG4gICAgLy8gY2Fub25pY2FsIHN5bnRheDogYGRpclByb3A6IHB1YmxpY1Byb3BgXG4gICAgLy8gaWYgdGhlcmUgaXMgbm8gYDpgLCB1c2UgZGlyUHJvcCA9IGVsUHJvcFxuICAgIGNvbnN0IHZhbHVlID0gbWFwW2tleV07XG4gICAgbGV0IGRlY2xhcmVkTmFtZTogc3RyaW5nO1xuICAgIGxldCBwdWJsaWNOYW1lOiBzdHJpbmc7XG4gICAgbGV0IG1pbmlmaWVkTmFtZTogc3RyaW5nO1xuICAgIGlmIChBcnJheS5pc0FycmF5KHZhbHVlKSkge1xuICAgICAgW3B1YmxpY05hbWUsIGRlY2xhcmVkTmFtZV0gPSB2YWx1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgW2RlY2xhcmVkTmFtZSwgcHVibGljTmFtZV0gPSBzcGxpdEF0Q29sb24oa2V5LCBba2V5LCB2YWx1ZV0pO1xuICAgIH1cbiAgICBtaW5pZmllZE5hbWUgPSBkZWNsYXJlZE5hbWU7XG4gICAgcmV0dXJuIHtcbiAgICAgIGtleTogbWluaWZpZWROYW1lLFxuICAgICAgLy8gcHV0IHF1b3RlcyBhcm91bmQga2V5cyB0aGF0IGNvbnRhaW4gcG90ZW50aWFsbHkgdW5zYWZlIGNoYXJhY3RlcnNcbiAgICAgIHF1b3RlZDogVU5TQUZFX09CSkVDVF9LRVlfTkFNRV9SRUdFWFAudGVzdChtaW5pZmllZE5hbWUpLFxuICAgICAgdmFsdWU6IChrZWVwRGVjbGFyZWQgJiYgcHVibGljTmFtZSAhPT0gZGVjbGFyZWROYW1lKSA/XG4gICAgICAgICAgby5saXRlcmFsQXJyKFthc0xpdGVyYWwocHVibGljTmFtZSksIGFzTGl0ZXJhbChkZWNsYXJlZE5hbWUpXSkgOlxuICAgICAgICAgIGFzTGl0ZXJhbChwdWJsaWNOYW1lKVxuICAgIH07XG4gIH0pKTtcbn1cblxuLyoqXG4gKiAgUmVtb3ZlIHRyYWlsaW5nIG51bGwgbm9kZXMgYXMgdGhleSBhcmUgaW1wbGllZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHRyaW1UcmFpbGluZ051bGxzKHBhcmFtZXRlcnM6IG8uRXhwcmVzc2lvbltdKTogby5FeHByZXNzaW9uW10ge1xuICB3aGlsZSAoby5pc051bGwocGFyYW1ldGVyc1twYXJhbWV0ZXJzLmxlbmd0aCAtIDFdKSkge1xuICAgIHBhcmFtZXRlcnMucG9wKCk7XG4gIH1cbiAgcmV0dXJuIHBhcmFtZXRlcnM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRRdWVyeVByZWRpY2F0ZShcbiAgICBxdWVyeTogUjNRdWVyeU1ldGFkYXRhLCBjb25zdGFudFBvb2w6IENvbnN0YW50UG9vbCk6IG8uRXhwcmVzc2lvbiB7XG4gIGlmIChBcnJheS5pc0FycmF5KHF1ZXJ5LnByZWRpY2F0ZSkpIHtcbiAgICBsZXQgcHJlZGljYXRlOiBvLkV4cHJlc3Npb25bXSA9IFtdO1xuICAgIHF1ZXJ5LnByZWRpY2F0ZS5mb3JFYWNoKChzZWxlY3Rvcjogc3RyaW5nKTogdm9pZCA9PiB7XG4gICAgICAvLyBFYWNoIGl0ZW0gaW4gcHJlZGljYXRlcyBhcnJheSBtYXkgY29udGFpbiBzdHJpbmdzIHdpdGggY29tbWEtc2VwYXJhdGVkIHJlZnNcbiAgICAgIC8vIChmb3IgZXguICdyZWYsIHJlZjEsIC4uLiwgcmVmTicpLCB0aHVzIHdlIGV4dHJhY3QgaW5kaXZpZHVhbCByZWZzIGFuZCBzdG9yZSB0aGVtXG4gICAgICAvLyBhcyBzZXBhcmF0ZSBhcnJheSBlbnRpdGllc1xuICAgICAgY29uc3Qgc2VsZWN0b3JzID0gc2VsZWN0b3Iuc3BsaXQoJywnKS5tYXAodG9rZW4gPT4gby5saXRlcmFsKHRva2VuLnRyaW0oKSkpO1xuICAgICAgcHJlZGljYXRlLnB1c2goLi4uc2VsZWN0b3JzKTtcbiAgICB9KTtcbiAgICByZXR1cm4gY29uc3RhbnRQb29sLmdldENvbnN0TGl0ZXJhbChvLmxpdGVyYWxBcnIocHJlZGljYXRlKSwgdHJ1ZSk7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIHF1ZXJ5LnByZWRpY2F0ZTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gbm9vcCgpIHt9XG5cbmV4cG9ydCBjbGFzcyBEZWZpbml0aW9uTWFwIHtcbiAgdmFsdWVzOiB7a2V5OiBzdHJpbmcsIHF1b3RlZDogYm9vbGVhbiwgdmFsdWU6IG8uRXhwcmVzc2lvbn1bXSA9IFtdO1xuXG4gIHNldChrZXk6IHN0cmluZywgdmFsdWU6IG8uRXhwcmVzc2lvbnxudWxsKTogdm9pZCB7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLnZhbHVlcy5wdXNoKHtrZXksIHZhbHVlLCBxdW90ZWQ6IGZhbHNlfSk7XG4gICAgfVxuICB9XG5cbiAgdG9MaXRlcmFsTWFwKCk6IG8uTGl0ZXJhbE1hcEV4cHIgeyByZXR1cm4gby5saXRlcmFsTWFwKHRoaXMudmFsdWVzKTsgfVxufVxuXG4vKipcbiAqIEV4dHJhY3QgYSBtYXAgb2YgcHJvcGVydGllcyB0byB2YWx1ZXMgZm9yIGEgZ2l2ZW4gZWxlbWVudCBvciB0ZW1wbGF0ZSBub2RlLCB3aGljaCBjYW4gYmUgdXNlZFxuICogYnkgdGhlIGRpcmVjdGl2ZSBtYXRjaGluZyBtYWNoaW5lcnkuXG4gKlxuICogQHBhcmFtIGVsT3JUcGwgdGhlIGVsZW1lbnQgb3IgdGVtcGxhdGUgaW4gcXVlc3Rpb25cbiAqIEByZXR1cm4gYW4gb2JqZWN0IHNldCB1cCBmb3IgZGlyZWN0aXZlIG1hdGNoaW5nLiBGb3IgYXR0cmlidXRlcyBvbiB0aGUgZWxlbWVudC90ZW1wbGF0ZSwgdGhpc1xuICogb2JqZWN0IG1hcHMgYSBwcm9wZXJ0eSBuYW1lIHRvIGl0cyAoc3RhdGljKSB2YWx1ZS4gRm9yIGFueSBiaW5kaW5ncywgdGhpcyBtYXAgc2ltcGx5IG1hcHMgdGhlXG4gKiBwcm9wZXJ0eSBuYW1lIHRvIGFuIGVtcHR5IHN0cmluZy5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldEF0dHJzRm9yRGlyZWN0aXZlTWF0Y2hpbmcoZWxPclRwbDogdC5FbGVtZW50IHwgdC5UZW1wbGF0ZSk6XG4gICAge1tuYW1lOiBzdHJpbmddOiBzdHJpbmd9IHtcbiAgY29uc3QgYXR0cmlidXRlc01hcDoge1tuYW1lOiBzdHJpbmddOiBzdHJpbmd9ID0ge307XG5cblxuICBpZiAoZWxPclRwbCBpbnN0YW5jZW9mIHQuVGVtcGxhdGUgJiYgZWxPclRwbC50YWdOYW1lICE9PSAnbmctdGVtcGxhdGUnKSB7XG4gICAgZWxPclRwbC50ZW1wbGF0ZUF0dHJzLmZvckVhY2goYSA9PiBhdHRyaWJ1dGVzTWFwW2EubmFtZV0gPSAnJyk7XG4gIH0gZWxzZSB7XG4gICAgZWxPclRwbC5hdHRyaWJ1dGVzLmZvckVhY2goYSA9PiB7XG4gICAgICBpZiAoIWlzSTE4bkF0dHJpYnV0ZShhLm5hbWUpKSB7XG4gICAgICAgIGF0dHJpYnV0ZXNNYXBbYS5uYW1lXSA9IGEudmFsdWU7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBlbE9yVHBsLmlucHV0cy5mb3JFYWNoKGkgPT4geyBhdHRyaWJ1dGVzTWFwW2kubmFtZV0gPSAnJzsgfSk7XG4gICAgZWxPclRwbC5vdXRwdXRzLmZvckVhY2gobyA9PiB7IGF0dHJpYnV0ZXNNYXBbby5uYW1lXSA9ICcnOyB9KTtcbiAgfVxuXG4gIHJldHVybiBhdHRyaWJ1dGVzTWFwO1xufVxuXG4vKiogUmV0dXJucyBhIGNhbGwgZXhwcmVzc2lvbiB0byBhIGNoYWluZWQgaW5zdHJ1Y3Rpb24sIGUuZy4gYHByb3BlcnR5KHBhcmFtc1swXSkocGFyYW1zWzFdKWAuICovXG5leHBvcnQgZnVuY3Rpb24gY2hhaW5lZEluc3RydWN0aW9uKFxuICAgIHJlZmVyZW5jZTogby5FeHRlcm5hbFJlZmVyZW5jZSwgY2FsbHM6IG8uRXhwcmVzc2lvbltdW10sIHNwYW4/OiBQYXJzZVNvdXJjZVNwYW4gfCBudWxsKSB7XG4gIGxldCBleHByZXNzaW9uID0gby5pbXBvcnRFeHByKHJlZmVyZW5jZSwgbnVsbCwgc3BhbikgYXMgby5FeHByZXNzaW9uO1xuXG4gIGlmIChjYWxscy5sZW5ndGggPiAwKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjYWxscy5sZW5ndGg7IGkrKykge1xuICAgICAgZXhwcmVzc2lvbiA9IGV4cHJlc3Npb24uY2FsbEZuKGNhbGxzW2ldLCBzcGFuKTtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgLy8gQWRkIGEgYmxhbmsgaW52b2NhdGlvbiwgaW4gY2FzZSB0aGUgYGNhbGxzYCBhcnJheSBpcyBlbXB0eS5cbiAgICBleHByZXNzaW9uID0gZXhwcmVzc2lvbi5jYWxsRm4oW10sIHNwYW4pO1xuICB9XG5cbiAgcmV0dXJuIGV4cHJlc3Npb247XG59XG5cbi8qKlxuICogR2V0cyB0aGUgbnVtYmVyIG9mIGFyZ3VtZW50cyBleHBlY3RlZCB0byBiZSBwYXNzZWQgdG8gYSBnZW5lcmF0ZWQgaW5zdHJ1Y3Rpb24gaW4gdGhlIGNhc2Ugb2ZcbiAqIGludGVycG9sYXRpb24gaW5zdHJ1Y3Rpb25zLlxuICogQHBhcmFtIGludGVycG9sYXRpb24gQW4gaW50ZXJwb2xhdGlvbiBhc3RcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldEludGVycG9sYXRpb25BcmdzTGVuZ3RoKGludGVycG9sYXRpb246IEludGVycG9sYXRpb24pIHtcbiAgY29uc3Qge2V4cHJlc3Npb25zLCBzdHJpbmdzfSA9IGludGVycG9sYXRpb247XG4gIGlmIChleHByZXNzaW9ucy5sZW5ndGggPT09IDEgJiYgc3RyaW5ncy5sZW5ndGggPT09IDIgJiYgc3RyaW5nc1swXSA9PT0gJycgJiYgc3RyaW5nc1sxXSA9PT0gJycpIHtcbiAgICAvLyBJZiB0aGUgaW50ZXJwb2xhdGlvbiBoYXMgb25lIGludGVycG9sYXRlZCB2YWx1ZSwgYnV0IHRoZSBwcmVmaXggYW5kIHN1ZmZpeCBhcmUgYm90aCBlbXB0eVxuICAgIC8vIHN0cmluZ3MsIHdlIG9ubHkgcGFzcyBvbmUgYXJndW1lbnQsIHRvIGEgc3BlY2lhbCBpbnN0cnVjdGlvbiBsaWtlIGBwcm9wZXJ0eUludGVycG9sYXRlYCBvclxuICAgIC8vIGB0ZXh0SW50ZXJwb2xhdGVgLlxuICAgIHJldHVybiAxO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBleHByZXNzaW9ucy5sZW5ndGggKyBzdHJpbmdzLmxlbmd0aDtcbiAgfVxufVxuIl19