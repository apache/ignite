/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { isDifferent } from '../util/misc_utils';
/** @type {?} */
const MAP_BASED_ENTRY_PROP_NAME = '--MAP--';
/** @type {?} */
const TEMPLATE_DIRECTIVE_INDEX = 0;
/**
 * Creates a new instance of the `TStylingContext`.
 *
 * The `TStylingContext` is used as a manifest of all style or all class bindings on
 * an element. Because it is a T-level data-structure, it is only created once per
 * tNode for styles and for classes. This function allocates a new instance of a
 * `TStylingContext` with the initial values (see `interfaces.ts` for more info).
 * @param {?=} initialStyling
 * @return {?}
 */
export function allocTStylingContext(initialStyling) {
    // because map-based bindings deal with a dynamic set of values, there
    // is no way to know ahead of time whether or not sanitization is required.
    // For this reason the configuration will always mark sanitization as active
    // (this means that when map-based values are applied then sanitization will
    // be checked against each property).
    /** @type {?} */
    const mapBasedConfig = 1 /* SanitizationRequired */;
    return [
        initialStyling || [''],
        0 /* Initial */,
        TEMPLATE_DIRECTIVE_INDEX,
        mapBasedConfig,
        0,
        MAP_BASED_ENTRY_PROP_NAME,
    ];
}
/**
 * Sets the provided directive as the last directive index in the provided `TStylingContext`.
 *
 * Styling in Angular can be applied from the template as well as multiple sources of
 * host bindings. This means that each binding function (the template function or the
 * hostBindings functions) will generate styling instructions as well as a styling
 * apply function (i.e. `stylingApply()`). Because host bindings functions and the
 * template function are independent from one another this means that the styling apply
 * function will be called multiple times. By tracking the last directive index (which
 * is what happens in this function) the styling algorithm knows exactly when to flush
 * styling (which is when the last styling apply function is executed).
 * @param {?} context
 * @param {?} lastDirectiveIndex
 * @return {?}
 */
export function updateLastDirectiveIndex(context, lastDirectiveIndex) {
    if (lastDirectiveIndex === TEMPLATE_DIRECTIVE_INDEX) {
        /** @type {?} */
        const currentValue = context[2 /* LastDirectiveIndexPosition */];
        if (currentValue > TEMPLATE_DIRECTIVE_INDEX) {
            // This means that a directive or two contained a host bindings function, but
            // now the template function also contains styling. When this combination of sources
            // comes up then we need to tell the context to store the state between updates
            // (because host bindings evaluation happens after template binding evaluation).
            markContextToPersistState(context);
        }
    }
    else {
        context[2 /* LastDirectiveIndexPosition */] = lastDirectiveIndex;
    }
}
/**
 * @param {?} context
 * @return {?}
 */
function getConfig(context) {
    return context[1 /* ConfigPosition */];
}
/**
 * @param {?} context
 * @param {?} value
 * @return {?}
 */
export function setConfig(context, value) {
    context[1 /* ConfigPosition */] = value;
}
/**
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
export function getProp(context, index) {
    return (/** @type {?} */ (context[index + 2 /* PropOffset */]));
}
/**
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
function getPropConfig(context, index) {
    return ((/** @type {?} */ (context[index + 0 /* ConfigAndGuardOffset */]))) &
        1 /* Mask */;
}
/**
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
export function isSanitizationRequired(context, index) {
    return (getPropConfig(context, index) & 1 /* SanitizationRequired */) > 0;
}
/**
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
export function getGuardMask(context, index) {
    /** @type {?} */
    const configGuardValue = (/** @type {?} */ (context[index + 0 /* ConfigAndGuardOffset */]));
    return configGuardValue >> 1 /* TotalBits */;
}
/**
 * @param {?} context
 * @param {?} index
 * @param {?} maskValue
 * @return {?}
 */
export function setGuardMask(context, index, maskValue) {
    /** @type {?} */
    const config = getPropConfig(context, index);
    /** @type {?} */
    const guardMask = maskValue << 1 /* TotalBits */;
    context[index + 0 /* ConfigAndGuardOffset */] = config | guardMask;
}
/**
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
export function getValuesCount(context, index) {
    return (/** @type {?} */ (context[index + 1 /* ValuesCountOffset */]));
}
/**
 * @param {?} context
 * @param {?} index
 * @param {?} offset
 * @return {?}
 */
export function getBindingValue(context, index, offset) {
    return (/** @type {?} */ (context[index + 3 /* BindingsStartOffset */ + offset]));
}
/**
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
export function getDefaultValue(context, index) {
    /** @type {?} */
    const valuesCount = getValuesCount(context, index);
    return (/** @type {?} */ (context[index + 3 /* BindingsStartOffset */ + valuesCount - 1]));
}
/**
 * Temporary function which determines whether or not a context is
 * allowed to be flushed based on the provided directive index.
 * @param {?} context
 * @param {?} index
 * @return {?}
 */
export function allowStylingFlush(context, index) {
    return (context && index === context[2 /* LastDirectiveIndexPosition */]) ? true :
        false;
}
/**
 * @param {?} context
 * @return {?}
 */
export function lockContext(context) {
    setConfig(context, getConfig(context) | 1 /* Locked */);
}
/**
 * @param {?} context
 * @return {?}
 */
export function isContextLocked(context) {
    return (getConfig(context) & 1 /* Locked */) > 0;
}
/**
 * @param {?} context
 * @return {?}
 */
export function stateIsPersisted(context) {
    return (getConfig(context) & 2 /* PersistStateValues */) > 0;
}
/**
 * @param {?} context
 * @return {?}
 */
export function markContextToPersistState(context) {
    setConfig(context, getConfig(context) | 2 /* PersistStateValues */);
}
/**
 * @param {?} context
 * @return {?}
 */
export function getPropValuesStartPosition(context) {
    return 6 /* MapBindingsBindingsStartPosition */ +
        context[4 /* MapBindingsValuesCountPosition */];
}
/**
 * @param {?} prop
 * @return {?}
 */
export function isMapBased(prop) {
    return prop === MAP_BASED_ENTRY_PROP_NAME;
}
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
export function hasValueChanged(a, b) {
    /** @type {?} */
    let compareValueA = Array.isArray(a) ? a[0 /* RawValuePosition */] : a;
    /** @type {?} */
    let compareValueB = Array.isArray(b) ? b[0 /* RawValuePosition */] : b;
    // these are special cases for String based values (which are created as artifacts
    // when sanitization is bypassed on a particular value)
    if (compareValueA instanceof String) {
        compareValueA = compareValueA.toString();
    }
    if (compareValueB instanceof String) {
        compareValueB = compareValueB.toString();
    }
    return isDifferent(compareValueA, compareValueB);
}
/**
 * Determines whether the provided styling value is truthy or falsy.
 * @param {?} value
 * @return {?}
 */
export function isStylingValueDefined(value) {
    // the reason why null is compared against is because
    // a CSS class value that is set to `false` must be
    // respected (otherwise it would be treated as falsy).
    // Empty string values are because developers usually
    // set a value to an empty string to remove it.
    return value != null && value !== '';
}
/**
 * @param {?} a
 * @param {?} b
 * @param {?=} separator
 * @return {?}
 */
export function concatString(a, b, separator = ' ') {
    return a + ((b.length && a.length) ? separator : '') + b;
}
/**
 * @param {?} value
 * @return {?}
 */
export function hyphenate(value) {
    return value.replace(/[a-z][A-Z]/g, (/**
     * @param {?} v
     * @return {?}
     */
    v => v.charAt(0) + '-' + v.charAt(1))).toLowerCase();
}
/**
 * Returns an instance of `StylingMapArray`.
 *
 * This function is designed to find an instance of `StylingMapArray` in case it is stored
 * inside of an instance of `TStylingContext`. When a styling context is created it
 * will copy over an initial styling values from the tNode (which are stored as a
 * `StylingMapArray` on the `tNode.classes` or `tNode.styles` values).
 * @param {?} value
 * @return {?}
 */
export function getStylingMapArray(value) {
    return isStylingContext(value) ?
        ((/** @type {?} */ (value)))[0 /* InitialStylingValuePosition */] :
        value;
}
/**
 * @param {?} value
 * @return {?}
 */
export function isStylingContext(value) {
    // the StylingMapArray is in the format of [initial, prop, string, prop, string]
    // and this is the defining value to distinguish between arrays
    return Array.isArray(value) &&
        value.length >= 6 /* MapBindingsBindingsStartPosition */ &&
        typeof value[1] !== 'string';
}
/**
 * @param {?} context
 * @return {?}
 */
export function getInitialStylingValue(context) {
    /** @type {?} */
    const map = getStylingMapArray(context);
    return map && ((/** @type {?} */ (map[0 /* RawValuePosition */]))) || '';
}
/**
 * @param {?} tNode
 * @return {?}
 */
export function hasClassInput(tNode) {
    return (tNode.flags & 8 /* hasClassInput */) !== 0;
}
/**
 * @param {?} tNode
 * @return {?}
 */
export function hasStyleInput(tNode) {
    return (tNode.flags & 16 /* hasStyleInput */) !== 0;
}
/**
 * @param {?} map
 * @param {?} index
 * @return {?}
 */
export function getMapProp(map, index) {
    return (/** @type {?} */ (map[index + 0 /* PropOffset */]));
}
/**
 * @param {?} map
 * @param {?} index
 * @param {?} value
 * @return {?}
 */
export function setMapValue(map, index, value) {
    map[index + 1 /* ValueOffset */] = value;
}
/**
 * @param {?} map
 * @param {?} index
 * @return {?}
 */
export function getMapValue(map, index) {
    return (/** @type {?} */ (map[index + 1 /* ValueOffset */]));
}
/**
 * @param {?} classes
 * @return {?}
 */
export function forceClassesAsString(classes) {
    if (classes && typeof classes !== 'string') {
        classes = Object.keys(classes).join(' ');
    }
    return ((/** @type {?} */ (classes))) || '';
}
/**
 * @param {?} styles
 * @return {?}
 */
export function forceStylesAsString(styles) {
    /** @type {?} */
    let str = '';
    if (styles) {
        /** @type {?} */
        const props = Object.keys(styles);
        for (let i = 0; i < props.length; i++) {
            /** @type {?} */
            const prop = props[i];
            str = concatString(str, `${prop}:${styles[prop]}`, ';');
        }
    }
    return str;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvc3R5bGluZ19uZXh0L3V0aWwudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQVFBLE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQzs7TUFJekMseUJBQXlCLEdBQUcsU0FBUzs7TUFDckMsd0JBQXdCLEdBQUcsQ0FBQzs7Ozs7Ozs7Ozs7QUFVbEMsTUFBTSxVQUFVLG9CQUFvQixDQUFDLGNBQXVDOzs7Ozs7O1VBTXBFLGNBQWMsK0JBQXNEO0lBQzFFLE9BQU87UUFDTCxjQUFjLElBQUksQ0FBQyxFQUFFLENBQUM7O1FBRXRCLHdCQUF3QjtRQUN4QixjQUFjO1FBQ2QsQ0FBQztRQUNELHlCQUF5QjtLQUMxQixDQUFDO0FBQ0osQ0FBQzs7Ozs7Ozs7Ozs7Ozs7OztBQWNELE1BQU0sVUFBVSx3QkFBd0IsQ0FDcEMsT0FBd0IsRUFBRSxrQkFBMEI7SUFDdEQsSUFBSSxrQkFBa0IsS0FBSyx3QkFBd0IsRUFBRTs7Y0FDN0MsWUFBWSxHQUFHLE9BQU8sb0NBQWlEO1FBQzdFLElBQUksWUFBWSxHQUFHLHdCQUF3QixFQUFFO1lBQzNDLDZFQUE2RTtZQUM3RSxvRkFBb0Y7WUFDcEYsK0VBQStFO1lBQy9FLGdGQUFnRjtZQUNoRix5QkFBeUIsQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUNwQztLQUNGO1NBQU07UUFDTCxPQUFPLG9DQUFpRCxHQUFHLGtCQUFrQixDQUFDO0tBQy9FO0FBQ0gsQ0FBQzs7Ozs7QUFFRCxTQUFTLFNBQVMsQ0FBQyxPQUF3QjtJQUN6QyxPQUFPLE9BQU8sd0JBQXFDLENBQUM7QUFDdEQsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLFNBQVMsQ0FBQyxPQUF3QixFQUFFLEtBQWE7SUFDL0QsT0FBTyx3QkFBcUMsR0FBRyxLQUFLLENBQUM7QUFDdkQsQ0FBQzs7Ozs7O0FB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QUFFLEdBQUcsSUFBSSxJQUFJLE1BQU0sQ0FBQyxJQUFJLENBQUMsRUFBRSxFQUFFLEdBQUcsQ0FBQyxDQUFDO1NBQ3pEO0tBQ0Y7SUFDRCxPQUFPLEdBQUcsQ0FBQztBQUNiLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiogQGxpY2Vuc2VcbiogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4qXG4qIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4qIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiovXG5pbXBvcnQge1ROb2RlLCBUTm9kZUZsYWdzfSBmcm9tICcuLi9pbnRlcmZhY2VzL25vZGUnO1xuaW1wb3J0IHtpc0RpZmZlcmVudH0gZnJvbSAnLi4vdXRpbC9taXNjX3V0aWxzJztcblxuaW1wb3J0IHtTdHlsaW5nTWFwQXJyYXksIFN0eWxpbmdNYXBBcnJheUluZGV4LCBUU3R5bGluZ0NvbmZpZ0ZsYWdzLCBUU3R5bGluZ0NvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4LCBUU3R5bGluZ0NvbnRleHRQcm9wQ29uZmlnRmxhZ3N9IGZyb20gJy4vaW50ZXJmYWNlcyc7XG5cbmNvbnN0IE1BUF9CQVNFRF9FTlRSWV9QUk9QX05BTUUgPSAnLS1NQVAtLSc7XG5jb25zdCBURU1QTEFURV9ESVJFQ1RJVkVfSU5ERVggPSAwO1xuXG4vKipcbiAqIENyZWF0ZXMgYSBuZXcgaW5zdGFuY2Ugb2YgdGhlIGBUU3R5bGluZ0NvbnRleHRgLlxuICpcbiAqIFRoZSBgVFN0eWxpbmdDb250ZXh0YCBpcyB1c2VkIGFzIGEgbWFuaWZlc3Qgb2YgYWxsIHN0eWxlIG9yIGFsbCBjbGFzcyBiaW5kaW5ncyBvblxuICogYW4gZWxlbWVudC4gQmVjYXVzZSBpdCBpcyBhIFQtbGV2ZWwgZGF0YS1zdHJ1Y3R1cmUsIGl0IGlzIG9ubHkgY3JlYXRlZCBvbmNlIHBlclxuICogdE5vZGUgZm9yIHN0eWxlcyBhbmQgZm9yIGNsYXNzZXMuIFRoaXMgZnVuY3Rpb24gYWxsb2NhdGVzIGEgbmV3IGluc3RhbmNlIG9mIGFcbiAqIGBUU3R5bGluZ0NvbnRleHRgIHdpdGggdGhlIGluaXRpYWwgdmFsdWVzIChzZWUgYGludGVyZmFjZXMudHNgIGZvciBtb3JlIGluZm8pLlxuICovXG5leHBvcnQgZnVuY3Rpb24gYWxsb2NUU3R5bGluZ0NvbnRleHQoaW5pdGlhbFN0eWxpbmc/OiBTdHlsaW5nTWFwQXJyYXkgfCBudWxsKTogVFN0eWxpbmdDb250ZXh0IHtcbiAgLy8gYmVjYXVzZSBtYXAtYmFzZWQgYmluZGluZ3MgZGVhbCB3aXRoIGEgZHluYW1pYyBzZXQgb2YgdmFsdWVzLCB0aGVyZVxuICAvLyBpcyBubyB3YXkgdG8ga25vdyBhaGVhZCBvZiB0aW1lIHdoZXRoZXIgb3Igbm90IHNhbml0aXphdGlvbiBpcyByZXF1aXJlZC5cbiAgLy8gRm9yIHRoaXMgcmVhc29uIHRoZSBjb25maWd1cmF0aW9uIHdpbGwgYWx3YXlzIG1hcmsgc2FuaXRpemF0aW9uIGFzIGFjdGl2ZVxuICAvLyAodGhpcyBtZWFucyB0aGF0IHdoZW4gbWFwLWJhc2VkIHZhbHVlcyBhcmUgYXBwbGllZCB0aGVuIHNhbml0aXphdGlvbiB3aWxsXG4gIC8vIGJlIGNoZWNrZWQgYWdhaW5zdCBlYWNoIHByb3BlcnR5KS5cbiAgY29uc3QgbWFwQmFzZWRDb25maWcgPSBUU3R5bGluZ0NvbnRleHRQcm9wQ29uZmlnRmxhZ3MuU2FuaXRpemF0aW9uUmVxdWlyZWQ7XG4gIHJldHVybiBbXG4gICAgaW5pdGlhbFN0eWxpbmcgfHwgWycnXSwgIC8vIGVtcHR5IGluaXRpYWwtc3R5bGluZyBtYXAgdmFsdWVcbiAgICBUU3R5bGluZ0NvbmZpZ0ZsYWdzLkluaXRpYWwsXG4gICAgVEVNUExBVEVfRElSRUNUSVZFX0lOREVYLFxuICAgIG1hcEJhc2VkQ29uZmlnLFxuICAgIDAsXG4gICAgTUFQX0JBU0VEX0VOVFJZX1BST1BfTkFNRSxcbiAgXTtcbn1cblxuLyoqXG4gKiBTZXRzIHRoZSBwcm92aWRlZCBkaXJlY3RpdmUgYXMgdGhlIGxhc3QgZGlyZWN0aXZlIGluZGV4IGluIHRoZSBwcm92aWRlZCBgVFN0eWxpbmdDb250ZXh0YC5cbiAqXG4gKiBTdHlsaW5nIGluIEFuZ3VsYXIgY2FuIGJlIGFwcGxpZWQgZnJvbSB0aGUgdGVtcGxhdGUgYXMgd2VsbCBhcyBtdWx0aXBsZSBzb3VyY2VzIG9mXG4gKiBob3N0IGJpbmRpbmdzLiBUaGlzIG1lYW5zIHRoYXQgZWFjaCBiaW5kaW5nIGZ1bmN0aW9uICh0aGUgdGVtcGxhdGUgZnVuY3Rpb24gb3IgdGhlXG4gKiBob3N0QmluZGluZ3MgZnVuY3Rpb25zKSB3aWxsIGdlbmVyYXRlIHN0eWxpbmcgaW5zdHJ1Y3Rpb25zIGFzIHdlbGwgYXMgYSBzdHlsaW5nXG4gKiBhcHBseSBmdW5jdGlvbiAoaS5lLiBgc3R5bGluZ0FwcGx5KClgKS4gQmVjYXVzZSBob3N0IGJpbmRpbmdzIGZ1bmN0aW9ucyBhbmQgdGhlXG4gKiB0ZW1wbGF0ZSBmdW5jdGlvbiBhcmUgaW5kZXBlbmRlbnQgZnJvbSBvbmUgYW5vdGhlciB0aGlzIG1lYW5zIHRoYXQgdGhlIHN0eWxpbmcgYXBwbHlcbiAqIGZ1bmN0aW9uIHdpbGwgYmUgY2FsbGVkIG11bHRpcGxlIHRpbWVzLiBCeSB0cmFja2luZyB0aGUgbGFzdCBkaXJlY3RpdmUgaW5kZXggKHdoaWNoXG4gKiBpcyB3aGF0IGhhcHBlbnMgaW4gdGhpcyBmdW5jdGlvbikgdGhlIHN0eWxpbmcgYWxnb3JpdGhtIGtub3dzIGV4YWN0bHkgd2hlbiB0byBmbHVzaFxuICogc3R5bGluZyAod2hpY2ggaXMgd2hlbiB0aGUgbGFzdCBzdHlsaW5nIGFwcGx5IGZ1bmN0aW9uIGlzIGV4ZWN1dGVkKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHVwZGF0ZUxhc3REaXJlY3RpdmVJbmRleChcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGxhc3REaXJlY3RpdmVJbmRleDogbnVtYmVyKTogdm9pZCB7XG4gIGlmIChsYXN0RGlyZWN0aXZlSW5kZXggPT09IFRFTVBMQVRFX0RJUkVDVElWRV9JTkRFWCkge1xuICAgIGNvbnN0IGN1cnJlbnRWYWx1ZSA9IGNvbnRleHRbVFN0eWxpbmdDb250ZXh0SW5kZXguTGFzdERpcmVjdGl2ZUluZGV4UG9zaXRpb25dO1xuICAgIGlmIChjdXJyZW50VmFsdWUgPiBURU1QTEFURV9ESVJFQ1RJVkVfSU5ERVgpIHtcbiAgICAgIC8vIFRoaXMgbWVhbnMgdGhhdCBhIGRpcmVjdGl2ZSBvciB0d28gY29udGFpbmVkIGEgaG9zdCBiaW5kaW5ncyBmdW5jdGlvbiwgYnV0XG4gICAgICAvLyBub3cgdGhlIHRlbXBsYXRlIGZ1bmN0aW9uIGFsc28gY29udGFpbnMgc3R5bGluZy4gV2hlbiB0aGlzIGNvbWJpbmF0aW9uIG9mIHNvdXJjZXNcbiAgICAgIC8vIGNvbWVzIHVwIHRoZW4gd2UgbmVlZCB0byB0ZWxsIHRoZSBjb250ZXh0IHRvIHN0b3JlIHRoZSBzdGF0ZSBiZXR3ZWVuIHVwZGF0ZXNcbiAgICAgIC8vIChiZWNhdXNlIGhvc3QgYmluZGluZ3MgZXZhbHVhdGlvbiBoYXBwZW5zIGFmdGVyIHRlbXBsYXRlIGJpbmRpbmcgZXZhbHVhdGlvbikuXG4gICAgICBtYXJrQ29udGV4dFRvUGVyc2lzdFN0YXRlKGNvbnRleHQpO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBjb250ZXh0W1RTdHlsaW5nQ29udGV4dEluZGV4Lkxhc3REaXJlY3RpdmVJbmRleFBvc2l0aW9uXSA9IGxhc3REaXJlY3RpdmVJbmRleDtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRDb25maWcoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KSB7XG4gIHJldHVybiBjb250ZXh0W1RTdHlsaW5nQ29udGV4dEluZGV4LkNvbmZpZ1Bvc2l0aW9uXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNldENvbmZpZyhjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIHZhbHVlOiBudW1iZXIpIHtcbiAgY29udGV4dFtUU3R5bGluZ0NvbnRleHRJbmRleC5Db25maWdQb3NpdGlvbl0gPSB2YWx1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFByb3AoY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBpbmRleDogbnVtYmVyKSB7XG4gIHJldHVybiBjb250ZXh0W2luZGV4ICsgVFN0eWxpbmdDb250ZXh0SW5kZXguUHJvcE9mZnNldF0gYXMgc3RyaW5nO1xufVxuXG5mdW5jdGlvbiBnZXRQcm9wQ29uZmlnKGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgaW5kZXg6IG51bWJlcik6IG51bWJlciB7XG4gIHJldHVybiAoY29udGV4dFtpbmRleCArIFRTdHlsaW5nQ29udGV4dEluZGV4LkNvbmZpZ0FuZEd1YXJkT2Zmc2V0XSBhcyBudW1iZXIpICZcbiAgICAgIFRTdHlsaW5nQ29udGV4dFByb3BDb25maWdGbGFncy5NYXNrO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTYW5pdGl6YXRpb25SZXF1aXJlZChjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGluZGV4OiBudW1iZXIpIHtcbiAgcmV0dXJuIChnZXRQcm9wQ29uZmlnKGNvbnRleHQsIGluZGV4KSAmIFRTdHlsaW5nQ29udGV4dFByb3BDb25maWdGbGFncy5TYW5pdGl6YXRpb25SZXF1aXJlZCkgPiAwO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0R3VhcmRNYXNrKGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgaW5kZXg6IG51bWJlcikge1xuICBjb25zdCBjb25maWdHdWFyZFZhbHVlID0gY29udGV4dFtpbmRleCArIFRTdHlsaW5nQ29udGV4dEluZGV4LkNvbmZpZ0FuZEd1YXJkT2Zmc2V0XSBhcyBudW1iZXI7XG4gIHJldHVybiBjb25maWdHdWFyZFZhbHVlID4+IFRTdHlsaW5nQ29udGV4dFByb3BDb25maWdGbGFncy5Ub3RhbEJpdHM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzZXRHdWFyZE1hc2soY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBpbmRleDogbnVtYmVyLCBtYXNrVmFsdWU6IG51bWJlcikge1xuICBjb25zdCBjb25maWcgPSBnZXRQcm9wQ29uZmlnKGNvbnRleHQsIGluZGV4KTtcbiAgY29uc3QgZ3VhcmRNYXNrID0gbWFza1ZhbHVlIDw8IFRTdHlsaW5nQ29udGV4dFByb3BDb25maWdGbGFncy5Ub3RhbEJpdHM7XG4gIGNvbnRleHRbaW5kZXggKyBUU3R5bGluZ0NvbnRleHRJbmRleC5Db25maWdBbmRHdWFyZE9mZnNldF0gPSBjb25maWcgfCBndWFyZE1hc2s7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRWYWx1ZXNDb3VudChjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGluZGV4OiBudW1iZXIpIHtcbiAgcmV0dXJuIGNvbnRleHRbaW5kZXggKyBUU3R5bGluZ0NvbnRleHRJbmRleC5WYWx1ZXNDb3VudE9mZnNldF0gYXMgbnVtYmVyO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0QmluZGluZ1ZhbHVlKGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgaW5kZXg6IG51bWJlciwgb2Zmc2V0OiBudW1iZXIpIHtcbiAgcmV0dXJuIGNvbnRleHRbaW5kZXggKyBUU3R5bGluZ0NvbnRleHRJbmRleC5CaW5kaW5nc1N0YXJ0T2Zmc2V0ICsgb2Zmc2V0XSBhcyBudW1iZXIgfCBzdHJpbmc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWZhdWx0VmFsdWUoY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBpbmRleDogbnVtYmVyKTogc3RyaW5nfGJvb2xlYW58bnVsbCB7XG4gIGNvbnN0IHZhbHVlc0NvdW50ID0gZ2V0VmFsdWVzQ291bnQoY29udGV4dCwgaW5kZXgpO1xuICByZXR1cm4gY29udGV4dFtpbmRleCArIFRTdHlsaW5nQ29udGV4dEluZGV4LkJpbmRpbmdzU3RhcnRPZmZzZXQgKyB2YWx1ZXNDb3VudCAtIDFdIGFzIHN0cmluZyB8XG4gICAgICBib29sZWFuIHwgbnVsbDtcbn1cblxuLyoqXG4gKiBUZW1wb3JhcnkgZnVuY3Rpb24gd2hpY2ggZGV0ZXJtaW5lcyB3aGV0aGVyIG9yIG5vdCBhIGNvbnRleHQgaXNcbiAqIGFsbG93ZWQgdG8gYmUgZmx1c2hlZCBiYXNlZCBvbiB0aGUgcHJvdmlkZWQgZGlyZWN0aXZlIGluZGV4LlxuICovXG5leHBvcnQgZnVuY3Rpb24gYWxsb3dTdHlsaW5nRmx1c2goY29udGV4dDogVFN0eWxpbmdDb250ZXh0IHwgbnVsbCwgaW5kZXg6IG51bWJlcikge1xuICByZXR1cm4gKGNvbnRleHQgJiYgaW5kZXggPT09IGNvbnRleHRbVFN0eWxpbmdDb250ZXh0SW5kZXguTGFzdERpcmVjdGl2ZUluZGV4UG9zaXRpb25dKSA/IHRydWUgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGZhbHNlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbG9ja0NvbnRleHQoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KSB7XG4gIHNldENvbmZpZyhjb250ZXh0LCBnZXRDb25maWcoY29udGV4dCkgfCBUU3R5bGluZ0NvbmZpZ0ZsYWdzLkxvY2tlZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0NvbnRleHRMb2NrZWQoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KTogYm9vbGVhbiB7XG4gIHJldHVybiAoZ2V0Q29uZmlnKGNvbnRleHQpICYgVFN0eWxpbmdDb25maWdGbGFncy5Mb2NrZWQpID4gMDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0YXRlSXNQZXJzaXN0ZWQoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KTogYm9vbGVhbiB7XG4gIHJldHVybiAoZ2V0Q29uZmlnKGNvbnRleHQpICYgVFN0eWxpbmdDb25maWdGbGFncy5QZXJzaXN0U3RhdGVWYWx1ZXMpID4gMDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG1hcmtDb250ZXh0VG9QZXJzaXN0U3RhdGUoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KSB7XG4gIHNldENvbmZpZyhjb250ZXh0LCBnZXRDb25maWcoY29udGV4dCkgfCBUU3R5bGluZ0NvbmZpZ0ZsYWdzLlBlcnNpc3RTdGF0ZVZhbHVlcyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRQcm9wVmFsdWVzU3RhcnRQb3NpdGlvbihjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQpIHtcbiAgcmV0dXJuIFRTdHlsaW5nQ29udGV4dEluZGV4Lk1hcEJpbmRpbmdzQmluZGluZ3NTdGFydFBvc2l0aW9uICtcbiAgICAgIGNvbnRleHRbVFN0eWxpbmdDb250ZXh0SW5kZXguTWFwQmluZGluZ3NWYWx1ZXNDb3VudFBvc2l0aW9uXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzTWFwQmFzZWQocHJvcDogc3RyaW5nKSB7XG4gIHJldHVybiBwcm9wID09PSBNQVBfQkFTRURfRU5UUllfUFJPUF9OQU1FO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaGFzVmFsdWVDaGFuZ2VkKFxuICAgIGE6IFN0eWxpbmdNYXBBcnJheSB8IG51bWJlciB8IFN0cmluZyB8IHN0cmluZyB8IG51bGwgfCBib29sZWFuIHwgdW5kZWZpbmVkIHwge30sXG4gICAgYjogU3R5bGluZ01hcEFycmF5IHwgbnVtYmVyIHwgU3RyaW5nIHwgc3RyaW5nIHwgbnVsbCB8IGJvb2xlYW4gfCB1bmRlZmluZWQgfCB7fSk6IGJvb2xlYW4ge1xuICBsZXQgY29tcGFyZVZhbHVlQSA9IEFycmF5LmlzQXJyYXkoYSkgPyBhW1N0eWxpbmdNYXBBcnJheUluZGV4LlJhd1ZhbHVlUG9zaXRpb25dIDogYTtcbiAgbGV0IGNvbXBhcmVWYWx1ZUIgPSBBcnJheS5pc0FycmF5KGIpID8gYltTdHlsaW5nTWFwQXJyYXlJbmRleC5SYXdWYWx1ZVBvc2l0aW9uXSA6IGI7XG5cbiAgLy8gdGhlc2UgYXJlIHNwZWNpYWwgY2FzZXMgZm9yIFN0cmluZyBiYXNlZCB2YWx1ZXMgKHdoaWNoIGFyZSBjcmVhdGVkIGFzIGFydGlmYWN0c1xuICAvLyB3aGVuIHNhbml0aXphdGlvbiBpcyBieXBhc3NlZCBvbiBhIHBhcnRpY3VsYXIgdmFsdWUpXG4gIGlmIChjb21wYXJlVmFsdWVBIGluc3RhbmNlb2YgU3RyaW5nKSB7XG4gICAgY29tcGFyZVZhbHVlQSA9IGNvbXBhcmVWYWx1ZUEudG9TdHJpbmcoKTtcbiAgfVxuICBpZiAoY29tcGFyZVZhbHVlQiBpbnN0YW5jZW9mIFN0cmluZykge1xuICAgIGNvbXBhcmVWYWx1ZUIgPSBjb21wYXJlVmFsdWVCLnRvU3RyaW5nKCk7XG4gIH1cbiAgcmV0dXJuIGlzRGlmZmVyZW50KGNvbXBhcmVWYWx1ZUEsIGNvbXBhcmVWYWx1ZUIpO1xufVxuXG4vKipcbiAqIERldGVybWluZXMgd2hldGhlciB0aGUgcHJvdmlkZWQgc3R5bGluZyB2YWx1ZSBpcyB0cnV0aHkgb3IgZmFsc3kuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc1N0eWxpbmdWYWx1ZURlZmluZWQodmFsdWU6IGFueSkge1xuICAvLyB0aGUgcmVhc29uIHdoeSBudWxsIGlzIGNvbXBhcmVkIGFnYWluc3QgaXMgYmVjYXVzZVxuICAvLyBhIENTUyBjbGFzcyB2YWx1ZSB0aGF0IGlzIHNldCB0byBgZmFsc2VgIG11c3QgYmVcbiAgLy8gcmVzcGVjdGVkIChvdGhlcndpc2UgaXQgd291bGQgYmUgdHJlYXRlZCBhcyBmYWxzeSkuXG4gIC8vIEVtcHR5IHN0cmluZyB2YWx1ZXMgYXJlIGJlY2F1c2UgZGV2ZWxvcGVycyB1c3VhbGx5XG4gIC8vIHNldCBhIHZhbHVlIHRvIGFuIGVtcHR5IHN0cmluZyB0byByZW1vdmUgaXQuXG4gIHJldHVybiB2YWx1ZSAhPSBudWxsICYmIHZhbHVlICE9PSAnJztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNvbmNhdFN0cmluZyhhOiBzdHJpbmcsIGI6IHN0cmluZywgc2VwYXJhdG9yID0gJyAnKTogc3RyaW5nIHtcbiAgcmV0dXJuIGEgKyAoKGIubGVuZ3RoICYmIGEubGVuZ3RoKSA/IHNlcGFyYXRvciA6ICcnKSArIGI7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBoeXBoZW5hdGUodmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiB2YWx1ZS5yZXBsYWNlKC9bYS16XVtBLVpdL2csIHYgPT4gdi5jaGFyQXQoMCkgKyAnLScgKyB2LmNoYXJBdCgxKSkudG9Mb3dlckNhc2UoKTtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIGFuIGluc3RhbmNlIG9mIGBTdHlsaW5nTWFwQXJyYXlgLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgZGVzaWduZWQgdG8gZmluZCBhbiBpbnN0YW5jZSBvZiBgU3R5bGluZ01hcEFycmF5YCBpbiBjYXNlIGl0IGlzIHN0b3JlZFxuICogaW5zaWRlIG9mIGFuIGluc3RhbmNlIG9mIGBUU3R5bGluZ0NvbnRleHRgLiBXaGVuIGEgc3R5bGluZyBjb250ZXh0IGlzIGNyZWF0ZWQgaXRcbiAqIHdpbGwgY29weSBvdmVyIGFuIGluaXRpYWwgc3R5bGluZyB2YWx1ZXMgZnJvbSB0aGUgdE5vZGUgKHdoaWNoIGFyZSBzdG9yZWQgYXMgYVxuICogYFN0eWxpbmdNYXBBcnJheWAgb24gdGhlIGB0Tm9kZS5jbGFzc2VzYCBvciBgdE5vZGUuc3R5bGVzYCB2YWx1ZXMpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0U3R5bGluZ01hcEFycmF5KHZhbHVlOiBUU3R5bGluZ0NvbnRleHQgfCBTdHlsaW5nTWFwQXJyYXkgfCBudWxsKTpcbiAgICBTdHlsaW5nTWFwQXJyYXl8bnVsbCB7XG4gIHJldHVybiBpc1N0eWxpbmdDb250ZXh0KHZhbHVlKSA/XG4gICAgICAodmFsdWUgYXMgVFN0eWxpbmdDb250ZXh0KVtUU3R5bGluZ0NvbnRleHRJbmRleC5Jbml0aWFsU3R5bGluZ1ZhbHVlUG9zaXRpb25dIDpcbiAgICAgIHZhbHVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTdHlsaW5nQ29udGV4dCh2YWx1ZTogVFN0eWxpbmdDb250ZXh0IHwgU3R5bGluZ01hcEFycmF5IHwgbnVsbCk6IGJvb2xlYW4ge1xuICAvLyB0aGUgU3R5bGluZ01hcEFycmF5IGlzIGluIHRoZSBmb3JtYXQgb2YgW2luaXRpYWwsIHByb3AsIHN0cmluZywgcHJvcCwgc3RyaW5nXVxuICAvLyBhbmQgdGhpcyBpcyB0aGUgZGVmaW5pbmcgdmFsdWUgdG8gZGlzdGluZ3Vpc2ggYmV0d2VlbiBhcnJheXNcbiAgcmV0dXJuIEFycmF5LmlzQXJyYXkodmFsdWUpICYmXG4gICAgICB2YWx1ZS5sZW5ndGggPj0gVFN0eWxpbmdDb250ZXh0SW5kZXguTWFwQmluZGluZ3NCaW5kaW5nc1N0YXJ0UG9zaXRpb24gJiZcbiAgICAgIHR5cGVvZiB2YWx1ZVsxXSAhPT0gJ3N0cmluZyc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRJbml0aWFsU3R5bGluZ1ZhbHVlKGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCB8IFN0eWxpbmdNYXBBcnJheSB8IG51bGwpOiBzdHJpbmcge1xuICBjb25zdCBtYXAgPSBnZXRTdHlsaW5nTWFwQXJyYXkoY29udGV4dCk7XG4gIHJldHVybiBtYXAgJiYgKG1hcFtTdHlsaW5nTWFwQXJyYXlJbmRleC5SYXdWYWx1ZVBvc2l0aW9uXSBhcyBzdHJpbmcgfCBudWxsKSB8fCAnJztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGhhc0NsYXNzSW5wdXQodE5vZGU6IFROb2RlKSB7XG4gIHJldHVybiAodE5vZGUuZmxhZ3MgJiBUTm9kZUZsYWdzLmhhc0NsYXNzSW5wdXQpICE9PSAwO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaGFzU3R5bGVJbnB1dCh0Tm9kZTogVE5vZGUpIHtcbiAgcmV0dXJuICh0Tm9kZS5mbGFncyAmIFROb2RlRmxhZ3MuaGFzU3R5bGVJbnB1dCkgIT09IDA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNYXBQcm9wKG1hcDogU3R5bGluZ01hcEFycmF5LCBpbmRleDogbnVtYmVyKTogc3RyaW5nIHtcbiAgcmV0dXJuIG1hcFtpbmRleCArIFN0eWxpbmdNYXBBcnJheUluZGV4LlByb3BPZmZzZXRdIGFzIHN0cmluZztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNldE1hcFZhbHVlKFxuICAgIG1hcDogU3R5bGluZ01hcEFycmF5LCBpbmRleDogbnVtYmVyLCB2YWx1ZTogc3RyaW5nIHwgYm9vbGVhbiB8IG51bGwpOiB2b2lkIHtcbiAgbWFwW2luZGV4ICsgU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVPZmZzZXRdID0gdmFsdWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNYXBWYWx1ZShtYXA6IFN0eWxpbmdNYXBBcnJheSwgaW5kZXg6IG51bWJlcik6IHN0cmluZ3xudWxsIHtcbiAgcmV0dXJuIG1hcFtpbmRleCArIFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlT2Zmc2V0XSBhcyBzdHJpbmcgfCBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9yY2VDbGFzc2VzQXNTdHJpbmcoY2xhc3Nlczogc3RyaW5nIHwge1trZXk6IHN0cmluZ106IGFueX0gfCBudWxsIHwgdW5kZWZpbmVkKTpcbiAgICBzdHJpbmcge1xuICBpZiAoY2xhc3NlcyAmJiB0eXBlb2YgY2xhc3NlcyAhPT0gJ3N0cmluZycpIHtcbiAgICBjbGFzc2VzID0gT2JqZWN0LmtleXMoY2xhc3Nlcykuam9pbignICcpO1xuICB9XG4gIHJldHVybiAoY2xhc3NlcyBhcyBzdHJpbmcpIHx8ICcnO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9yY2VTdHlsZXNBc1N0cmluZyhzdHlsZXM6IHtba2V5OiBzdHJpbmddOiBhbnl9IHwgbnVsbCB8IHVuZGVmaW5lZCk6IHN0cmluZyB7XG4gIGxldCBzdHIgPSAnJztcbiAgaWYgKHN0eWxlcykge1xuICAgIGNvbnN0IHByb3BzID0gT2JqZWN0LmtleXMoc3R5bGVzKTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHByb3BzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBwcm9wID0gcHJvcHNbaV07XG4gICAgICBzdHIgPSBjb25jYXRTdHJpbmcoc3RyLCBgJHtwcm9wfToke3N0eWxlc1twcm9wXX1gLCAnOycpO1xuICAgIH1cbiAgfVxuICByZXR1cm4gc3RyO1xufVxuIl19