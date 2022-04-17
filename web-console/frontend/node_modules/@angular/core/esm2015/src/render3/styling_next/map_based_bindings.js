/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { setStylingMapsSyncFn } from './bindings';
import { concatString, getBindingValue, getMapProp, getMapValue, getValuesCount, hyphenate, isStylingValueDefined, setMapValue } from './util';
/**
 * Used to apply styling values presently within any map-based bindings on an element.
 *
 * Angular supports map-based styling bindings which can be applied via the
 * `[style]` and `[class]` bindings which can be placed on any HTML element.
 * These bindings can work independently, together or alongside prop-based
 * styling bindings (e.g. `<div [style]="x" [style.width]="w">`).
 *
 * If a map-based styling binding is detected by the compiler, the following
 * AOT code is produced:
 *
 * ```typescript
 * styleMap(ctx.styles); // styles = {key:value}
 * classMap(ctx.classes); // classes = {key:value}|string
 * ```
 *
 * If and when either of the instructions above are evaluated, then the code
 * present in this file is included into the bundle. The mechanism used, to
 * activate support for map-based bindings at runtime is possible via the
 * `activeStylingMapFeature` function (which is also present in this file).
 *
 * # The Algorithm
 * Whenever a map-based binding updates (which is when the identity of the
 * map-value changes) then the map is iterated over and a `StylingMapArray` array
 * is produced. The `StylingMapArray` instance is stored in the binding location
 * where the `BINDING_INDEX` is situated when the `styleMap()` or `classMap()`
 * instruction were called. Once the binding changes, then the internal `bitMask`
 * value is marked as dirty.
 *
 * Styling values are applied once CD exits the element (which happens when
 * the `select(n)` instruction is called or the template function exits). When
 * this occurs, all prop-based bindings are applied. If a map-based binding is
 * present then a special flushing function (called a sync function) is made
 * available and it will be called each time a styling property is flushed.
 *
 * The flushing algorithm is designed to apply styling for a property (which is
 * a CSS property or a className value) one by one. If map-based bindings
 * are present, then the flushing algorithm will keep calling the maps styling
 * sync function each time a property is visited. This way, the flushing
 * behavior of map-based bindings will always be at the same property level
 * as the current prop-based property being iterated over (because everything
 * is alphabetically sorted).
 *
 * Let's imagine we have the following HTML template code:
 *
 * ```html
 * <div [style]="{width:'100px', height:'200px', 'z-index':'10'}"
 *      [style.width.px]="200">...</div>
 * ```
 *
 * When CD occurs, both the `[style]` and `[style.width]` bindings
 * are evaluated. Then when the styles are flushed on screen, the
 * following operations happen:
 *
 * 1. `[style.width]` is attempted to be written to the element.
 *
 * 2.  Once that happens, the algorithm instructs the map-based
 *     entries (`[style]` in this case) to "catch up" and apply
 *     all values up to the `width` value. When this happens the
 *     `height` value is applied to the element (since it is
 *     alphabetically situated before the `width` property).
 *
 * 3. Since there are no more prop-based entries anymore, the
 *    loop exits and then, just before the flushing ends, it
 *    instructs all map-based bindings to "finish up" applying
 *    their values.
 *
 * 4. The only remaining value within the map-based entries is
 *    the `z-index` value (`width` got skipped because it was
 *    successfully applied via the prop-based `[style.width]`
 *    binding). Since all map-based entries are told to "finish up",
 *    the `z-index` value is iterated over and it is then applied
 *    to the element.
 *
 * The most important thing to take note of here is that prop-based
 * bindings are evaluated in order alongside map-based bindings.
 * This allows all styling across an element to be applied in O(n)
 * time (a similar algorithm is that of the array merge algorithm
 * in merge sort).
 * @type {?}
 */
export const syncStylingMap = (/**
 * @param {?} context
 * @param {?} renderer
 * @param {?} element
 * @param {?} data
 * @param {?} applyStylingFn
 * @param {?} sanitizer
 * @param {?} mode
 * @param {?=} targetProp
 * @param {?=} defaultValue
 * @return {?}
 */
(context, renderer, element, data, applyStylingFn, sanitizer, mode, targetProp, defaultValue) => {
    /** @type {?} */
    let targetPropValueWasApplied = false;
    // once the map-based styling code is activate it is never deactivated. For this reason a
    // check to see if the current styling context has any map based bindings is required.
    /** @type {?} */
    const totalMaps = getValuesCount(context, 3 /* MapBindingsPosition */);
    if (totalMaps) {
        /** @type {?} */
        let runTheSyncAlgorithm = true;
        /** @type {?} */
        const loopUntilEnd = !targetProp;
        // If the code is told to finish up (run until the end), but the mode
        // hasn't been flagged to apply values (it only traverses values) then
        // there is no point in iterating over the array because nothing will
        // be applied to the element.
        if (loopUntilEnd && (mode & ~1 /* ApplyAllValues */)) {
            runTheSyncAlgorithm = false;
            targetPropValueWasApplied = true;
        }
        if (runTheSyncAlgorithm) {
            targetPropValueWasApplied = innerSyncStylingMap(context, renderer, element, data, applyStylingFn, sanitizer, mode, targetProp || null, 0, defaultValue || null);
        }
        if (loopUntilEnd) {
            resetSyncCursors();
        }
    }
    return targetPropValueWasApplied;
});
/**
 * Recursive function designed to apply map-based styling to an element one map at a time.
 *
 * This function is designed to be called from the `syncStylingMap` function and will
 * apply map-based styling data one map at a time to the provided `element`.
 *
 * This function is recursive and it will call itself if a follow-up map value is to be
 * processed. To learn more about how the algorithm works, see `syncStylingMap`.
 * @param {?} context
 * @param {?} renderer
 * @param {?} element
 * @param {?} data
 * @param {?} applyStylingFn
 * @param {?} sanitizer
 * @param {?} mode
 * @param {?} targetProp
 * @param {?} currentMapIndex
 * @param {?} defaultValue
 * @return {?}
 */
function innerSyncStylingMap(context, renderer, element, data, applyStylingFn, sanitizer, mode, targetProp, currentMapIndex, defaultValue) {
    /** @type {?} */
    let targetPropValueWasApplied = false;
    /** @type {?} */
    const totalMaps = getValuesCount(context, 3 /* MapBindingsPosition */);
    if (currentMapIndex < totalMaps) {
        /** @type {?} */
        const bindingIndex = (/** @type {?} */ (getBindingValue(context, 3 /* MapBindingsPosition */, currentMapIndex)));
        /** @type {?} */
        const stylingMapArr = (/** @type {?} */ (data[bindingIndex]));
        /** @type {?} */
        let cursor = getCurrentSyncCursor(currentMapIndex);
        while (cursor < stylingMapArr.length) {
            /** @type {?} */
            const prop = getMapProp(stylingMapArr, cursor);
            /** @type {?} */
            const iteratedTooFar = targetProp && prop > targetProp;
            /** @type {?} */
            const isTargetPropMatched = !iteratedTooFar && prop === targetProp;
            /** @type {?} */
            const value = getMapValue(stylingMapArr, cursor);
            /** @type {?} */
            const valueIsDefined = isStylingValueDefined(value);
            // the recursive code is designed to keep applying until
            // it reaches or goes past the target prop. If and when
            // this happens then it will stop processing values, but
            // all other map values must also catch up to the same
            // point. This is why a recursive call is still issued
            // even if the code has iterated too far.
            /** @type {?} */
            const innerMode = iteratedTooFar ? mode : resolveInnerMapMode(mode, valueIsDefined, isTargetPropMatched);
            /** @type {?} */
            const innerProp = iteratedTooFar ? targetProp : prop;
            /** @type {?} */
            let valueApplied = innerSyncStylingMap(context, renderer, element, data, applyStylingFn, sanitizer, innerMode, innerProp, currentMapIndex + 1, defaultValue);
            if (iteratedTooFar) {
                if (!targetPropValueWasApplied) {
                    targetPropValueWasApplied = valueApplied;
                }
                break;
            }
            if (!valueApplied && isValueAllowedToBeApplied(mode, isTargetPropMatched)) {
                /** @type {?} */
                const useDefault = isTargetPropMatched && !valueIsDefined;
                /** @type {?} */
                const valueToApply = useDefault ? defaultValue : value;
                /** @type {?} */
                const bindingIndexToApply = useDefault ? bindingIndex : null;
                /** @type {?} */
                const finalValue = sanitizer ?
                    sanitizer(prop, valueToApply, 3 /* ValidateAndSanitize */) :
                    valueToApply;
                applyStylingFn(renderer, element, prop, finalValue, bindingIndexToApply);
                valueApplied = true;
            }
            targetPropValueWasApplied = valueApplied && isTargetPropMatched;
            cursor += 2 /* TupleSize */;
        }
        setCurrentSyncCursor(currentMapIndex, cursor);
        // this is a fallback case in the event that the styling map is `null` for this
        // binding but there are other map-based bindings that need to be evaluated
        // afterwards. If the `prop` value is falsy then the intention is to cycle
        // through all of the properties in the remaining maps as well. If the current
        // styling map is too short then there are no values to iterate over. In either
        // case the follow-up maps need to be iterated over.
        if (stylingMapArr.length === 1 /* ValuesStartPosition */ || !targetProp) {
            return innerSyncStylingMap(context, renderer, element, data, applyStylingFn, sanitizer, mode, targetProp, currentMapIndex + 1, defaultValue);
        }
    }
    return targetPropValueWasApplied;
}
/**
 * Enables support for map-based styling bindings (e.g. `[style]` and `[class]` bindings).
 * @return {?}
 */
export function activateStylingMapFeature() {
    setStylingMapsSyncFn(syncStylingMap);
}
/**
 * Used to determine the mode for the inner recursive call.
 *
 * If an inner map is iterated on then this is done so for one
 * of two reasons:
 *
 * - value is being applied:
 *   if the value is being applied from this current styling
 *   map then there is no need to apply it in a deeper map.
 *
 * - value is being not applied:
 *   apply the value if it is found in a deeper map.
 *
 * When these reasons are encountered the flags will for the
 * inner map mode will be configured.
 * @param {?} currentMode
 * @param {?} valueIsDefined
 * @param {?} isExactMatch
 * @return {?}
 */
function resolveInnerMapMode(currentMode, valueIsDefined, isExactMatch) {
    /** @type {?} */
    let innerMode = currentMode;
    if (!valueIsDefined && !(currentMode & 4 /* SkipTargetProp */) &&
        (isExactMatch || (currentMode & 1 /* ApplyAllValues */))) {
        // case 1: set the mode to apply the targeted prop value if it
        // ends up being encountered in another map value
        innerMode |= 2 /* ApplyTargetProp */;
        innerMode &= ~4 /* SkipTargetProp */;
    }
    else {
        // case 2: set the mode to skip the targeted prop value if it
        // ends up being encountered in another map value
        innerMode |= 4 /* SkipTargetProp */;
        innerMode &= ~2 /* ApplyTargetProp */;
    }
    return innerMode;
}
/**
 * Decides whether or not a prop/value entry will be applied to an element.
 *
 * To determine whether or not a value is to be applied,
 * the following procedure is evaluated:
 *
 * First check to see the current `mode` status:
 *  1. If the mode value permits all props to be applied then allow.
 *    - But do not allow if the current prop is set to be skipped.
 *  2. Otherwise if the current prop is permitted then allow.
 * @param {?} mode
 * @param {?} isTargetPropMatched
 * @return {?}
 */
function isValueAllowedToBeApplied(mode, isTargetPropMatched) {
    /** @type {?} */
    let doApplyValue = (mode & 1 /* ApplyAllValues */) > 0;
    if (!doApplyValue) {
        if (mode & 2 /* ApplyTargetProp */) {
            doApplyValue = isTargetPropMatched;
        }
    }
    else if ((mode & 4 /* SkipTargetProp */) && isTargetPropMatched) {
        doApplyValue = false;
    }
    return doApplyValue;
}
/**
 * Used to keep track of concurrent cursor values for multiple map-based styling bindings present on
 * an element.
 * @type {?}
 */
const MAP_CURSORS = [];
/**
 * Used to reset the state of each cursor value being used to iterate over map-based styling
 * bindings.
 * @return {?}
 */
function resetSyncCursors() {
    for (let i = 0; i < MAP_CURSORS.length; i++) {
        MAP_CURSORS[i] = 1 /* ValuesStartPosition */;
    }
}
/**
 * Returns an active cursor value at a given mapIndex location.
 * @param {?} mapIndex
 * @return {?}
 */
function getCurrentSyncCursor(mapIndex) {
    if (mapIndex >= MAP_CURSORS.length) {
        MAP_CURSORS.push(1 /* ValuesStartPosition */);
    }
    return MAP_CURSORS[mapIndex];
}
/**
 * Sets a cursor value at a given mapIndex location.
 * @param {?} mapIndex
 * @param {?} indexValue
 * @return {?}
 */
function setCurrentSyncCursor(mapIndex, indexValue) {
    MAP_CURSORS[mapIndex] = indexValue;
}
/**
 * Used to convert a {key:value} map into a `StylingMapArray` array.
 *
 * This function will either generate a new `StylingMapArray` instance
 * or it will patch the provided `newValues` map value into an
 * existing `StylingMapArray` value (this only happens if `bindingValue`
 * is an instance of `StylingMapArray`).
 *
 * If a new key/value map is provided with an old `StylingMapArray`
 * value then all properties will be overwritten with their new
 * values or with `null`. This means that the array will never
 * shrink in size (but it will also not be created and thrown
 * away whenever the {key:value} map entries change).
 * @param {?} bindingValue
 * @param {?} newValues
 * @param {?=} normalizeProps
 * @return {?}
 */
export function normalizeIntoStylingMap(bindingValue, newValues, normalizeProps) {
    /** @type {?} */
    const stylingMapArr = Array.isArray(bindingValue) ? bindingValue : [null];
    stylingMapArr[0 /* RawValuePosition */] = newValues || null;
    // because the new values may not include all the properties
    // that the old ones had, all values are set to `null` before
    // the new values are applied. This way, when flushed, the
    // styling algorithm knows exactly what style/class values
    // to remove from the element (since they are `null`).
    for (let j = 1 /* ValuesStartPosition */; j < stylingMapArr.length; j += 2 /* TupleSize */) {
        setMapValue(stylingMapArr, j, null);
    }
    /** @type {?} */
    let props = null;
    /** @type {?} */
    let map;
    /** @type {?} */
    let allValuesTrue = false;
    if (typeof newValues === 'string') { // [class] bindings allow string values
        if (newValues.length) {
            props = newValues.split(/\s+/);
            allValuesTrue = true;
        }
    }
    else {
        props = newValues ? Object.keys(newValues) : null;
        map = newValues;
    }
    if (props) {
        for (let i = 0; i < props.length; i++) {
            /** @type {?} */
            const prop = (/** @type {?} */ (props[i]));
            /** @type {?} */
            const newProp = normalizeProps ? hyphenate(prop) : prop;
            /** @type {?} */
            const value = allValuesTrue ? true : (/** @type {?} */ (map))[prop];
            addItemToStylingMap(stylingMapArr, newProp, value, true);
        }
    }
    return stylingMapArr;
}
/**
 * Inserts the provided item into the provided styling array at the right spot.
 *
 * The `StylingMapArray` type is a sorted key/value array of entries. This means
 * that when a new entry is inserted it must be placed at the right spot in the
 * array. This function figures out exactly where to place it.
 * @param {?} stylingMapArr
 * @param {?} prop
 * @param {?} value
 * @param {?=} allowOverwrite
 * @return {?}
 */
export function addItemToStylingMap(stylingMapArr, prop, value, allowOverwrite) {
    for (let j = 1 /* ValuesStartPosition */; j < stylingMapArr.length; j += 2 /* TupleSize */) {
        /** @type {?} */
        const propAtIndex = getMapProp(stylingMapArr, j);
        if (prop <= propAtIndex) {
            /** @type {?} */
            let applied = false;
            if (propAtIndex === prop) {
                /** @type {?} */
                const valueAtIndex = stylingMapArr[j];
                if (allowOverwrite || !isStylingValueDefined(valueAtIndex)) {
                    applied = true;
                    setMapValue(stylingMapArr, j, value);
                }
            }
            else {
                applied = true;
                stylingMapArr.splice(j, 0, prop, value);
            }
            return applied;
        }
    }
    stylingMapArr.push(prop, value);
    return true;
}
/**
 * Converts the provided styling map array into a string.
 *
 * Classes => `one two three`
 * Styles => `prop:value; prop2:value2`
 * @param {?} map
 * @param {?} isClassBased
 * @return {?}
 */
export function stylingMapToString(map, isClassBased) {
    /** @type {?} */
    let str = '';
    for (let i = 1 /* ValuesStartPosition */; i < map.length; i += 2 /* TupleSize */) {
        /** @type {?} */
        const prop = getMapProp(map, i);
        /** @type {?} */
        const value = (/** @type {?} */ (getMapValue(map, i)));
        /** @type {?} */
        const attrValue = concatString(prop, isClassBased ? '' : value, ':');
        str = concatString(str, attrValue, isClassBased ? ' ' : '; ');
    }
    return str;
}
/**
 * Converts the provided styling map array into a key value map.
 * @param {?} map
 * @return {?}
 */
export function stylingMapToStringMap(map) {
    /** @type {?} */
    let stringMap = {};
    if (map) {
        for (let i = 1 /* ValuesStartPosition */; i < map.length; i += 2 /* TupleSize */) {
            /** @type {?} */
            const prop = getMapProp(map, i);
            /** @type {?} */
            const value = (/** @type {?} */ (getMapValue(map, i)));
            stringMap[prop] = value;
        }
    }
    return stringMap;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFwX2Jhc2VkX2JpbmRpbmdzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9zdHlsaW5nX25leHQvbWFwX2Jhc2VkX2JpbmRpbmdzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFVQSxPQUFPLEVBQUMsb0JBQW9CLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFFaEQsT0FBTyxFQUFDLFlBQVksRUFBRSxlQUFlLEVBQUUsVUFBVSxFQUFFLFdBQVcsRUFBRSxjQUFjLEVBQUUsU0FBUyxFQUFFLHFCQUFxQixFQUFFLFdBQVcsRUFBQyxNQUFNLFFBQVEsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQTZGN0ksTUFBTSxPQUFPLGNBQWM7Ozs7Ozs7Ozs7OztBQUN2QixDQUFDLE9BQXdCLEVBQUUsUUFBZ0QsRUFBRSxPQUFpQixFQUM3RixJQUFrQixFQUFFLGNBQThCLEVBQUUsU0FBaUMsRUFDckYsSUFBeUIsRUFBRSxVQUEwQixFQUNyRCxZQUE0QixFQUFXLEVBQUU7O1FBQ3BDLHlCQUF5QixHQUFHLEtBQUs7Ozs7VUFJL0IsU0FBUyxHQUFHLGNBQWMsQ0FBQyxPQUFPLDhCQUEyQztJQUNuRixJQUFJLFNBQVMsRUFBRTs7WUFDVCxtQkFBbUIsR0FBRyxJQUFJOztjQUN4QixZQUFZLEdBQUcsQ0FBQyxVQUFVO1FBRWhDLHFFQUFxRTtRQUNyRSxzRUFBc0U7UUFDdEUscUVBQXFFO1FBQ3JFLDZCQUE2QjtRQUM3QixJQUFJLFlBQVksSUFBSSxDQUFDLElBQUksR0FBRyx1QkFBbUMsQ0FBQyxFQUFFO1lBQ2hFLG1CQUFtQixHQUFHLEtBQUssQ0FBQztZQUM1Qix5QkFBeUIsR0FBRyxJQUFJLENBQUM7U0FDbEM7UUFFRCxJQUFJLG1CQUFtQixFQUFFO1lB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RSxDQUFDLEdBQUcsR0FBRyxDQUFDLE1BQU0sRUFDaEUsQ0FBQyxxQkFBa0MsRUFBRTs7Y0FDbEMsSUFBSSxHQUFHLFVBQVUsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDOztjQUN6QixLQUFLLEdBQUcsbUJBQUEsV0FBVyxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQUMsRUFBVTs7Y0FDckMsU0FBUyxHQUFHLFlBQVksQ0FBQyxJQUFJLEVBQUUsWUFBWSxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUM7UUFDcEUsR0FBRyxHQUFHLFlBQVksQ0FBQyxHQUFHLEVBQUUsU0FBUyxFQUFFLFlBQVksQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQztLQUMvRDtJQUNELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQzs7Ozs7O0FBS0QsTUFBTSxVQUFVLHFCQUFxQixDQUFDLEdBQTJCOztRQUMzRCxTQUFTLEdBQXlCLEVBQUU7SUFDeEMsSUFBSSxHQUFHLEVBQUU7UUFDUCxLQUFLLElBQUksQ0FBQyw4QkFBMkMsRUFBRSxDQUFDLEdBQUcsR0FBRyxDQUFDLE1BQU0sRUFDaEUsQ0FBQyxxQkFBa0MsRUFBRTs7a0JBQ2xDLElBQUksR0FBRyxVQUFVLENBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQzs7a0JBQ3pCLEtBQUssR0FBRyxtQkFBQSxXQUFXLENBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFVO1lBQzNDLFNBQVMsQ0FBQyxJQUFJLENBQUMsR0FBRyxLQUFLLENBQUM7U0FDekI7S0FDRjtJQUNELE9BQU8sU0FBUyxDQUFDO0FBQ25CLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiogQGxpY2Vuc2VcbiogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4qXG4qIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4qIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiovXG5pbXBvcnQge1N0eWxlU2FuaXRpemVGbiwgU3R5bGVTYW5pdGl6ZU1vZGV9IGZyb20gJy4uLy4uL3Nhbml0aXphdGlvbi9zdHlsZV9zYW5pdGl6ZXInO1xuaW1wb3J0IHtQcm9jZWR1cmFsUmVuZGVyZXIzLCBSRWxlbWVudCwgUmVuZGVyZXIzfSBmcm9tICcuLi9pbnRlcmZhY2VzL3JlbmRlcmVyJztcblxuaW1wb3J0IHtzZXRTdHlsaW5nTWFwc1N5bmNGbn0gZnJvbSAnLi9iaW5kaW5ncyc7XG5pbXBvcnQge0FwcGx5U3R5bGluZ0ZuLCBMU3R5bGluZ0RhdGEsIFN0eWxpbmdNYXBBcnJheSwgU3R5bGluZ01hcEFycmF5SW5kZXgsIFN0eWxpbmdNYXBzU3luY01vZGUsIFN5bmNTdHlsaW5nTWFwc0ZuLCBUU3R5bGluZ0NvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4fSBmcm9tICcuL2ludGVyZmFjZXMnO1xuaW1wb3J0IHtjb25jYXRTdHJpbmcsIGdldEJpbmRpbmdWYWx1ZSwgZ2V0TWFwUHJvcCwgZ2V0TWFwVmFsdWUsIGdldFZhbHVlc0NvdW50LCBoeXBoZW5hdGUsIGlzU3R5bGluZ1ZhbHVlRGVmaW5lZCwgc2V0TWFwVmFsdWV9IGZyb20gJy4vdXRpbCc7XG5cblxuXG4vKipcbiAqIC0tLS0tLS0tXG4gKlxuICogVGhpcyBmaWxlIGNvbnRhaW5zIHRoZSBhbGdvcml0aG0gbG9naWMgZm9yIGFwcGx5aW5nIG1hcC1iYXNlZCBiaW5kaW5nc1xuICogc3VjaCBhcyBgW3N0eWxlXWAgYW5kIGBbY2xhc3NdYC5cbiAqXG4gKiAtLS0tLS0tLVxuICovXG5cbi8qKlxuICogVXNlZCB0byBhcHBseSBzdHlsaW5nIHZhbHVlcyBwcmVzZW50bHkgd2l0aGluIGFueSBtYXAtYmFzZWQgYmluZGluZ3Mgb24gYW4gZWxlbWVudC5cbiAqXG4gKiBBbmd1bGFyIHN1cHBvcnRzIG1hcC1iYXNlZCBzdHlsaW5nIGJpbmRpbmdzIHdoaWNoIGNhbiBiZSBhcHBsaWVkIHZpYSB0aGVcbiAqIGBbc3R5bGVdYCBhbmQgYFtjbGFzc11gIGJpbmRpbmdzIHdoaWNoIGNhbiBiZSBwbGFjZWQgb24gYW55IEhUTUwgZWxlbWVudC5cbiAqIFRoZXNlIGJpbmRpbmdzIGNhbiB3b3JrIGluZGVwZW5kZW50bHksIHRvZ2V0aGVyIG9yIGFsb25nc2lkZSBwcm9wLWJhc2VkXG4gKiBzdHlsaW5nIGJpbmRpbmdzIChlLmcuIGA8ZGl2IFtzdHlsZV09XCJ4XCIgW3N0eWxlLndpZHRoXT1cIndcIj5gKS5cbiAqXG4gKiBJZiBhIG1hcC1iYXNlZCBzdHlsaW5nIGJpbmRpbmcgaXMgZGV0ZWN0ZWQgYnkgdGhlIGNvbXBpbGVyLCB0aGUgZm9sbG93aW5nXG4gKiBBT1QgY29kZSBpcyBwcm9kdWNlZDpcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBzdHlsZU1hcChjdHguc3R5bGVzKTsgLy8gc3R5bGVzID0ge2tleTp2YWx1ZX1cbiAqIGNsYXNzTWFwKGN0eC5jbGFzc2VzKTsgLy8gY2xhc3NlcyA9IHtrZXk6dmFsdWV9fHN0cmluZ1xuICogYGBgXG4gKlxuICogSWYgYW5kIHdoZW4gZWl0aGVyIG9mIHRoZSBpbnN0cnVjdGlvbnMgYWJvdmUgYXJlIGV2YWx1YXRlZCwgdGhlbiB0aGUgY29kZVxuICogcHJlc2VudCBpbiB0aGlzIGZpbGUgaXMgaW5jbHVkZWQgaW50byB0aGUgYnVuZGxlLiBUaGUgbWVjaGFuaXNtIHVzZWQsIHRvXG4gKiBhY3RpdmF0ZSBzdXBwb3J0IGZvciBtYXAtYmFzZWQgYmluZGluZ3MgYXQgcnVudGltZSBpcyBwb3NzaWJsZSB2aWEgdGhlXG4gKiBgYWN0aXZlU3R5bGluZ01hcEZlYXR1cmVgIGZ1bmN0aW9uICh3aGljaCBpcyBhbHNvIHByZXNlbnQgaW4gdGhpcyBmaWxlKS5cbiAqXG4gKiAjIFRoZSBBbGdvcml0aG1cbiAqIFdoZW5ldmVyIGEgbWFwLWJhc2VkIGJpbmRpbmcgdXBkYXRlcyAod2hpY2ggaXMgd2hlbiB0aGUgaWRlbnRpdHkgb2YgdGhlXG4gKiBtYXAtdmFsdWUgY2hhbmdlcykgdGhlbiB0aGUgbWFwIGlzIGl0ZXJhdGVkIG92ZXIgYW5kIGEgYFN0eWxpbmdNYXBBcnJheWAgYXJyYXlcbiAqIGlzIHByb2R1Y2VkLiBUaGUgYFN0eWxpbmdNYXBBcnJheWAgaW5zdGFuY2UgaXMgc3RvcmVkIGluIHRoZSBiaW5kaW5nIGxvY2F0aW9uXG4gKiB3aGVyZSB0aGUgYEJJTkRJTkdfSU5ERVhgIGlzIHNpdHVhdGVkIHdoZW4gdGhlIGBzdHlsZU1hcCgpYCBvciBgY2xhc3NNYXAoKWBcbiAqIGluc3RydWN0aW9uIHdlcmUgY2FsbGVkLiBPbmNlIHRoZSBiaW5kaW5nIGNoYW5nZXMsIHRoZW4gdGhlIGludGVybmFsIGBiaXRNYXNrYFxuICogdmFsdWUgaXMgbWFya2VkIGFzIGRpcnR5LlxuICpcbiAqIFN0eWxpbmcgdmFsdWVzIGFyZSBhcHBsaWVkIG9uY2UgQ0QgZXhpdHMgdGhlIGVsZW1lbnQgKHdoaWNoIGhhcHBlbnMgd2hlblxuICogdGhlIGBzZWxlY3QobilgIGluc3RydWN0aW9uIGlzIGNhbGxlZCBvciB0aGUgdGVtcGxhdGUgZnVuY3Rpb24gZXhpdHMpLiBXaGVuXG4gKiB0aGlzIG9jY3VycywgYWxsIHByb3AtYmFzZWQgYmluZGluZ3MgYXJlIGFwcGxpZWQuIElmIGEgbWFwLWJhc2VkIGJpbmRpbmcgaXNcbiAqIHByZXNlbnQgdGhlbiBhIHNwZWNpYWwgZmx1c2hpbmcgZnVuY3Rpb24gKGNhbGxlZCBhIHN5bmMgZnVuY3Rpb24pIGlzIG1hZGVcbiAqIGF2YWlsYWJsZSBhbmQgaXQgd2lsbCBiZSBjYWxsZWQgZWFjaCB0aW1lIGEgc3R5bGluZyBwcm9wZXJ0eSBpcyBmbHVzaGVkLlxuICpcbiAqIFRoZSBmbHVzaGluZyBhbGdvcml0aG0gaXMgZGVzaWduZWQgdG8gYXBwbHkgc3R5bGluZyBmb3IgYSBwcm9wZXJ0eSAod2hpY2ggaXNcbiAqIGEgQ1NTIHByb3BlcnR5IG9yIGEgY2xhc3NOYW1lIHZhbHVlKSBvbmUgYnkgb25lLiBJZiBtYXAtYmFzZWQgYmluZGluZ3NcbiAqIGFyZSBwcmVzZW50LCB0aGVuIHRoZSBmbHVzaGluZyBhbGdvcml0aG0gd2lsbCBrZWVwIGNhbGxpbmcgdGhlIG1hcHMgc3R5bGluZ1xuICogc3luYyBmdW5jdGlvbiBlYWNoIHRpbWUgYSBwcm9wZXJ0eSBpcyB2aXNpdGVkLiBUaGlzIHdheSwgdGhlIGZsdXNoaW5nXG4gKiBiZWhhdmlvciBvZiBtYXAtYmFzZWQgYmluZGluZ3Mgd2lsbCBhbHdheXMgYmUgYXQgdGhlIHNhbWUgcHJvcGVydHkgbGV2ZWxcbiAqIGFzIHRoZSBjdXJyZW50IHByb3AtYmFzZWQgcHJvcGVydHkgYmVpbmcgaXRlcmF0ZWQgb3ZlciAoYmVjYXVzZSBldmVyeXRoaW5nXG4gKiBpcyBhbHBoYWJldGljYWxseSBzb3J0ZWQpLlxuICpcbiAqIExldCdzIGltYWdpbmUgd2UgaGF2ZSB0aGUgZm9sbG93aW5nIEhUTUwgdGVtcGxhdGUgY29kZTpcbiAqXG4gKiBgYGBodG1sXG4gKiA8ZGl2IFtzdHlsZV09XCJ7d2lkdGg6JzEwMHB4JywgaGVpZ2h0OicyMDBweCcsICd6LWluZGV4JzonMTAnfVwiXG4gKiAgICAgIFtzdHlsZS53aWR0aC5weF09XCIyMDBcIj4uLi48L2Rpdj5cbiAqIGBgYFxuICpcbiAqIFdoZW4gQ0Qgb2NjdXJzLCBib3RoIHRoZSBgW3N0eWxlXWAgYW5kIGBbc3R5bGUud2lkdGhdYCBiaW5kaW5nc1xuICogYXJlIGV2YWx1YXRlZC4gVGhlbiB3aGVuIHRoZSBzdHlsZXMgYXJlIGZsdXNoZWQgb24gc2NyZWVuLCB0aGVcbiAqIGZvbGxvd2luZyBvcGVyYXRpb25zIGhhcHBlbjpcbiAqXG4gKiAxLiBgW3N0eWxlLndpZHRoXWAgaXMgYXR0ZW1wdGVkIHRvIGJlIHdyaXR0ZW4gdG8gdGhlIGVsZW1lbnQuXG4gKlxuICogMi4gIE9uY2UgdGhhdCBoYXBwZW5zLCB0aGUgYWxnb3JpdGhtIGluc3RydWN0cyB0aGUgbWFwLWJhc2VkXG4gKiAgICAgZW50cmllcyAoYFtzdHlsZV1gIGluIHRoaXMgY2FzZSkgdG8gXCJjYXRjaCB1cFwiIGFuZCBhcHBseVxuICogICAgIGFsbCB2YWx1ZXMgdXAgdG8gdGhlIGB3aWR0aGAgdmFsdWUuIFdoZW4gdGhpcyBoYXBwZW5zIHRoZVxuICogICAgIGBoZWlnaHRgIHZhbHVlIGlzIGFwcGxpZWQgdG8gdGhlIGVsZW1lbnQgKHNpbmNlIGl0IGlzXG4gKiAgICAgYWxwaGFiZXRpY2FsbHkgc2l0dWF0ZWQgYmVmb3JlIHRoZSBgd2lkdGhgIHByb3BlcnR5KS5cbiAqXG4gKiAzLiBTaW5jZSB0aGVyZSBhcmUgbm8gbW9yZSBwcm9wLWJhc2VkIGVudHJpZXMgYW55bW9yZSwgdGhlXG4gKiAgICBsb29wIGV4aXRzIGFuZCB0aGVuLCBqdXN0IGJlZm9yZSB0aGUgZmx1c2hpbmcgZW5kcywgaXRcbiAqICAgIGluc3RydWN0cyBhbGwgbWFwLWJhc2VkIGJpbmRpbmdzIHRvIFwiZmluaXNoIHVwXCIgYXBwbHlpbmdcbiAqICAgIHRoZWlyIHZhbHVlcy5cbiAqXG4gKiA0LiBUaGUgb25seSByZW1haW5pbmcgdmFsdWUgd2l0aGluIHRoZSBtYXAtYmFzZWQgZW50cmllcyBpc1xuICogICAgdGhlIGB6LWluZGV4YCB2YWx1ZSAoYHdpZHRoYCBnb3Qgc2tpcHBlZCBiZWNhdXNlIGl0IHdhc1xuICogICAgc3VjY2Vzc2Z1bGx5IGFwcGxpZWQgdmlhIHRoZSBwcm9wLWJhc2VkIGBbc3R5bGUud2lkdGhdYFxuICogICAgYmluZGluZykuIFNpbmNlIGFsbCBtYXAtYmFzZWQgZW50cmllcyBhcmUgdG9sZCB0byBcImZpbmlzaCB1cFwiLFxuICogICAgdGhlIGB6LWluZGV4YCB2YWx1ZSBpcyBpdGVyYXRlZCBvdmVyIGFuZCBpdCBpcyB0aGVuIGFwcGxpZWRcbiAqICAgIHRvIHRoZSBlbGVtZW50LlxuICpcbiAqIFRoZSBtb3N0IGltcG9ydGFudCB0aGluZyB0byB0YWtlIG5vdGUgb2YgaGVyZSBpcyB0aGF0IHByb3AtYmFzZWRcbiAqIGJpbmRpbmdzIGFyZSBldmFsdWF0ZWQgaW4gb3JkZXIgYWxvbmdzaWRlIG1hcC1iYXNlZCBiaW5kaW5ncy5cbiAqIFRoaXMgYWxsb3dzIGFsbCBzdHlsaW5nIGFjcm9zcyBhbiBlbGVtZW50IHRvIGJlIGFwcGxpZWQgaW4gTyhuKVxuICogdGltZSAoYSBzaW1pbGFyIGFsZ29yaXRobSBpcyB0aGF0IG9mIHRoZSBhcnJheSBtZXJnZSBhbGdvcml0aG1cbiAqIGluIG1lcmdlIHNvcnQpLlxuICovXG5leHBvcnQgY29uc3Qgc3luY1N0eWxpbmdNYXA6IFN5bmNTdHlsaW5nTWFwc0ZuID1cbiAgICAoY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCByZW5kZXJlcjogUmVuZGVyZXIzIHwgUHJvY2VkdXJhbFJlbmRlcmVyMyB8IG51bGwsIGVsZW1lbnQ6IFJFbGVtZW50LFxuICAgICBkYXRhOiBMU3R5bGluZ0RhdGEsIGFwcGx5U3R5bGluZ0ZuOiBBcHBseVN0eWxpbmdGbiwgc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm4gfCBudWxsLFxuICAgICBtb2RlOiBTdHlsaW5nTWFwc1N5bmNNb2RlLCB0YXJnZXRQcm9wPzogc3RyaW5nIHwgbnVsbCxcbiAgICAgZGVmYXVsdFZhbHVlPzogc3RyaW5nIHwgbnVsbCk6IGJvb2xlYW4gPT4ge1xuICAgICAgbGV0IHRhcmdldFByb3BWYWx1ZVdhc0FwcGxpZWQgPSBmYWxzZTtcblxuICAgICAgLy8gb25jZSB0aGUgbWFwLWJhc2VkIHN0eWxpbmcgY29kZSBpcyBhY3RpdmF0ZSBpdCBpcyBuZXZlciBkZWFjdGl2YXRlZC4gRm9yIHRoaXMgcmVhc29uIGFcbiAgICAgIC8vIGNoZWNrIHRvIHNlZSBpZiB0aGUgY3VycmVudCBzdHlsaW5nIGNvbnRleHQgaGFzIGFueSBtYXAgYmFzZWQgYmluZGluZ3MgaXMgcmVxdWlyZWQuXG4gICAgICBjb25zdCB0b3RhbE1hcHMgPSBnZXRWYWx1ZXNDb3VudChjb250ZXh0LCBUU3R5bGluZ0NvbnRleHRJbmRleC5NYXBCaW5kaW5nc1Bvc2l0aW9uKTtcbiAgICAgIGlmICh0b3RhbE1hcHMpIHtcbiAgICAgICAgbGV0IHJ1blRoZVN5bmNBbGdvcml0aG0gPSB0cnVlO1xuICAgICAgICBjb25zdCBsb29wVW50aWxFbmQgPSAhdGFyZ2V0UHJvcDtcblxuICAgICAgICAvLyBJZiB0aGUgY29kZSBpcyB0b2xkIHRvIGZpbmlzaCB1cCAocnVuIHVudGlsIHRoZSBlbmQpLCBidXQgdGhlIG1vZGVcbiAgICAgICAgLy8gaGFzbid0IGJlZW4gZmxhZ2dlZCB0byBhcHBseSB2YWx1ZXMgKGl0IG9ubHkgdHJhdmVyc2VzIHZhbHVlcykgdGhlblxuICAgICAgICAvLyB0aGVyZSBpcyBubyBwb2ludCBpbiBpdGVyYXRpbmcgb3ZlciB0aGUgYXJyYXkgYmVjYXVzZSBub3RoaW5nIHdpbGxcbiAgICAgICAgLy8gYmUgYXBwbGllZCB0byB0aGUgZWxlbWVudC5cbiAgICAgICAgaWYgKGxvb3BVbnRpbEVuZCAmJiAobW9kZSAmIH5TdHlsaW5nTWFwc1N5bmNNb2RlLkFwcGx5QWxsVmFsdWVzKSkge1xuICAgICAgICAgIHJ1blRoZVN5bmNBbGdvcml0aG0gPSBmYWxzZTtcbiAgICAgICAgICB0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkID0gdHJ1ZTtcbiAgICAgICAgfVxuXG4gICAgICAgIGlmIChydW5UaGVTeW5jQWxnb3JpdGhtKSB7XG4gICAgICAgICAgdGFyZ2V0UHJvcFZhbHVlV2FzQXBwbGllZCA9IGlubmVyU3luY1N0eWxpbmdNYXAoXG4gICAgICAgICAgICAgIGNvbnRleHQsIHJlbmRlcmVyLCBlbGVtZW50LCBkYXRhLCBhcHBseVN0eWxpbmdGbiwgc2FuaXRpemVyLCBtb2RlLCB0YXJnZXRQcm9wIHx8IG51bGwsXG4gICAgICAgICAgICAgIDAsIGRlZmF1bHRWYWx1ZSB8fCBudWxsKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGlmIChsb29wVW50aWxFbmQpIHtcbiAgICAgICAgICByZXNldFN5bmNDdXJzb3JzKCk7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHRhcmdldFByb3BWYWx1ZVdhc0FwcGxpZWQ7XG4gICAgfTtcblxuLyoqXG4gKiBSZWN1cnNpdmUgZnVuY3Rpb24gZGVzaWduZWQgdG8gYXBwbHkgbWFwLWJhc2VkIHN0eWxpbmcgdG8gYW4gZWxlbWVudCBvbmUgbWFwIGF0IGEgdGltZS5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIGRlc2lnbmVkIHRvIGJlIGNhbGxlZCBmcm9tIHRoZSBgc3luY1N0eWxpbmdNYXBgIGZ1bmN0aW9uIGFuZCB3aWxsXG4gKiBhcHBseSBtYXAtYmFzZWQgc3R5bGluZyBkYXRhIG9uZSBtYXAgYXQgYSB0aW1lIHRvIHRoZSBwcm92aWRlZCBgZWxlbWVudGAuXG4gKlxuICogVGhpcyBmdW5jdGlvbiBpcyByZWN1cnNpdmUgYW5kIGl0IHdpbGwgY2FsbCBpdHNlbGYgaWYgYSBmb2xsb3ctdXAgbWFwIHZhbHVlIGlzIHRvIGJlXG4gKiBwcm9jZXNzZWQuIFRvIGxlYXJuIG1vcmUgYWJvdXQgaG93IHRoZSBhbGdvcml0aG0gd29ya3MsIHNlZSBgc3luY1N0eWxpbmdNYXBgLlxuICovXG5mdW5jdGlvbiBpbm5lclN5bmNTdHlsaW5nTWFwKFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgcmVuZGVyZXI6IFJlbmRlcmVyMyB8IFByb2NlZHVyYWxSZW5kZXJlcjMgfCBudWxsLCBlbGVtZW50OiBSRWxlbWVudCxcbiAgICBkYXRhOiBMU3R5bGluZ0RhdGEsIGFwcGx5U3R5bGluZ0ZuOiBBcHBseVN0eWxpbmdGbiwgc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm4gfCBudWxsLFxuICAgIG1vZGU6IFN0eWxpbmdNYXBzU3luY01vZGUsIHRhcmdldFByb3A6IHN0cmluZyB8IG51bGwsIGN1cnJlbnRNYXBJbmRleDogbnVtYmVyLFxuICAgIGRlZmF1bHRWYWx1ZTogc3RyaW5nIHwgbnVsbCk6IGJvb2xlYW4ge1xuICBsZXQgdGFyZ2V0UHJvcFZhbHVlV2FzQXBwbGllZCA9IGZhbHNlO1xuICBjb25zdCB0b3RhbE1hcHMgPSBnZXRWYWx1ZXNDb3VudChjb250ZXh0LCBUU3R5bGluZ0NvbnRleHRJbmRleC5NYXBCaW5kaW5nc1Bvc2l0aW9uKTtcbiAgaWYgKGN1cnJlbnRNYXBJbmRleCA8IHRvdGFsTWFwcykge1xuICAgIGNvbnN0IGJpbmRpbmdJbmRleCA9IGdldEJpbmRpbmdWYWx1ZShcbiAgICAgICAgY29udGV4dCwgVFN0eWxpbmdDb250ZXh0SW5kZXguTWFwQmluZGluZ3NQb3NpdGlvbiwgY3VycmVudE1hcEluZGV4KSBhcyBudW1iZXI7XG4gICAgY29uc3Qgc3R5bGluZ01hcEFyciA9IGRhdGFbYmluZGluZ0luZGV4XSBhcyBTdHlsaW5nTWFwQXJyYXk7XG5cbiAgICBsZXQgY3Vyc29yID0gZ2V0Q3VycmVudFN5bmNDdXJzb3IoY3VycmVudE1hcEluZGV4KTtcbiAgICB3aGlsZSAoY3Vyc29yIDwgc3R5bGluZ01hcEFyci5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IHByb3AgPSBnZXRNYXBQcm9wKHN0eWxpbmdNYXBBcnIsIGN1cnNvcik7XG4gICAgICBjb25zdCBpdGVyYXRlZFRvb0ZhciA9IHRhcmdldFByb3AgJiYgcHJvcCA+IHRhcmdldFByb3A7XG4gICAgICBjb25zdCBpc1RhcmdldFByb3BNYXRjaGVkID0gIWl0ZXJhdGVkVG9vRmFyICYmIHByb3AgPT09IHRhcmdldFByb3A7XG4gICAgICBjb25zdCB2YWx1ZSA9IGdldE1hcFZhbHVlKHN0eWxpbmdNYXBBcnIsIGN1cnNvcik7XG4gICAgICBjb25zdCB2YWx1ZUlzRGVmaW5lZCA9IGlzU3R5bGluZ1ZhbHVlRGVmaW5lZCh2YWx1ZSk7XG5cbiAgICAgIC8vIHRoZSByZWN1cnNpdmUgY29kZSBpcyBkZXNpZ25lZCB0byBrZWVwIGFwcGx5aW5nIHVudGlsXG4gICAgICAvLyBpdCByZWFjaGVzIG9yIGdvZXMgcGFzdCB0aGUgdGFyZ2V0IHByb3AuIElmIGFuZCB3aGVuXG4gICAgICAvLyB0aGlzIGhhcHBlbnMgdGhlbiBpdCB3aWxsIHN0b3AgcHJvY2Vzc2luZyB2YWx1ZXMsIGJ1dFxuICAgICAgLy8gYWxsIG90aGVyIG1hcCB2YWx1ZXMgbXVzdCBhbHNvIGNhdGNoIHVwIHRvIHRoZSBzYW1lXG4gICAgICAvLyBwb2ludC4gVGhpcyBpcyB3aHkgYSByZWN1cnNpdmUgY2FsbCBpcyBzdGlsbCBpc3N1ZWRcbiAgICAgIC8vIGV2ZW4gaWYgdGhlIGNvZGUgaGFzIGl0ZXJhdGVkIHRvbyBmYXIuXG4gICAgICBjb25zdCBpbm5lck1vZGUgPVxuICAgICAgICAgIGl0ZXJhdGVkVG9vRmFyID8gbW9kZSA6IHJlc29sdmVJbm5lck1hcE1vZGUobW9kZSwgdmFsdWVJc0RlZmluZWQsIGlzVGFyZ2V0UHJvcE1hdGNoZWQpO1xuICAgICAgY29uc3QgaW5uZXJQcm9wID0gaXRlcmF0ZWRUb29GYXIgPyB0YXJnZXRQcm9wIDogcHJvcDtcbiAgICAgIGxldCB2YWx1ZUFwcGxpZWQgPSBpbm5lclN5bmNTdHlsaW5nTWFwKFxuICAgICAgICAgIGNvbnRleHQsIHJlbmRlcmVyLCBlbGVtZW50LCBkYXRhLCBhcHBseVN0eWxpbmdGbiwgc2FuaXRpemVyLCBpbm5lck1vZGUsIGlubmVyUHJvcCxcbiAgICAgICAgICBjdXJyZW50TWFwSW5kZXggKyAxLCBkZWZhdWx0VmFsdWUpO1xuXG4gICAgICBpZiAoaXRlcmF0ZWRUb29GYXIpIHtcbiAgICAgICAgaWYgKCF0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkKSB7XG4gICAgICAgICAgdGFyZ2V0UHJvcFZhbHVlV2FzQXBwbGllZCA9IHZhbHVlQXBwbGllZDtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICAgIH1cblxuICAgICAgaWYgKCF2YWx1ZUFwcGxpZWQgJiYgaXNWYWx1ZUFsbG93ZWRUb0JlQXBwbGllZChtb2RlLCBpc1RhcmdldFByb3BNYXRjaGVkKSkge1xuICAgICAgICBjb25zdCB1c2VEZWZhdWx0ID0gaXNUYXJnZXRQcm9wTWF0Y2hlZCAmJiAhdmFsdWVJc0RlZmluZWQ7XG4gICAgICAgIGNvbnN0IHZhbHVlVG9BcHBseSA9IHVzZURlZmF1bHQgPyBkZWZhdWx0VmFsdWUgOiB2YWx1ZTtcbiAgICAgICAgY29uc3QgYmluZGluZ0luZGV4VG9BcHBseSA9IHVzZURlZmF1bHQgPyBiaW5kaW5nSW5kZXggOiBudWxsO1xuICAgICAgICBjb25zdCBmaW5hbFZhbHVlID0gc2FuaXRpemVyID9cbiAgICAgICAgICAgIHNhbml0aXplcihwcm9wLCB2YWx1ZVRvQXBwbHksIFN0eWxlU2FuaXRpemVNb2RlLlZhbGlkYXRlQW5kU2FuaXRpemUpIDpcbiAgICAgICAgICAgIHZhbHVlVG9BcHBseTtcbiAgICAgICAgYXBwbHlTdHlsaW5nRm4ocmVuZGVyZXIsIGVsZW1lbnQsIHByb3AsIGZpbmFsVmFsdWUsIGJpbmRpbmdJbmRleFRvQXBwbHkpO1xuICAgICAgICB2YWx1ZUFwcGxpZWQgPSB0cnVlO1xuICAgICAgfVxuXG4gICAgICB0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkID0gdmFsdWVBcHBsaWVkICYmIGlzVGFyZ2V0UHJvcE1hdGNoZWQ7XG4gICAgICBjdXJzb3IgKz0gU3R5bGluZ01hcEFycmF5SW5kZXguVHVwbGVTaXplO1xuICAgIH1cbiAgICBzZXRDdXJyZW50U3luY0N1cnNvcihjdXJyZW50TWFwSW5kZXgsIGN1cnNvcik7XG5cbiAgICAvLyB0aGlzIGlzIGEgZmFsbGJhY2sgY2FzZSBpbiB0aGUgZXZlbnQgdGhhdCB0aGUgc3R5bGluZyBtYXAgaXMgYG51bGxgIGZvciB0aGlzXG4gICAgLy8gYmluZGluZyBidXQgdGhlcmUgYXJlIG90aGVyIG1hcC1iYXNlZCBiaW5kaW5ncyB0aGF0IG5lZWQgdG8gYmUgZXZhbHVhdGVkXG4gICAgLy8gYWZ0ZXJ3YXJkcy4gSWYgdGhlIGBwcm9wYCB2YWx1ZSBpcyBmYWxzeSB0aGVuIHRoZSBpbnRlbnRpb24gaXMgdG8gY3ljbGVcbiAgICAvLyB0aHJvdWdoIGFsbCBvZiB0aGUgcHJvcGVydGllcyBpbiB0aGUgcmVtYWluaW5nIG1hcHMgYXMgd2VsbC4gSWYgdGhlIGN1cnJlbnRcbiAgICAvLyBzdHlsaW5nIG1hcCBpcyB0b28gc2hvcnQgdGhlbiB0aGVyZSBhcmUgbm8gdmFsdWVzIHRvIGl0ZXJhdGUgb3Zlci4gSW4gZWl0aGVyXG4gICAgLy8gY2FzZSB0aGUgZm9sbG93LXVwIG1hcHMgbmVlZCB0byBiZSBpdGVyYXRlZCBvdmVyLlxuICAgIGlmIChzdHlsaW5nTWFwQXJyLmxlbmd0aCA9PT0gU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbiB8fCAhdGFyZ2V0UHJvcCkge1xuICAgICAgcmV0dXJuIGlubmVyU3luY1N0eWxpbmdNYXAoXG4gICAgICAgICAgY29udGV4dCwgcmVuZGVyZXIsIGVsZW1lbnQsIGRhdGEsIGFwcGx5U3R5bGluZ0ZuLCBzYW5pdGl6ZXIsIG1vZGUsIHRhcmdldFByb3AsXG4gICAgICAgICAgY3VycmVudE1hcEluZGV4ICsgMSwgZGVmYXVsdFZhbHVlKTtcbiAgICB9XG4gIH1cblxuXG4gIHJldHVybiB0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkO1xufVxuXG5cbi8qKlxuICogRW5hYmxlcyBzdXBwb3J0IGZvciBtYXAtYmFzZWQgc3R5bGluZyBiaW5kaW5ncyAoZS5nLiBgW3N0eWxlXWAgYW5kIGBbY2xhc3NdYCBiaW5kaW5ncykuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBhY3RpdmF0ZVN0eWxpbmdNYXBGZWF0dXJlKCkge1xuICBzZXRTdHlsaW5nTWFwc1N5bmNGbihzeW5jU3R5bGluZ01hcCk7XG59XG5cbi8qKlxuICogVXNlZCB0byBkZXRlcm1pbmUgdGhlIG1vZGUgZm9yIHRoZSBpbm5lciByZWN1cnNpdmUgY2FsbC5cbiAqXG4gKiBJZiBhbiBpbm5lciBtYXAgaXMgaXRlcmF0ZWQgb24gdGhlbiB0aGlzIGlzIGRvbmUgc28gZm9yIG9uZVxuICogb2YgdHdvIHJlYXNvbnM6XG4gKlxuICogLSB2YWx1ZSBpcyBiZWluZyBhcHBsaWVkOlxuICogICBpZiB0aGUgdmFsdWUgaXMgYmVpbmcgYXBwbGllZCBmcm9tIHRoaXMgY3VycmVudCBzdHlsaW5nXG4gKiAgIG1hcCB0aGVuIHRoZXJlIGlzIG5vIG5lZWQgdG8gYXBwbHkgaXQgaW4gYSBkZWVwZXIgbWFwLlxuICpcbiAqIC0gdmFsdWUgaXMgYmVpbmcgbm90IGFwcGxpZWQ6XG4gKiAgIGFwcGx5IHRoZSB2YWx1ZSBpZiBpdCBpcyBmb3VuZCBpbiBhIGRlZXBlciBtYXAuXG4gKlxuICogV2hlbiB0aGVzZSByZWFzb25zIGFyZSBlbmNvdW50ZXJlZCB0aGUgZmxhZ3Mgd2lsbCBmb3IgdGhlXG4gKiBpbm5lciBtYXAgbW9kZSB3aWxsIGJlIGNvbmZpZ3VyZWQuXG4gKi9cbmZ1bmN0aW9uIHJlc29sdmVJbm5lck1hcE1vZGUoXG4gICAgY3VycmVudE1vZGU6IG51bWJlciwgdmFsdWVJc0RlZmluZWQ6IGJvb2xlYW4sIGlzRXhhY3RNYXRjaDogYm9vbGVhbik6IG51bWJlciB7XG4gIGxldCBpbm5lck1vZGUgPSBjdXJyZW50TW9kZTtcbiAgaWYgKCF2YWx1ZUlzRGVmaW5lZCAmJiAhKGN1cnJlbnRNb2RlICYgU3R5bGluZ01hcHNTeW5jTW9kZS5Ta2lwVGFyZ2V0UHJvcCkgJiZcbiAgICAgIChpc0V4YWN0TWF0Y2ggfHwgKGN1cnJlbnRNb2RlICYgU3R5bGluZ01hcHNTeW5jTW9kZS5BcHBseUFsbFZhbHVlcykpKSB7XG4gICAgLy8gY2FzZSAxOiBzZXQgdGhlIG1vZGUgdG8gYXBwbHkgdGhlIHRhcmdldGVkIHByb3AgdmFsdWUgaWYgaXRcbiAgICAvLyBlbmRzIHVwIGJlaW5nIGVuY291bnRlcmVkIGluIGFub3RoZXIgbWFwIHZhbHVlXG4gICAgaW5uZXJNb2RlIHw9IFN0eWxpbmdNYXBzU3luY01vZGUuQXBwbHlUYXJnZXRQcm9wO1xuICAgIGlubmVyTW9kZSAmPSB+U3R5bGluZ01hcHNTeW5jTW9kZS5Ta2lwVGFyZ2V0UHJvcDtcbiAgfSBlbHNlIHtcbiAgICAvLyBjYXNlIDI6IHNldCB0aGUgbW9kZSB0byBza2lwIHRoZSB0YXJnZXRlZCBwcm9wIHZhbHVlIGlmIGl0XG4gICAgLy8gZW5kcyB1cCBiZWluZyBlbmNvdW50ZXJlZCBpbiBhbm90aGVyIG1hcCB2YWx1ZVxuICAgIGlubmVyTW9kZSB8PSBTdHlsaW5nTWFwc1N5bmNNb2RlLlNraXBUYXJnZXRQcm9wO1xuICAgIGlubmVyTW9kZSAmPSB+U3R5bGluZ01hcHNTeW5jTW9kZS5BcHBseVRhcmdldFByb3A7XG4gIH1cblxuICByZXR1cm4gaW5uZXJNb2RlO1xufVxuXG4vKipcbiAqIERlY2lkZXMgd2hldGhlciBvciBub3QgYSBwcm9wL3ZhbHVlIGVudHJ5IHdpbGwgYmUgYXBwbGllZCB0byBhbiBlbGVtZW50LlxuICpcbiAqIFRvIGRldGVybWluZSB3aGV0aGVyIG9yIG5vdCBhIHZhbHVlIGlzIHRvIGJlIGFwcGxpZWQsXG4gKiB0aGUgZm9sbG93aW5nIHByb2NlZHVyZSBpcyBldmFsdWF0ZWQ6XG4gKlxuICogRmlyc3QgY2hlY2sgdG8gc2VlIHRoZSBjdXJyZW50IGBtb2RlYCBzdGF0dXM6XG4gKiAgMS4gSWYgdGhlIG1vZGUgdmFsdWUgcGVybWl0cyBhbGwgcHJvcHMgdG8gYmUgYXBwbGllZCB0aGVuIGFsbG93LlxuICogICAgLSBCdXQgZG8gbm90IGFsbG93IGlmIHRoZSBjdXJyZW50IHByb3AgaXMgc2V0IHRvIGJlIHNraXBwZWQuXG4gKiAgMi4gT3RoZXJ3aXNlIGlmIHRoZSBjdXJyZW50IHByb3AgaXMgcGVybWl0dGVkIHRoZW4gYWxsb3cuXG4gKi9cbmZ1bmN0aW9uIGlzVmFsdWVBbGxvd2VkVG9CZUFwcGxpZWQobW9kZTogbnVtYmVyLCBpc1RhcmdldFByb3BNYXRjaGVkOiBib29sZWFuKSB7XG4gIGxldCBkb0FwcGx5VmFsdWUgPSAobW9kZSAmIFN0eWxpbmdNYXBzU3luY01vZGUuQXBwbHlBbGxWYWx1ZXMpID4gMDtcbiAgaWYgKCFkb0FwcGx5VmFsdWUpIHtcbiAgICBpZiAobW9kZSAmIFN0eWxpbmdNYXBzU3luY01vZGUuQXBwbHlUYXJnZXRQcm9wKSB7XG4gICAgICBkb0FwcGx5VmFsdWUgPSBpc1RhcmdldFByb3BNYXRjaGVkO1xuICAgIH1cbiAgfSBlbHNlIGlmICgobW9kZSAmIFN0eWxpbmdNYXBzU3luY01vZGUuU2tpcFRhcmdldFByb3ApICYmIGlzVGFyZ2V0UHJvcE1hdGNoZWQpIHtcbiAgICBkb0FwcGx5VmFsdWUgPSBmYWxzZTtcbiAgfVxuICByZXR1cm4gZG9BcHBseVZhbHVlO1xufVxuXG4vKipcbiAqIFVzZWQgdG8ga2VlcCB0cmFjayBvZiBjb25jdXJyZW50IGN1cnNvciB2YWx1ZXMgZm9yIG11bHRpcGxlIG1hcC1iYXNlZCBzdHlsaW5nIGJpbmRpbmdzIHByZXNlbnQgb25cbiAqIGFuIGVsZW1lbnQuXG4gKi9cbmNvbnN0IE1BUF9DVVJTT1JTOiBudW1iZXJbXSA9IFtdO1xuXG4vKipcbiAqIFVzZWQgdG8gcmVzZXQgdGhlIHN0YXRlIG9mIGVhY2ggY3Vyc29yIHZhbHVlIGJlaW5nIHVzZWQgdG8gaXRlcmF0ZSBvdmVyIG1hcC1iYXNlZCBzdHlsaW5nXG4gKiBiaW5kaW5ncy5cbiAqL1xuZnVuY3Rpb24gcmVzZXRTeW5jQ3Vyc29ycygpIHtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBNQVBfQ1VSU09SUy5sZW5ndGg7IGkrKykge1xuICAgIE1BUF9DVVJTT1JTW2ldID0gU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbjtcbiAgfVxufVxuXG4vKipcbiAqIFJldHVybnMgYW4gYWN0aXZlIGN1cnNvciB2YWx1ZSBhdCBhIGdpdmVuIG1hcEluZGV4IGxvY2F0aW9uLlxuICovXG5mdW5jdGlvbiBnZXRDdXJyZW50U3luY0N1cnNvcihtYXBJbmRleDogbnVtYmVyKSB7XG4gIGlmIChtYXBJbmRleCA+PSBNQVBfQ1VSU09SUy5sZW5ndGgpIHtcbiAgICBNQVBfQ1VSU09SUy5wdXNoKFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb24pO1xuICB9XG4gIHJldHVybiBNQVBfQ1VSU09SU1ttYXBJbmRleF07XG59XG5cbi8qKlxuICogU2V0cyBhIGN1cnNvciB2YWx1ZSBhdCBhIGdpdmVuIG1hcEluZGV4IGxvY2F0aW9uLlxuICovXG5mdW5jdGlvbiBzZXRDdXJyZW50U3luY0N1cnNvcihtYXBJbmRleDogbnVtYmVyLCBpbmRleFZhbHVlOiBudW1iZXIpIHtcbiAgTUFQX0NVUlNPUlNbbWFwSW5kZXhdID0gaW5kZXhWYWx1ZTtcbn1cblxuLyoqXG4gKiBVc2VkIHRvIGNvbnZlcnQgYSB7a2V5OnZhbHVlfSBtYXAgaW50byBhIGBTdHlsaW5nTWFwQXJyYXlgIGFycmF5LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gd2lsbCBlaXRoZXIgZ2VuZXJhdGUgYSBuZXcgYFN0eWxpbmdNYXBBcnJheWAgaW5zdGFuY2VcbiAqIG9yIGl0IHdpbGwgcGF0Y2ggdGhlIHByb3ZpZGVkIGBuZXdWYWx1ZXNgIG1hcCB2YWx1ZSBpbnRvIGFuXG4gKiBleGlzdGluZyBgU3R5bGluZ01hcEFycmF5YCB2YWx1ZSAodGhpcyBvbmx5IGhhcHBlbnMgaWYgYGJpbmRpbmdWYWx1ZWBcbiAqIGlzIGFuIGluc3RhbmNlIG9mIGBTdHlsaW5nTWFwQXJyYXlgKS5cbiAqXG4gKiBJZiBhIG5ldyBrZXkvdmFsdWUgbWFwIGlzIHByb3ZpZGVkIHdpdGggYW4gb2xkIGBTdHlsaW5nTWFwQXJyYXlgXG4gKiB2YWx1ZSB0aGVuIGFsbCBwcm9wZXJ0aWVzIHdpbGwgYmUgb3ZlcndyaXR0ZW4gd2l0aCB0aGVpciBuZXdcbiAqIHZhbHVlcyBvciB3aXRoIGBudWxsYC4gVGhpcyBtZWFucyB0aGF0IHRoZSBhcnJheSB3aWxsIG5ldmVyXG4gKiBzaHJpbmsgaW4gc2l6ZSAoYnV0IGl0IHdpbGwgYWxzbyBub3QgYmUgY3JlYXRlZCBhbmQgdGhyb3duXG4gKiBhd2F5IHdoZW5ldmVyIHRoZSB7a2V5OnZhbHVlfSBtYXAgZW50cmllcyBjaGFuZ2UpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gbm9ybWFsaXplSW50b1N0eWxpbmdNYXAoXG4gICAgYmluZGluZ1ZhbHVlOiBudWxsIHwgU3R5bGluZ01hcEFycmF5LFxuICAgIG5ld1ZhbHVlczoge1trZXk6IHN0cmluZ106IGFueX0gfCBzdHJpbmcgfCBudWxsIHwgdW5kZWZpbmVkLFxuICAgIG5vcm1hbGl6ZVByb3BzPzogYm9vbGVhbik6IFN0eWxpbmdNYXBBcnJheSB7XG4gIGNvbnN0IHN0eWxpbmdNYXBBcnI6IFN0eWxpbmdNYXBBcnJheSA9IEFycmF5LmlzQXJyYXkoYmluZGluZ1ZhbHVlKSA/IGJpbmRpbmdWYWx1ZSA6IFtudWxsXTtcbiAgc3R5bGluZ01hcEFycltTdHlsaW5nTWFwQXJyYXlJbmRleC5SYXdWYWx1ZVBvc2l0aW9uXSA9IG5ld1ZhbHVlcyB8fCBudWxsO1xuXG4gIC8vIGJlY2F1c2UgdGhlIG5ldyB2YWx1ZXMgbWF5IG5vdCBpbmNsdWRlIGFsbCB0aGUgcHJvcGVydGllc1xuICAvLyB0aGF0IHRoZSBvbGQgb25lcyBoYWQsIGFsbCB2YWx1ZXMgYXJlIHNldCB0byBgbnVsbGAgYmVmb3JlXG4gIC8vIHRoZSBuZXcgdmFsdWVzIGFyZSBhcHBsaWVkLiBUaGlzIHdheSwgd2hlbiBmbHVzaGVkLCB0aGVcbiAgLy8gc3R5bGluZyBhbGdvcml0aG0ga25vd3MgZXhhY3RseSB3aGF0IHN0eWxlL2NsYXNzIHZhbHVlc1xuICAvLyB0byByZW1vdmUgZnJvbSB0aGUgZWxlbWVudCAoc2luY2UgdGhleSBhcmUgYG51bGxgKS5cbiAgZm9yIChsZXQgaiA9IFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb247IGogPCBzdHlsaW5nTWFwQXJyLmxlbmd0aDtcbiAgICAgICBqICs9IFN0eWxpbmdNYXBBcnJheUluZGV4LlR1cGxlU2l6ZSkge1xuICAgIHNldE1hcFZhbHVlKHN0eWxpbmdNYXBBcnIsIGosIG51bGwpO1xuICB9XG5cbiAgbGV0IHByb3BzOiBzdHJpbmdbXXxudWxsID0gbnVsbDtcbiAgbGV0IG1hcDoge1trZXk6IHN0cmluZ106IGFueX18dW5kZWZpbmVkfG51bGw7XG4gIGxldCBhbGxWYWx1ZXNUcnVlID0gZmFsc2U7XG4gIGlmICh0eXBlb2YgbmV3VmFsdWVzID09PSAnc3RyaW5nJykgeyAgLy8gW2NsYXNzXSBiaW5kaW5ncyBhbGxvdyBzdHJpbmcgdmFsdWVzXG4gICAgaWYgKG5ld1ZhbHVlcy5sZW5ndGgpIHtcbiAgICAgIHByb3BzID0gbmV3VmFsdWVzLnNwbGl0KC9cXHMrLyk7XG4gICAgICBhbGxWYWx1ZXNUcnVlID0gdHJ1ZTtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgcHJvcHMgPSBuZXdWYWx1ZXMgPyBPYmplY3Qua2V5cyhuZXdWYWx1ZXMpIDogbnVsbDtcbiAgICBtYXAgPSBuZXdWYWx1ZXM7XG4gIH1cblxuICBpZiAocHJvcHMpIHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHByb3BzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBwcm9wID0gcHJvcHNbaV0gYXMgc3RyaW5nO1xuICAgICAgY29uc3QgbmV3UHJvcCA9IG5vcm1hbGl6ZVByb3BzID8gaHlwaGVuYXRlKHByb3ApIDogcHJvcDtcbiAgICAgIGNvbnN0IHZhbHVlID0gYWxsVmFsdWVzVHJ1ZSA/IHRydWUgOiBtYXAgIVtwcm9wXTtcbiAgICAgIGFkZEl0ZW1Ub1N0eWxpbmdNYXAoc3R5bGluZ01hcEFyciwgbmV3UHJvcCwgdmFsdWUsIHRydWUpO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBzdHlsaW5nTWFwQXJyO1xufVxuXG4vKipcbiAqIEluc2VydHMgdGhlIHByb3ZpZGVkIGl0ZW0gaW50byB0aGUgcHJvdmlkZWQgc3R5bGluZyBhcnJheSBhdCB0aGUgcmlnaHQgc3BvdC5cbiAqXG4gKiBUaGUgYFN0eWxpbmdNYXBBcnJheWAgdHlwZSBpcyBhIHNvcnRlZCBrZXkvdmFsdWUgYXJyYXkgb2YgZW50cmllcy4gVGhpcyBtZWFuc1xuICogdGhhdCB3aGVuIGEgbmV3IGVudHJ5IGlzIGluc2VydGVkIGl0IG11c3QgYmUgcGxhY2VkIGF0IHRoZSByaWdodCBzcG90IGluIHRoZVxuICogYXJyYXkuIFRoaXMgZnVuY3Rpb24gZmlndXJlcyBvdXQgZXhhY3RseSB3aGVyZSB0byBwbGFjZSBpdC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGFkZEl0ZW1Ub1N0eWxpbmdNYXAoXG4gICAgc3R5bGluZ01hcEFycjogU3R5bGluZ01hcEFycmF5LCBwcm9wOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcgfCBib29sZWFuIHwgbnVsbCxcbiAgICBhbGxvd092ZXJ3cml0ZT86IGJvb2xlYW4pIHtcbiAgZm9yIChsZXQgaiA9IFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb247IGogPCBzdHlsaW5nTWFwQXJyLmxlbmd0aDtcbiAgICAgICBqICs9IFN0eWxpbmdNYXBBcnJheUluZGV4LlR1cGxlU2l6ZSkge1xuICAgIGNvbnN0IHByb3BBdEluZGV4ID0gZ2V0TWFwUHJvcChzdHlsaW5nTWFwQXJyLCBqKTtcbiAgICBpZiAocHJvcCA8PSBwcm9wQXRJbmRleCkge1xuICAgICAgbGV0IGFwcGxpZWQgPSBmYWxzZTtcbiAgICAgIGlmIChwcm9wQXRJbmRleCA9PT0gcHJvcCkge1xuICAgICAgICBjb25zdCB2YWx1ZUF0SW5kZXggPSBzdHlsaW5nTWFwQXJyW2pdO1xuICAgICAgICBpZiAoYWxsb3dPdmVyd3JpdGUgfHwgIWlzU3R5bGluZ1ZhbHVlRGVmaW5lZCh2YWx1ZUF0SW5kZXgpKSB7XG4gICAgICAgICAgYXBwbGllZCA9IHRydWU7XG4gICAgICAgICAgc2V0TWFwVmFsdWUoc3R5bGluZ01hcEFyciwgaiwgdmFsdWUpO1xuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBhcHBsaWVkID0gdHJ1ZTtcbiAgICAgICAgc3R5bGluZ01hcEFyci5zcGxpY2UoaiwgMCwgcHJvcCwgdmFsdWUpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIGFwcGxpZWQ7XG4gICAgfVxuICB9XG5cbiAgc3R5bGluZ01hcEFyci5wdXNoKHByb3AsIHZhbHVlKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbi8qKlxuICogQ29udmVydHMgdGhlIHByb3ZpZGVkIHN0eWxpbmcgbWFwIGFycmF5IGludG8gYSBzdHJpbmcuXG4gKlxuICogQ2xhc3NlcyA9PiBgb25lIHR3byB0aHJlZWBcbiAqIFN0eWxlcyA9PiBgcHJvcDp2YWx1ZTsgcHJvcDI6dmFsdWUyYFxuICovXG5leHBvcnQgZnVuY3Rpb24gc3R5bGluZ01hcFRvU3RyaW5nKG1hcDogU3R5bGluZ01hcEFycmF5LCBpc0NsYXNzQmFzZWQ6IGJvb2xlYW4pOiBzdHJpbmcge1xuICBsZXQgc3RyID0gJyc7XG4gIGZvciAobGV0IGkgPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5WYWx1ZXNTdGFydFBvc2l0aW9uOyBpIDwgbWFwLmxlbmd0aDtcbiAgICAgICBpICs9IFN0eWxpbmdNYXBBcnJheUluZGV4LlR1cGxlU2l6ZSkge1xuICAgIGNvbnN0IHByb3AgPSBnZXRNYXBQcm9wKG1hcCwgaSk7XG4gICAgY29uc3QgdmFsdWUgPSBnZXRNYXBWYWx1ZShtYXAsIGkpIGFzIHN0cmluZztcbiAgICBjb25zdCBhdHRyVmFsdWUgPSBjb25jYXRTdHJpbmcocHJvcCwgaXNDbGFzc0Jhc2VkID8gJycgOiB2YWx1ZSwgJzonKTtcbiAgICBzdHIgPSBjb25jYXRTdHJpbmcoc3RyLCBhdHRyVmFsdWUsIGlzQ2xhc3NCYXNlZCA/ICcgJyA6ICc7ICcpO1xuICB9XG4gIHJldHVybiBzdHI7XG59XG5cbi8qKlxuICogQ29udmVydHMgdGhlIHByb3ZpZGVkIHN0eWxpbmcgbWFwIGFycmF5IGludG8gYSBrZXkgdmFsdWUgbWFwLlxuICovXG5leHBvcnQgZnVuY3Rpb24gc3R5bGluZ01hcFRvU3RyaW5nTWFwKG1hcDogU3R5bGluZ01hcEFycmF5IHwgbnVsbCk6IHtba2V5OiBzdHJpbmddOiBhbnl9IHtcbiAgbGV0IHN0cmluZ01hcDoge1trZXk6IHN0cmluZ106IGFueX0gPSB7fTtcbiAgaWYgKG1hcCkge1xuICAgIGZvciAobGV0IGkgPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5WYWx1ZXNTdGFydFBvc2l0aW9uOyBpIDwgbWFwLmxlbmd0aDtcbiAgICAgICAgIGkgKz0gU3R5bGluZ01hcEFycmF5SW5kZXguVHVwbGVTaXplKSB7XG4gICAgICBjb25zdCBwcm9wID0gZ2V0TWFwUHJvcChtYXAsIGkpO1xuICAgICAgY29uc3QgdmFsdWUgPSBnZXRNYXBWYWx1ZShtYXAsIGkpIGFzIHN0cmluZztcbiAgICAgIHN0cmluZ01hcFtwcm9wXSA9IHZhbHVlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gc3RyaW5nTWFwO1xufVxuIl19