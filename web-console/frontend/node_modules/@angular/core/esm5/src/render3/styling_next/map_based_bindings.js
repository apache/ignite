import { setStylingMapsSyncFn } from './bindings';
import { concatString, getBindingValue, getMapProp, getMapValue, getValuesCount, hyphenate, isStylingValueDefined, setMapValue } from './util';
/**
 * --------
 *
 * This file contains the algorithm logic for applying map-based bindings
 * such as `[style]` and `[class]`.
 *
 * --------
 */
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
 */
export var syncStylingMap = function (context, renderer, element, data, applyStylingFn, sanitizer, mode, targetProp, defaultValue) {
    var targetPropValueWasApplied = false;
    // once the map-based styling code is activate it is never deactivated. For this reason a
    // check to see if the current styling context has any map based bindings is required.
    var totalMaps = getValuesCount(context, 3 /* MapBindingsPosition */);
    if (totalMaps) {
        var runTheSyncAlgorithm = true;
        var loopUntilEnd = !targetProp;
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
};
/**
 * Recursive function designed to apply map-based styling to an element one map at a time.
 *
 * This function is designed to be called from the `syncStylingMap` function and will
 * apply map-based styling data one map at a time to the provided `element`.
 *
 * This function is recursive and it will call itself if a follow-up map value is to be
 * processed. To learn more about how the algorithm works, see `syncStylingMap`.
 */
function innerSyncStylingMap(context, renderer, element, data, applyStylingFn, sanitizer, mode, targetProp, currentMapIndex, defaultValue) {
    var targetPropValueWasApplied = false;
    var totalMaps = getValuesCount(context, 3 /* MapBindingsPosition */);
    if (currentMapIndex < totalMaps) {
        var bindingIndex = getBindingValue(context, 3 /* MapBindingsPosition */, currentMapIndex);
        var stylingMapArr = data[bindingIndex];
        var cursor = getCurrentSyncCursor(currentMapIndex);
        while (cursor < stylingMapArr.length) {
            var prop = getMapProp(stylingMapArr, cursor);
            var iteratedTooFar = targetProp && prop > targetProp;
            var isTargetPropMatched = !iteratedTooFar && prop === targetProp;
            var value = getMapValue(stylingMapArr, cursor);
            var valueIsDefined = isStylingValueDefined(value);
            // the recursive code is designed to keep applying until
            // it reaches or goes past the target prop. If and when
            // this happens then it will stop processing values, but
            // all other map values must also catch up to the same
            // point. This is why a recursive call is still issued
            // even if the code has iterated too far.
            var innerMode = iteratedTooFar ? mode : resolveInnerMapMode(mode, valueIsDefined, isTargetPropMatched);
            var innerProp = iteratedTooFar ? targetProp : prop;
            var valueApplied = innerSyncStylingMap(context, renderer, element, data, applyStylingFn, sanitizer, innerMode, innerProp, currentMapIndex + 1, defaultValue);
            if (iteratedTooFar) {
                if (!targetPropValueWasApplied) {
                    targetPropValueWasApplied = valueApplied;
                }
                break;
            }
            if (!valueApplied && isValueAllowedToBeApplied(mode, isTargetPropMatched)) {
                var useDefault = isTargetPropMatched && !valueIsDefined;
                var valueToApply = useDefault ? defaultValue : value;
                var bindingIndexToApply = useDefault ? bindingIndex : null;
                var finalValue = sanitizer ?
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
 */
function resolveInnerMapMode(currentMode, valueIsDefined, isExactMatch) {
    var innerMode = currentMode;
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
 */
function isValueAllowedToBeApplied(mode, isTargetPropMatched) {
    var doApplyValue = (mode & 1 /* ApplyAllValues */) > 0;
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
 */
var MAP_CURSORS = [];
/**
 * Used to reset the state of each cursor value being used to iterate over map-based styling
 * bindings.
 */
function resetSyncCursors() {
    for (var i = 0; i < MAP_CURSORS.length; i++) {
        MAP_CURSORS[i] = 1 /* ValuesStartPosition */;
    }
}
/**
 * Returns an active cursor value at a given mapIndex location.
 */
function getCurrentSyncCursor(mapIndex) {
    if (mapIndex >= MAP_CURSORS.length) {
        MAP_CURSORS.push(1 /* ValuesStartPosition */);
    }
    return MAP_CURSORS[mapIndex];
}
/**
 * Sets a cursor value at a given mapIndex location.
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
 */
export function normalizeIntoStylingMap(bindingValue, newValues, normalizeProps) {
    var stylingMapArr = Array.isArray(bindingValue) ? bindingValue : [null];
    stylingMapArr[0 /* RawValuePosition */] = newValues || null;
    // because the new values may not include all the properties
    // that the old ones had, all values are set to `null` before
    // the new values are applied. This way, when flushed, the
    // styling algorithm knows exactly what style/class values
    // to remove from the element (since they are `null`).
    for (var j = 1 /* ValuesStartPosition */; j < stylingMapArr.length; j += 2 /* TupleSize */) {
        setMapValue(stylingMapArr, j, null);
    }
    var props = null;
    var map;
    var allValuesTrue = false;
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
        for (var i = 0; i < props.length; i++) {
            var prop = props[i];
            var newProp = normalizeProps ? hyphenate(prop) : prop;
            var value = allValuesTrue ? true : map[prop];
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
 */
export function addItemToStylingMap(stylingMapArr, prop, value, allowOverwrite) {
    for (var j = 1 /* ValuesStartPosition */; j < stylingMapArr.length; j += 2 /* TupleSize */) {
        var propAtIndex = getMapProp(stylingMapArr, j);
        if (prop <= propAtIndex) {
            var applied = false;
            if (propAtIndex === prop) {
                var valueAtIndex = stylingMapArr[j];
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
 */
export function stylingMapToString(map, isClassBased) {
    var str = '';
    for (var i = 1 /* ValuesStartPosition */; i < map.length; i += 2 /* TupleSize */) {
        var prop = getMapProp(map, i);
        var value = getMapValue(map, i);
        var attrValue = concatString(prop, isClassBased ? '' : value, ':');
        str = concatString(str, attrValue, isClassBased ? ' ' : '; ');
    }
    return str;
}
/**
 * Converts the provided styling map array into a key value map.
 */
export function stylingMapToStringMap(map) {
    var stringMap = {};
    if (map) {
        for (var i = 1 /* ValuesStartPosition */; i < map.length; i += 2 /* TupleSize */) {
            var prop = getMapProp(map, i);
            var value = getMapValue(map, i);
            stringMap[prop] = value;
        }
    }
    return stringMap;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFwX2Jhc2VkX2JpbmRpbmdzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9zdHlsaW5nX25leHQvbWFwX2Jhc2VkX2JpbmRpbmdzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQVVBLE9BQU8sRUFBQyxvQkFBb0IsRUFBQyxNQUFNLFlBQVksQ0FBQztBQUVoRCxPQUFPLEVBQUMsWUFBWSxFQUFFLGVBQWUsRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLGNBQWMsRUFBRSxTQUFTLEVBQUUscUJBQXFCLEVBQUUsV0FBVyxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBSTdJOzs7Ozs7O0dBT0c7QUFFSDs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztHQStFRztBQUNILE1BQU0sQ0FBQyxJQUFNLGNBQWMsR0FDdkIsVUFBQyxPQUF3QixFQUFFLFFBQWdELEVBQUUsT0FBaUIsRUFDN0YsSUFBa0IsRUFBRSxjQUE4QixFQUFFLFNBQWlDLEVBQ3JGLElBQXlCLEVBQUUsVUFBMEIsRUFDckQsWUFBNEI7SUFDM0IsSUFBSSx5QkFBeUIsR0FBRyxLQUFLLENBQUM7SUFFdEMseUZBQXlGO0lBQ3pGLHNGQUFzRjtJQUN0RixJQUFNLFNBQVMsR0FBRyxjQUFjLENBQUMsT0FBTyw4QkFBMkMsQ0FBQztJQUNwRixJQUFJLFNBQVMsRUFBRTtRQUNiLElBQUksbUJBQW1CLEdBQUcsSUFBSSxDQUFDO1FBQy9CLElBQU0sWUFBWSxHQUFHLENBQUMsVUFBVSxDQUFDO1FBRWpDLHFFQUFxRTtRQUNyRSxzRUFBc0U7UUFDdEUscUVBQXFFO1FBQ3JFLDZCQUE2QjtRQUM3QixJQUFJLFlBQVksSUFBSSxDQUFDLElBQUksR0FBRyx1QkFBbUMsQ0FBQyxFQUFFO1lBQ2hFLG1CQUFtQixHQUFHLEtBQUssQ0FBQztZQUM1Qix5QkFBeUIsR0FBRyxJQUFJLENBQUM7U0FDbEM7UUFFRCxJQUFJLG1CQUFtQixFQUFFO1lBQ3ZCLHlCQUF5QixHQUFHLG1CQUFtQixDQUMzQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE9BQU8sRUFBRSxJQUFJLEVBQUUsY0FBYyxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQUUsVUFBVSxJQUFJLElBQUksRUFDckYsQ0FBQyxFQUFFLFlBQVksSUFBSSxJQUFJLENBQUMsQ0FBQztTQUM5QjtRQUVELElBQUksWUFBWSxFQUFFO1lBQ2hCLGdCQUFnQixFQUFFLENBQUM7U0FDcEI7S0FDRjtJQUVELE9BQU8seUJBQXlCLENBQUM7QUFDbkMsQ0FBQyxDQUFDO0FBRU47Ozs7Ozs7O0dBUUc7QUFDSCxTQUFTLG1CQUFtQixDQUN4QixPQUF3QixFQUFFLFFBQWdELEVBQUUsT0FBaUIsRUFDN0YsSUFBa0IsRUFBRSxjQUE4QixFQUFFLFNBQWlDLEVBQ3JGLElBQXlCLEVBQUUsVUFBeUIsRUFBRSxlQUF1QixFQUM3RSxZQUEyQjtJQUM3QixJQUFJLHlCQUF5QixHQUFHLEtBQUssQ0FBQztJQUN0QyxJQUFNLFNBQVMsR0FBRyxjQUFjLENBQUMsT0FBTyw4QkFBMkMsQ0FBQztJQUNwRixJQUFJLGVBQWUsR0FBRyxTQUFTLEVBQUU7UUFDL0IsSUFBTSxZQUFZLEdBQUcsZUFBZSxDQUNoQyxPQUFPLCtCQUE0QyxlQUFlLENBQVcsQ0FBQztRQUNsRixJQUFNLGFBQWEsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFvQixDQUFDO1FBRTVELElBQUksTUFBTSxHQUFHLG9CQUFvQixDQUFDLGVBQWUsQ0FBQyxDQUFDO1FBQ25ELE9BQU8sTUFBTSxHQUFHLGFBQWEsQ0FBQyxNQUFNLEVBQUU7WUFDcEMsSUFBTSxJQUFJLEdBQUcsVUFBVSxDQUFDLGFBQWEsRUFBRSxNQUFNLENBQUMsQ0FBQztZQUMvQyxJQUFNLGNBQWMsR0FBRyxVQUFVLElBQUksSUFBSSxHQUFHLFVBQVUsQ0FBQztZQUN2RCxJQUFNLG1CQUFtQixHQUFHLENBQUMsY0FBYyxJQUFJLElBQUksS0FBSyxVQUFVLENBQUM7WUFDbkUsSUFBTSxLQUFLLEdBQUcsV0FBVyxDQUFDLGFBQWEsRUFBRSxNQUFNLENBQUMsQ0FBQztZQUNqRCxJQUFNLGNBQWMsR0FBRyxxQkFBcUIsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUVwRCx3REFBd0Q7WUFDeEQsdURBQXVEO1lBQ3ZELHdEQUF3RDtZQUN4RCxzREFBc0Q7WUFDdEQsc0RBQXNEO1lBQ3RELHlDQUF5QztZQUN6QyxJQUFNLFNBQVMsR0FDWCxjQUFjLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsbUJBQW1CLENBQUMsSUFBSSxFQUFFLGNBQWMsRUFBRSxtQkFBbUIsQ0FBQyxDQUFDO1lBQzNGLElBQU0sU0FBUyxHQUFHLGNBQWMsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7WUFDckQsSUFBSSxZQUFZLEdBQUcsbUJBQW1CLENBQ2xDLE9BQU8sRUFBRSxRQUFRLEVBQUUsT0FBTyxFQUFFLElBQUksRUFBRSxjQUFjLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxTQUFTLEVBQ2pGLGVBQWUsR0FBRyxDQUFDLEVBQUUsWUFBWSxDQUFDLENBQUM7WUFFdkMsSUFBSSxjQUFjLEVBQUU7Z0JBQ2xCLElBQUksQ0FBQyx5QkFBeUIsRUFBRTtvQkFDOUIseUJBQXlCLEdBQUcsWUFBWSxDQUFDO2lCQUMxQztnQkFDRCxNQUFNO2FBQ1A7WUFFRCxJQUFJLENBQUMsWUFBWSxJQUFJLHlCQUF5QixDQUFDLElBQUksRUFBRSxtQkFBbUIsQ0FBQyxFQUFFO2dCQUN6RSxJQUFNLFVBQVUsR0FBRyxtQkFBbUIsSUFBSSxDQUFDLGNBQWMsQ0FBQztnQkFDMUQsSUFBTSxZQUFZLEdBQUcsVUFBVSxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQztnQkFDdkQsSUFBTSxtQkFBbUIsR0FBRyxVQUFVLENBQUMsQ0FBQyxDQUFDLFlBQVksQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO2dCQUM3RCxJQUFNLFVBQVUsR0FBRyxTQUFTLENBQUMsQ0FBQztvQkFDMUIsU0FBUyxDQUFDLElBQUksRUFBRSxZQUFZLDhCQUF3QyxDQUFDLENBQUM7b0JBQ3RFLFlBQVksQ0FBQztnQkFDakIsY0FBYyxDQUFDLFFBQVEsRUFBRSxPQUFPLEVBQUUsSUFBSSxFQUFFLFVBQVUsRUFBRSxtQkFBbUIsQ0FBQyxDQUFDO2dCQUN6RSxZQUFZLEdBQUcsSUFBSSxDQUFDO2FBQ3JCO1lBRUQseUJBQXlCLEdBQUcsWUFBWSxJQUFJLG1CQUFtQixDQUFDO1lBQ2hFLE1BQU0scUJBQWtDLENBQUM7U0FDMUM7UUFDRCxvQkFBb0IsQ0FBQyxlQUFlLEVBQUUsTUFBTSxDQUFDLENBQUM7UUFFOUMsK0VBQStFO1FBQy9FLDJFQUEyRTtRQUMzRSwwRUFBMEU7UUFDMUUsOEVBQThFO1FBQzlFLCtFQUErRTtRQUMvRSxvREFBb0Q7UUFDcEQsSUFBSSxhQUFhLENBQUMsTUFBTSxnQ0FBNkMsSUFBSSxDQUFDLFVBQVUsRUFBRTtZQUNwRixPQUFPLG1CQUFtQixDQUN0QixPQUFPLEVBQUUsUUFBUSxFQUFFLE9BQU8sRUFBRSxJQUFJLEVBQUUsY0FBYyxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQUUsVUFBVSxFQUM3RSxlQUFlLEdBQUcsQ0FBQyxFQUFFLFlBQVksQ0FBQyxDQUFDO1NBQ3hDO0tBQ0Y7SUFHRCxPQUFPLHlCQUF5QixDQUFDO0FBQ25DLENBQUM7QUFHRDs7R0FFRztBQUNILE1BQU0sVUFBVSx5QkFBeUI7SUFDdkMsb0JBQW9CLENBQUMsY0FBYyxDQUFDLENBQUM7QUFDdkMsQ0FBQztBQUVEOzs7Ozs7Ozs7Ozs7Ozs7R0FlRztBQUNILFNBQVMsbUJBQW1CLENBQ3hCLFdBQW1CLEVBQUUsY0FBdUIsRUFBRSxZQUFxQjtJQUNyRSxJQUFJLFNBQVMsR0FBRyxXQUFXLENBQUM7SUFDNUIsSUFBSSxDQUFDLGNBQWMsSUFBSSxDQUFDLENBQUMsV0FBVyx5QkFBcUMsQ0FBQztRQUN0RSxDQUFDLFlBQVksSUFBSSxDQUFDLFdBQVcseUJBQXFDLENBQUMsQ0FBQyxFQUFFO1FBQ3hFLDhEQUE4RDtRQUM5RCxpREFBaUQ7UUFDakQsU0FBUywyQkFBdUMsQ0FBQztRQUNqRCxTQUFTLElBQUksdUJBQW1DLENBQUM7S0FDbEQ7U0FBTTtRQUNMLDZEQUE2RDtRQUM3RCxpREFBaUQ7UUFDakQsU0FBUywwQkFBc0MsQ0FBQztRQUNoRCxTQUFTLElBQUksd0JBQW9DLENBQUM7S0FDbkQ7SUFFRCxPQUFPLFNBQVMsQ0FBQztBQUNuQixDQUFDO0FBRUQ7Ozs7Ozs7Ozs7R0FVRztBQUNILFNBQVMseUJBQXlCLENBQUMsSUFBWSxFQUFFLG1CQUE0QjtJQUMzRSxJQUFJLFlBQVksR0FBRyxDQUFDLElBQUkseUJBQXFDLENBQUMsR0FBRyxDQUFDLENBQUM7SUFDbkUsSUFBSSxDQUFDLFlBQVksRUFBRTtRQUNqQixJQUFJLElBQUksMEJBQXNDLEVBQUU7WUFDOUMsWUFBWSxHQUFHLG1CQUFtQixDQUFDO1NBQ3BDO0tBQ0Y7U0FBTSxJQUFJLENBQUMsSUFBSSx5QkFBcUMsQ0FBQyxJQUFJLG1CQUFtQixFQUFFO1FBQzdFLFlBQVksR0FBRyxLQUFLLENBQUM7S0FDdEI7SUFDRCxPQUFPLFlBQVksQ0FBQztBQUN0QixDQUFDO0FBRUQ7OztHQUdHO0FBQ0gsSUFBTSxXQUFXLEdBQWEsRUFBRSxDQUFDO0FBRWpDOzs7R0FHRztBQUNILFNBQVMsZ0JBQWdCO0lBQ3ZCLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxXQUFXLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO1FBQzNDLFdBQVcsQ0FBQyxDQUFDLENBQUMsOEJBQTJDLENBQUM7S0FDM0Q7QUFDSCxDQUFDO0FBRUQ7O0dBRUc7QUFDSCxTQUFTLG9CQUFvQixDQUFDLFFBQWdCO0lBQzVDLElBQUksUUFBUSxJQUFJLFdBQVcsQ0FBQyxNQUFNLEVBQUU7UUFDbEMsV0FBVyxDQUFDLElBQUksNkJBQTBDLENBQUM7S0FDNUQ7SUFDRCxPQUFPLFdBQVcsQ0FBQyxRQUFRLENBQUMsQ0FBQztBQUMvQixDQUFDO0FBRUQ7O0dBRUc7QUFDSCxTQUFTLG9CQUFvQixDQUFDLFFBQWdCLEVBQUUsVUFBa0I7SUFDaEUsV0FBVyxDQUFDLFFBQVEsQ0FBQyxHQUFHLFVBQVUsQ0FBQztBQUNyQyxDQUFDO0FBRUQ7Ozs7Ozs7Ozs7Ozs7R0FhRztBQUNILE1BQU0sVUFBVSx1QkFBdUIsQ0FDbkMsWUFBb0MsRUFDcEMsU0FBMkQsRUFDM0QsY0FBd0I7SUFDMUIsSUFBTSxhQUFhLEdBQW9CLEtBQUssQ0FBQyxPQUFPLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxDQUFDLFlBQVksQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUMzRixhQUFhLDBCQUF1QyxHQUFHLFNBQVMsSUFBSSxJQUFJLENBQUM7SUFFekUsNERBQTREO0lBQzVELDZEQUE2RDtJQUM3RCwwREFBMEQ7SUFDMUQsMERBQTBEO0lBQzFELHNEQUFzRDtJQUN0RCxLQUFLLElBQUksQ0FBQyw4QkFBMkMsRUFBRSxDQUFDLEdBQUcsYUFBYSxDQUFDLE1BQU0sRUFDMUUsQ0FBQyxxQkFBa0MsRUFBRTtRQUN4QyxXQUFXLENBQUMsYUFBYSxFQUFFLENBQUMsRUFBRSxJQUFJLENBQUMsQ0FBQztLQUNyQztJQUVELElBQUksS0FBSyxHQUFrQixJQUFJLENBQUM7SUFDaEMsSUFBSSxHQUF3QyxDQUFDO0lBQzdDLElBQUksYUFBYSxHQUFHLEtBQUssQ0FBQztJQUMxQixJQUFJLE9BQU8sU0FBUyxLQUFLLFFBQVEsRUFBRSxFQUFHLHVDQUF1QztRQUMzRSxJQUFJLFNBQVMsQ0FBQyxNQUFNLEVBQUU7WUFDcEIsS0FBSyxHQUFHLFNBQVMsQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUM7WUFDL0IsYUFBYSxHQUFHLElBQUksQ0FBQztTQUN0QjtLQUNGO1NBQU07UUFDTCxLQUFLLEdBQUcsU0FBUyxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7UUFDbEQsR0FBRyxHQUFHLFNBQVMsQ0FBQztLQUNqQjtJQUVELElBQUksS0FBSyxFQUFFO1FBQ1QsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLEtBQUssQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7WUFDckMsSUFBTSxJQUFJLEdBQUcsS0FBSyxDQUFDLENBQUMsQ0FBVyxDQUFDO1lBQ2hDLElBQU0sT0FBTyxHQUFHLGNBQWMsQ0FBQyxDQUFDLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7WUFDeEQsSUFBTSxLQUFLLEdBQUcsYUFBYSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEdBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUNqRCxtQkFBbUIsQ0FBQyxhQUFhLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBRSxJQUFJLENBQUMsQ0FBQztTQUMxRDtLQUNGO0lBRUQsT0FBTyxhQUFhLENBQUM7QUFDdkIsQ0FBQztBQUVEOzs7Ozs7R0FNRztBQUNILE1BQU0sVUFBVSxtQkFBbUIsQ0FDL0IsYUFBOEIsRUFBRSxJQUFZLEVBQUUsS0FBOEIsRUFDNUUsY0FBd0I7SUFDMUIsS0FBSyxJQUFJLENBQUMsOEJBQTJDLEVBQUUsQ0FBQyxHQUFHLGFBQWEsQ0FBQyxNQUFNLEVBQzFFLENBQUMscUJBQWtDLEVBQUU7UUFDeEMsSUFBTSxXQUFXLEdBQUcsVUFBVSxDQUFDLGFBQWEsRUFBRSxDQUFDLENBQUMsQ0FBQztRQUNqRCxJQUFJLElBQUksSUFBSSxXQUFXLEVBQUU7WUFDdkIsSUFBSSxPQUFPLEdBQUcsS0FBSyxDQUFDO1lBQ3BCLElBQUksV0FBVyxLQUFLLElBQUksRUFBRTtnQkFDeEIsSUFBTSxZQUFZLEdBQUcsYUFBYSxDQUFDLENBQUMsQ0FBQyxDQUFDO2dCQUN0QyxJQUFJLGNBQWMsSUFBSSxDQUFDLHFCQUFxQixDQUFDLFlBQVksQ0FBQyxFQUFFO29CQUMxRCxPQUFPLEdBQUcsSUFBSSxDQUFDO29CQUNmLFdBQVcsQ0FBQyxhQUFhLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxDQUFDO2lCQUN0QzthQUNGO2lCQUFNO2dCQUNMLE9BQU8sR0FBRyxJQUFJLENBQUM7Z0JBQ2YsYUFBYSxDQUFDLE1BQU0sQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQzthQUN6QztZQUNELE9BQU8sT0FBTyxDQUFDO1NBQ2hCO0tBQ0Y7SUFFRCxhQUFhLENBQUMsSUFBSSxDQUFDLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQztJQUNoQyxPQUFPLElBQUksQ0FBQztBQUNkLENBQUM7QUFFRDs7Ozs7R0FLRztBQUNILE1BQU0sVUFBVSxrQkFBa0IsQ0FBQyxHQUFvQixFQUFFLFlBQXFCO0lBQzVFLElBQUksR0FBRyxHQUFHLEVBQUUsQ0FBQztJQUNiLEtBQUssSUFBSSxDQUFDLDhCQUEyQyxFQUFFLENBQUMsR0FBRyxHQUFHLENBQUMsTUFBTSxFQUNoRSxDQUFDLHFCQUFrQyxFQUFFO1FBQ3hDLElBQU0sSUFBSSxHQUFHLFVBQVUsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDLENBQUM7UUFDaEMsSUFBTSxLQUFLLEdBQUcsV0FBVyxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQVcsQ0FBQztRQUM1QyxJQUFNLFNBQVMsR0FBRyxZQUFZLENBQUMsSUFBSSxFQUFFLFlBQVksQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUM7UUFDckUsR0FBRyxHQUFHLFlBQVksQ0FBQyxHQUFHLEVBQUUsU0FBUyxFQUFFLFlBQVksQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQztLQUMvRDtJQUNELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQztBQUVEOztHQUVHO0FBQ0gsTUFBTSxVQUFVLHFCQUFxQixDQUFDLEdBQTJCO0lBQy9ELElBQUksU0FBUyxHQUF5QixFQUFFLENBQUM7SUFDekMsSUFBSSxHQUFHLEVBQUU7UUFDUCxLQUFLLElBQUksQ0FBQyw4QkFBMkMsRUFBRSxDQUFDLEdBQUcsR0FBRyxDQUFDLE1BQU0sRUFDaEUsQ0FBQyxxQkFBa0MsRUFBRTtZQUN4QyxJQUFNLElBQUksR0FBRyxVQUFVLENBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQyxDQUFDO1lBQ2hDLElBQU0sS0FBSyxHQUFHLFdBQVcsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFXLENBQUM7WUFDNUMsU0FBUyxDQUFDLElBQUksQ0FBQyxHQUFHLEtBQUssQ0FBQztTQUN6QjtLQUNGO0lBQ0QsT0FBTyxTQUFTLENBQUM7QUFDbkIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuKiBAbGljZW5zZVxuKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbipcbiogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuKi9cbmltcG9ydCB7U3R5bGVTYW5pdGl6ZUZuLCBTdHlsZVNhbml0aXplTW9kZX0gZnJvbSAnLi4vLi4vc2FuaXRpemF0aW9uL3N0eWxlX3Nhbml0aXplcic7XG5pbXBvcnQge1Byb2NlZHVyYWxSZW5kZXJlcjMsIFJFbGVtZW50LCBSZW5kZXJlcjN9IGZyb20gJy4uL2ludGVyZmFjZXMvcmVuZGVyZXInO1xuXG5pbXBvcnQge3NldFN0eWxpbmdNYXBzU3luY0ZufSBmcm9tICcuL2JpbmRpbmdzJztcbmltcG9ydCB7QXBwbHlTdHlsaW5nRm4sIExTdHlsaW5nRGF0YSwgU3R5bGluZ01hcEFycmF5LCBTdHlsaW5nTWFwQXJyYXlJbmRleCwgU3R5bGluZ01hcHNTeW5jTW9kZSwgU3luY1N0eWxpbmdNYXBzRm4sIFRTdHlsaW5nQ29udGV4dCwgVFN0eWxpbmdDb250ZXh0SW5kZXh9IGZyb20gJy4vaW50ZXJmYWNlcyc7XG5pbXBvcnQge2NvbmNhdFN0cmluZywgZ2V0QmluZGluZ1ZhbHVlLCBnZXRNYXBQcm9wLCBnZXRNYXBWYWx1ZSwgZ2V0VmFsdWVzQ291bnQsIGh5cGhlbmF0ZSwgaXNTdHlsaW5nVmFsdWVEZWZpbmVkLCBzZXRNYXBWYWx1ZX0gZnJvbSAnLi91dGlsJztcblxuXG5cbi8qKlxuICogLS0tLS0tLS1cbiAqXG4gKiBUaGlzIGZpbGUgY29udGFpbnMgdGhlIGFsZ29yaXRobSBsb2dpYyBmb3IgYXBwbHlpbmcgbWFwLWJhc2VkIGJpbmRpbmdzXG4gKiBzdWNoIGFzIGBbc3R5bGVdYCBhbmQgYFtjbGFzc11gLlxuICpcbiAqIC0tLS0tLS0tXG4gKi9cblxuLyoqXG4gKiBVc2VkIHRvIGFwcGx5IHN0eWxpbmcgdmFsdWVzIHByZXNlbnRseSB3aXRoaW4gYW55IG1hcC1iYXNlZCBiaW5kaW5ncyBvbiBhbiBlbGVtZW50LlxuICpcbiAqIEFuZ3VsYXIgc3VwcG9ydHMgbWFwLWJhc2VkIHN0eWxpbmcgYmluZGluZ3Mgd2hpY2ggY2FuIGJlIGFwcGxpZWQgdmlhIHRoZVxuICogYFtzdHlsZV1gIGFuZCBgW2NsYXNzXWAgYmluZGluZ3Mgd2hpY2ggY2FuIGJlIHBsYWNlZCBvbiBhbnkgSFRNTCBlbGVtZW50LlxuICogVGhlc2UgYmluZGluZ3MgY2FuIHdvcmsgaW5kZXBlbmRlbnRseSwgdG9nZXRoZXIgb3IgYWxvbmdzaWRlIHByb3AtYmFzZWRcbiAqIHN0eWxpbmcgYmluZGluZ3MgKGUuZy4gYDxkaXYgW3N0eWxlXT1cInhcIiBbc3R5bGUud2lkdGhdPVwid1wiPmApLlxuICpcbiAqIElmIGEgbWFwLWJhc2VkIHN0eWxpbmcgYmluZGluZyBpcyBkZXRlY3RlZCBieSB0aGUgY29tcGlsZXIsIHRoZSBmb2xsb3dpbmdcbiAqIEFPVCBjb2RlIGlzIHByb2R1Y2VkOlxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIHN0eWxlTWFwKGN0eC5zdHlsZXMpOyAvLyBzdHlsZXMgPSB7a2V5OnZhbHVlfVxuICogY2xhc3NNYXAoY3R4LmNsYXNzZXMpOyAvLyBjbGFzc2VzID0ge2tleTp2YWx1ZX18c3RyaW5nXG4gKiBgYGBcbiAqXG4gKiBJZiBhbmQgd2hlbiBlaXRoZXIgb2YgdGhlIGluc3RydWN0aW9ucyBhYm92ZSBhcmUgZXZhbHVhdGVkLCB0aGVuIHRoZSBjb2RlXG4gKiBwcmVzZW50IGluIHRoaXMgZmlsZSBpcyBpbmNsdWRlZCBpbnRvIHRoZSBidW5kbGUuIFRoZSBtZWNoYW5pc20gdXNlZCwgdG9cbiAqIGFjdGl2YXRlIHN1cHBvcnQgZm9yIG1hcC1iYXNlZCBiaW5kaW5ncyBhdCBydW50aW1lIGlzIHBvc3NpYmxlIHZpYSB0aGVcbiAqIGBhY3RpdmVTdHlsaW5nTWFwRmVhdHVyZWAgZnVuY3Rpb24gKHdoaWNoIGlzIGFsc28gcHJlc2VudCBpbiB0aGlzIGZpbGUpLlxuICpcbiAqICMgVGhlIEFsZ29yaXRobVxuICogV2hlbmV2ZXIgYSBtYXAtYmFzZWQgYmluZGluZyB1cGRhdGVzICh3aGljaCBpcyB3aGVuIHRoZSBpZGVudGl0eSBvZiB0aGVcbiAqIG1hcC12YWx1ZSBjaGFuZ2VzKSB0aGVuIHRoZSBtYXAgaXMgaXRlcmF0ZWQgb3ZlciBhbmQgYSBgU3R5bGluZ01hcEFycmF5YCBhcnJheVxuICogaXMgcHJvZHVjZWQuIFRoZSBgU3R5bGluZ01hcEFycmF5YCBpbnN0YW5jZSBpcyBzdG9yZWQgaW4gdGhlIGJpbmRpbmcgbG9jYXRpb25cbiAqIHdoZXJlIHRoZSBgQklORElOR19JTkRFWGAgaXMgc2l0dWF0ZWQgd2hlbiB0aGUgYHN0eWxlTWFwKClgIG9yIGBjbGFzc01hcCgpYFxuICogaW5zdHJ1Y3Rpb24gd2VyZSBjYWxsZWQuIE9uY2UgdGhlIGJpbmRpbmcgY2hhbmdlcywgdGhlbiB0aGUgaW50ZXJuYWwgYGJpdE1hc2tgXG4gKiB2YWx1ZSBpcyBtYXJrZWQgYXMgZGlydHkuXG4gKlxuICogU3R5bGluZyB2YWx1ZXMgYXJlIGFwcGxpZWQgb25jZSBDRCBleGl0cyB0aGUgZWxlbWVudCAod2hpY2ggaGFwcGVucyB3aGVuXG4gKiB0aGUgYHNlbGVjdChuKWAgaW5zdHJ1Y3Rpb24gaXMgY2FsbGVkIG9yIHRoZSB0ZW1wbGF0ZSBmdW5jdGlvbiBleGl0cykuIFdoZW5cbiAqIHRoaXMgb2NjdXJzLCBhbGwgcHJvcC1iYXNlZCBiaW5kaW5ncyBhcmUgYXBwbGllZC4gSWYgYSBtYXAtYmFzZWQgYmluZGluZyBpc1xuICogcHJlc2VudCB0aGVuIGEgc3BlY2lhbCBmbHVzaGluZyBmdW5jdGlvbiAoY2FsbGVkIGEgc3luYyBmdW5jdGlvbikgaXMgbWFkZVxuICogYXZhaWxhYmxlIGFuZCBpdCB3aWxsIGJlIGNhbGxlZCBlYWNoIHRpbWUgYSBzdHlsaW5nIHByb3BlcnR5IGlzIGZsdXNoZWQuXG4gKlxuICogVGhlIGZsdXNoaW5nIGFsZ29yaXRobSBpcyBkZXNpZ25lZCB0byBhcHBseSBzdHlsaW5nIGZvciBhIHByb3BlcnR5ICh3aGljaCBpc1xuICogYSBDU1MgcHJvcGVydHkgb3IgYSBjbGFzc05hbWUgdmFsdWUpIG9uZSBieSBvbmUuIElmIG1hcC1iYXNlZCBiaW5kaW5nc1xuICogYXJlIHByZXNlbnQsIHRoZW4gdGhlIGZsdXNoaW5nIGFsZ29yaXRobSB3aWxsIGtlZXAgY2FsbGluZyB0aGUgbWFwcyBzdHlsaW5nXG4gKiBzeW5jIGZ1bmN0aW9uIGVhY2ggdGltZSBhIHByb3BlcnR5IGlzIHZpc2l0ZWQuIFRoaXMgd2F5LCB0aGUgZmx1c2hpbmdcbiAqIGJlaGF2aW9yIG9mIG1hcC1iYXNlZCBiaW5kaW5ncyB3aWxsIGFsd2F5cyBiZSBhdCB0aGUgc2FtZSBwcm9wZXJ0eSBsZXZlbFxuICogYXMgdGhlIGN1cnJlbnQgcHJvcC1iYXNlZCBwcm9wZXJ0eSBiZWluZyBpdGVyYXRlZCBvdmVyIChiZWNhdXNlIGV2ZXJ5dGhpbmdcbiAqIGlzIGFscGhhYmV0aWNhbGx5IHNvcnRlZCkuXG4gKlxuICogTGV0J3MgaW1hZ2luZSB3ZSBoYXZlIHRoZSBmb2xsb3dpbmcgSFRNTCB0ZW1wbGF0ZSBjb2RlOlxuICpcbiAqIGBgYGh0bWxcbiAqIDxkaXYgW3N0eWxlXT1cInt3aWR0aDonMTAwcHgnLCBoZWlnaHQ6JzIwMHB4JywgJ3otaW5kZXgnOicxMCd9XCJcbiAqICAgICAgW3N0eWxlLndpZHRoLnB4XT1cIjIwMFwiPi4uLjwvZGl2PlxuICogYGBgXG4gKlxuICogV2hlbiBDRCBvY2N1cnMsIGJvdGggdGhlIGBbc3R5bGVdYCBhbmQgYFtzdHlsZS53aWR0aF1gIGJpbmRpbmdzXG4gKiBhcmUgZXZhbHVhdGVkLiBUaGVuIHdoZW4gdGhlIHN0eWxlcyBhcmUgZmx1c2hlZCBvbiBzY3JlZW4sIHRoZVxuICogZm9sbG93aW5nIG9wZXJhdGlvbnMgaGFwcGVuOlxuICpcbiAqIDEuIGBbc3R5bGUud2lkdGhdYCBpcyBhdHRlbXB0ZWQgdG8gYmUgd3JpdHRlbiB0byB0aGUgZWxlbWVudC5cbiAqXG4gKiAyLiAgT25jZSB0aGF0IGhhcHBlbnMsIHRoZSBhbGdvcml0aG0gaW5zdHJ1Y3RzIHRoZSBtYXAtYmFzZWRcbiAqICAgICBlbnRyaWVzIChgW3N0eWxlXWAgaW4gdGhpcyBjYXNlKSB0byBcImNhdGNoIHVwXCIgYW5kIGFwcGx5XG4gKiAgICAgYWxsIHZhbHVlcyB1cCB0byB0aGUgYHdpZHRoYCB2YWx1ZS4gV2hlbiB0aGlzIGhhcHBlbnMgdGhlXG4gKiAgICAgYGhlaWdodGAgdmFsdWUgaXMgYXBwbGllZCB0byB0aGUgZWxlbWVudCAoc2luY2UgaXQgaXNcbiAqICAgICBhbHBoYWJldGljYWxseSBzaXR1YXRlZCBiZWZvcmUgdGhlIGB3aWR0aGAgcHJvcGVydHkpLlxuICpcbiAqIDMuIFNpbmNlIHRoZXJlIGFyZSBubyBtb3JlIHByb3AtYmFzZWQgZW50cmllcyBhbnltb3JlLCB0aGVcbiAqICAgIGxvb3AgZXhpdHMgYW5kIHRoZW4sIGp1c3QgYmVmb3JlIHRoZSBmbHVzaGluZyBlbmRzLCBpdFxuICogICAgaW5zdHJ1Y3RzIGFsbCBtYXAtYmFzZWQgYmluZGluZ3MgdG8gXCJmaW5pc2ggdXBcIiBhcHBseWluZ1xuICogICAgdGhlaXIgdmFsdWVzLlxuICpcbiAqIDQuIFRoZSBvbmx5IHJlbWFpbmluZyB2YWx1ZSB3aXRoaW4gdGhlIG1hcC1iYXNlZCBlbnRyaWVzIGlzXG4gKiAgICB0aGUgYHotaW5kZXhgIHZhbHVlIChgd2lkdGhgIGdvdCBza2lwcGVkIGJlY2F1c2UgaXQgd2FzXG4gKiAgICBzdWNjZXNzZnVsbHkgYXBwbGllZCB2aWEgdGhlIHByb3AtYmFzZWQgYFtzdHlsZS53aWR0aF1gXG4gKiAgICBiaW5kaW5nKS4gU2luY2UgYWxsIG1hcC1iYXNlZCBlbnRyaWVzIGFyZSB0b2xkIHRvIFwiZmluaXNoIHVwXCIsXG4gKiAgICB0aGUgYHotaW5kZXhgIHZhbHVlIGlzIGl0ZXJhdGVkIG92ZXIgYW5kIGl0IGlzIHRoZW4gYXBwbGllZFxuICogICAgdG8gdGhlIGVsZW1lbnQuXG4gKlxuICogVGhlIG1vc3QgaW1wb3J0YW50IHRoaW5nIHRvIHRha2Ugbm90ZSBvZiBoZXJlIGlzIHRoYXQgcHJvcC1iYXNlZFxuICogYmluZGluZ3MgYXJlIGV2YWx1YXRlZCBpbiBvcmRlciBhbG9uZ3NpZGUgbWFwLWJhc2VkIGJpbmRpbmdzLlxuICogVGhpcyBhbGxvd3MgYWxsIHN0eWxpbmcgYWNyb3NzIGFuIGVsZW1lbnQgdG8gYmUgYXBwbGllZCBpbiBPKG4pXG4gKiB0aW1lIChhIHNpbWlsYXIgYWxnb3JpdGhtIGlzIHRoYXQgb2YgdGhlIGFycmF5IG1lcmdlIGFsZ29yaXRobVxuICogaW4gbWVyZ2Ugc29ydCkuXG4gKi9cbmV4cG9ydCBjb25zdCBzeW5jU3R5bGluZ01hcDogU3luY1N0eWxpbmdNYXBzRm4gPVxuICAgIChjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIHJlbmRlcmVyOiBSZW5kZXJlcjMgfCBQcm9jZWR1cmFsUmVuZGVyZXIzIHwgbnVsbCwgZWxlbWVudDogUkVsZW1lbnQsXG4gICAgIGRhdGE6IExTdHlsaW5nRGF0YSwgYXBwbHlTdHlsaW5nRm46IEFwcGx5U3R5bGluZ0ZuLCBzYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbiB8IG51bGwsXG4gICAgIG1vZGU6IFN0eWxpbmdNYXBzU3luY01vZGUsIHRhcmdldFByb3A/OiBzdHJpbmcgfCBudWxsLFxuICAgICBkZWZhdWx0VmFsdWU/OiBzdHJpbmcgfCBudWxsKTogYm9vbGVhbiA9PiB7XG4gICAgICBsZXQgdGFyZ2V0UHJvcFZhbHVlV2FzQXBwbGllZCA9IGZhbHNlO1xuXG4gICAgICAvLyBvbmNlIHRoZSBtYXAtYmFzZWQgc3R5bGluZyBjb2RlIGlzIGFjdGl2YXRlIGl0IGlzIG5ldmVyIGRlYWN0aXZhdGVkLiBGb3IgdGhpcyByZWFzb24gYVxuICAgICAgLy8gY2hlY2sgdG8gc2VlIGlmIHRoZSBjdXJyZW50IHN0eWxpbmcgY29udGV4dCBoYXMgYW55IG1hcCBiYXNlZCBiaW5kaW5ncyBpcyByZXF1aXJlZC5cbiAgICAgIGNvbnN0IHRvdGFsTWFwcyA9IGdldFZhbHVlc0NvdW50KGNvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4Lk1hcEJpbmRpbmdzUG9zaXRpb24pO1xuICAgICAgaWYgKHRvdGFsTWFwcykge1xuICAgICAgICBsZXQgcnVuVGhlU3luY0FsZ29yaXRobSA9IHRydWU7XG4gICAgICAgIGNvbnN0IGxvb3BVbnRpbEVuZCA9ICF0YXJnZXRQcm9wO1xuXG4gICAgICAgIC8vIElmIHRoZSBjb2RlIGlzIHRvbGQgdG8gZmluaXNoIHVwIChydW4gdW50aWwgdGhlIGVuZCksIGJ1dCB0aGUgbW9kZVxuICAgICAgICAvLyBoYXNuJ3QgYmVlbiBmbGFnZ2VkIHRvIGFwcGx5IHZhbHVlcyAoaXQgb25seSB0cmF2ZXJzZXMgdmFsdWVzKSB0aGVuXG4gICAgICAgIC8vIHRoZXJlIGlzIG5vIHBvaW50IGluIGl0ZXJhdGluZyBvdmVyIHRoZSBhcnJheSBiZWNhdXNlIG5vdGhpbmcgd2lsbFxuICAgICAgICAvLyBiZSBhcHBsaWVkIHRvIHRoZSBlbGVtZW50LlxuICAgICAgICBpZiAobG9vcFVudGlsRW5kICYmIChtb2RlICYgflN0eWxpbmdNYXBzU3luY01vZGUuQXBwbHlBbGxWYWx1ZXMpKSB7XG4gICAgICAgICAgcnVuVGhlU3luY0FsZ29yaXRobSA9IGZhbHNlO1xuICAgICAgICAgIHRhcmdldFByb3BWYWx1ZVdhc0FwcGxpZWQgPSB0cnVlO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKHJ1blRoZVN5bmNBbGdvcml0aG0pIHtcbiAgICAgICAgICB0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkID0gaW5uZXJTeW5jU3R5bGluZ01hcChcbiAgICAgICAgICAgICAgY29udGV4dCwgcmVuZGVyZXIsIGVsZW1lbnQsIGRhdGEsIGFwcGx5U3R5bGluZ0ZuLCBzYW5pdGl6ZXIsIG1vZGUsIHRhcmdldFByb3AgfHwgbnVsbCxcbiAgICAgICAgICAgICAgMCwgZGVmYXVsdFZhbHVlIHx8IG51bGwpO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKGxvb3BVbnRpbEVuZCkge1xuICAgICAgICAgIHJlc2V0U3luY0N1cnNvcnMoKTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICByZXR1cm4gdGFyZ2V0UHJvcFZhbHVlV2FzQXBwbGllZDtcbiAgICB9O1xuXG4vKipcbiAqIFJlY3Vyc2l2ZSBmdW5jdGlvbiBkZXNpZ25lZCB0byBhcHBseSBtYXAtYmFzZWQgc3R5bGluZyB0byBhbiBlbGVtZW50IG9uZSBtYXAgYXQgYSB0aW1lLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgZGVzaWduZWQgdG8gYmUgY2FsbGVkIGZyb20gdGhlIGBzeW5jU3R5bGluZ01hcGAgZnVuY3Rpb24gYW5kIHdpbGxcbiAqIGFwcGx5IG1hcC1iYXNlZCBzdHlsaW5nIGRhdGEgb25lIG1hcCBhdCBhIHRpbWUgdG8gdGhlIHByb3ZpZGVkIGBlbGVtZW50YC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIHJlY3Vyc2l2ZSBhbmQgaXQgd2lsbCBjYWxsIGl0c2VsZiBpZiBhIGZvbGxvdy11cCBtYXAgdmFsdWUgaXMgdG8gYmVcbiAqIHByb2Nlc3NlZC4gVG8gbGVhcm4gbW9yZSBhYm91dCBob3cgdGhlIGFsZ29yaXRobSB3b3Jrcywgc2VlIGBzeW5jU3R5bGluZ01hcGAuXG4gKi9cbmZ1bmN0aW9uIGlubmVyU3luY1N0eWxpbmdNYXAoXG4gICAgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCByZW5kZXJlcjogUmVuZGVyZXIzIHwgUHJvY2VkdXJhbFJlbmRlcmVyMyB8IG51bGwsIGVsZW1lbnQ6IFJFbGVtZW50LFxuICAgIGRhdGE6IExTdHlsaW5nRGF0YSwgYXBwbHlTdHlsaW5nRm46IEFwcGx5U3R5bGluZ0ZuLCBzYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbiB8IG51bGwsXG4gICAgbW9kZTogU3R5bGluZ01hcHNTeW5jTW9kZSwgdGFyZ2V0UHJvcDogc3RyaW5nIHwgbnVsbCwgY3VycmVudE1hcEluZGV4OiBudW1iZXIsXG4gICAgZGVmYXVsdFZhbHVlOiBzdHJpbmcgfCBudWxsKTogYm9vbGVhbiB7XG4gIGxldCB0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkID0gZmFsc2U7XG4gIGNvbnN0IHRvdGFsTWFwcyA9IGdldFZhbHVlc0NvdW50KGNvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4Lk1hcEJpbmRpbmdzUG9zaXRpb24pO1xuICBpZiAoY3VycmVudE1hcEluZGV4IDwgdG90YWxNYXBzKSB7XG4gICAgY29uc3QgYmluZGluZ0luZGV4ID0gZ2V0QmluZGluZ1ZhbHVlKFxuICAgICAgICBjb250ZXh0LCBUU3R5bGluZ0NvbnRleHRJbmRleC5NYXBCaW5kaW5nc1Bvc2l0aW9uLCBjdXJyZW50TWFwSW5kZXgpIGFzIG51bWJlcjtcbiAgICBjb25zdCBzdHlsaW5nTWFwQXJyID0gZGF0YVtiaW5kaW5nSW5kZXhdIGFzIFN0eWxpbmdNYXBBcnJheTtcblxuICAgIGxldCBjdXJzb3IgPSBnZXRDdXJyZW50U3luY0N1cnNvcihjdXJyZW50TWFwSW5kZXgpO1xuICAgIHdoaWxlIChjdXJzb3IgPCBzdHlsaW5nTWFwQXJyLmxlbmd0aCkge1xuICAgICAgY29uc3QgcHJvcCA9IGdldE1hcFByb3Aoc3R5bGluZ01hcEFyciwgY3Vyc29yKTtcbiAgICAgIGNvbnN0IGl0ZXJhdGVkVG9vRmFyID0gdGFyZ2V0UHJvcCAmJiBwcm9wID4gdGFyZ2V0UHJvcDtcbiAgICAgIGNvbnN0IGlzVGFyZ2V0UHJvcE1hdGNoZWQgPSAhaXRlcmF0ZWRUb29GYXIgJiYgcHJvcCA9PT0gdGFyZ2V0UHJvcDtcbiAgICAgIGNvbnN0IHZhbHVlID0gZ2V0TWFwVmFsdWUoc3R5bGluZ01hcEFyciwgY3Vyc29yKTtcbiAgICAgIGNvbnN0IHZhbHVlSXNEZWZpbmVkID0gaXNTdHlsaW5nVmFsdWVEZWZpbmVkKHZhbHVlKTtcblxuICAgICAgLy8gdGhlIHJlY3Vyc2l2ZSBjb2RlIGlzIGRlc2lnbmVkIHRvIGtlZXAgYXBwbHlpbmcgdW50aWxcbiAgICAgIC8vIGl0IHJlYWNoZXMgb3IgZ29lcyBwYXN0IHRoZSB0YXJnZXQgcHJvcC4gSWYgYW5kIHdoZW5cbiAgICAgIC8vIHRoaXMgaGFwcGVucyB0aGVuIGl0IHdpbGwgc3RvcCBwcm9jZXNzaW5nIHZhbHVlcywgYnV0XG4gICAgICAvLyBhbGwgb3RoZXIgbWFwIHZhbHVlcyBtdXN0IGFsc28gY2F0Y2ggdXAgdG8gdGhlIHNhbWVcbiAgICAgIC8vIHBvaW50LiBUaGlzIGlzIHdoeSBhIHJlY3Vyc2l2ZSBjYWxsIGlzIHN0aWxsIGlzc3VlZFxuICAgICAgLy8gZXZlbiBpZiB0aGUgY29kZSBoYXMgaXRlcmF0ZWQgdG9vIGZhci5cbiAgICAgIGNvbnN0IGlubmVyTW9kZSA9XG4gICAgICAgICAgaXRlcmF0ZWRUb29GYXIgPyBtb2RlIDogcmVzb2x2ZUlubmVyTWFwTW9kZShtb2RlLCB2YWx1ZUlzRGVmaW5lZCwgaXNUYXJnZXRQcm9wTWF0Y2hlZCk7XG4gICAgICBjb25zdCBpbm5lclByb3AgPSBpdGVyYXRlZFRvb0ZhciA/IHRhcmdldFByb3AgOiBwcm9wO1xuICAgICAgbGV0IHZhbHVlQXBwbGllZCA9IGlubmVyU3luY1N0eWxpbmdNYXAoXG4gICAgICAgICAgY29udGV4dCwgcmVuZGVyZXIsIGVsZW1lbnQsIGRhdGEsIGFwcGx5U3R5bGluZ0ZuLCBzYW5pdGl6ZXIsIGlubmVyTW9kZSwgaW5uZXJQcm9wLFxuICAgICAgICAgIGN1cnJlbnRNYXBJbmRleCArIDEsIGRlZmF1bHRWYWx1ZSk7XG5cbiAgICAgIGlmIChpdGVyYXRlZFRvb0Zhcikge1xuICAgICAgICBpZiAoIXRhcmdldFByb3BWYWx1ZVdhc0FwcGxpZWQpIHtcbiAgICAgICAgICB0YXJnZXRQcm9wVmFsdWVXYXNBcHBsaWVkID0gdmFsdWVBcHBsaWVkO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuXG4gICAgICBpZiAoIXZhbHVlQXBwbGllZCAmJiBpc1ZhbHVlQWxsb3dlZFRvQmVBcHBsaWVkKG1vZGUsIGlzVGFyZ2V0UHJvcE1hdGNoZWQpKSB7XG4gICAgICAgIGNvbnN0IHVzZURlZmF1bHQgPSBpc1RhcmdldFByb3BNYXRjaGVkICYmICF2YWx1ZUlzRGVmaW5lZDtcbiAgICAgICAgY29uc3QgdmFsdWVUb0FwcGx5ID0gdXNlRGVmYXVsdCA/IGRlZmF1bHRWYWx1ZSA6IHZhbHVlO1xuICAgICAgICBjb25zdCBiaW5kaW5nSW5kZXhUb0FwcGx5ID0gdXNlRGVmYXVsdCA/IGJpbmRpbmdJbmRleCA6IG51bGw7XG4gICAgICAgIGNvbnN0IGZpbmFsVmFsdWUgPSBzYW5pdGl6ZXIgP1xuICAgICAgICAgICAgc2FuaXRpemVyKHByb3AsIHZhbHVlVG9BcHBseSwgU3R5bGVTYW5pdGl6ZU1vZGUuVmFsaWRhdGVBbmRTYW5pdGl6ZSkgOlxuICAgICAgICAgICAgdmFsdWVUb0FwcGx5O1xuICAgICAgICBhcHBseVN0eWxpbmdGbihyZW5kZXJlciwgZWxlbWVudCwgcHJvcCwgZmluYWxWYWx1ZSwgYmluZGluZ0luZGV4VG9BcHBseSk7XG4gICAgICAgIHZhbHVlQXBwbGllZCA9IHRydWU7XG4gICAgICB9XG5cbiAgICAgIHRhcmdldFByb3BWYWx1ZVdhc0FwcGxpZWQgPSB2YWx1ZUFwcGxpZWQgJiYgaXNUYXJnZXRQcm9wTWF0Y2hlZDtcbiAgICAgIGN1cnNvciArPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5UdXBsZVNpemU7XG4gICAgfVxuICAgIHNldEN1cnJlbnRTeW5jQ3Vyc29yKGN1cnJlbnRNYXBJbmRleCwgY3Vyc29yKTtcblxuICAgIC8vIHRoaXMgaXMgYSBmYWxsYmFjayBjYXNlIGluIHRoZSBldmVudCB0aGF0IHRoZSBzdHlsaW5nIG1hcCBpcyBgbnVsbGAgZm9yIHRoaXNcbiAgICAvLyBiaW5kaW5nIGJ1dCB0aGVyZSBhcmUgb3RoZXIgbWFwLWJhc2VkIGJpbmRpbmdzIHRoYXQgbmVlZCB0byBiZSBldmFsdWF0ZWRcbiAgICAvLyBhZnRlcndhcmRzLiBJZiB0aGUgYHByb3BgIHZhbHVlIGlzIGZhbHN5IHRoZW4gdGhlIGludGVudGlvbiBpcyB0byBjeWNsZVxuICAgIC8vIHRocm91Z2ggYWxsIG9mIHRoZSBwcm9wZXJ0aWVzIGluIHRoZSByZW1haW5pbmcgbWFwcyBhcyB3ZWxsLiBJZiB0aGUgY3VycmVudFxuICAgIC8vIHN0eWxpbmcgbWFwIGlzIHRvbyBzaG9ydCB0aGVuIHRoZXJlIGFyZSBubyB2YWx1ZXMgdG8gaXRlcmF0ZSBvdmVyLiBJbiBlaXRoZXJcbiAgICAvLyBjYXNlIHRoZSBmb2xsb3ctdXAgbWFwcyBuZWVkIHRvIGJlIGl0ZXJhdGVkIG92ZXIuXG4gICAgaWYgKHN0eWxpbmdNYXBBcnIubGVuZ3RoID09PSBTdHlsaW5nTWFwQXJyYXlJbmRleC5WYWx1ZXNTdGFydFBvc2l0aW9uIHx8ICF0YXJnZXRQcm9wKSB7XG4gICAgICByZXR1cm4gaW5uZXJTeW5jU3R5bGluZ01hcChcbiAgICAgICAgICBjb250ZXh0LCByZW5kZXJlciwgZWxlbWVudCwgZGF0YSwgYXBwbHlTdHlsaW5nRm4sIHNhbml0aXplciwgbW9kZSwgdGFyZ2V0UHJvcCxcbiAgICAgICAgICBjdXJyZW50TWFwSW5kZXggKyAxLCBkZWZhdWx0VmFsdWUpO1xuICAgIH1cbiAgfVxuXG5cbiAgcmV0dXJuIHRhcmdldFByb3BWYWx1ZVdhc0FwcGxpZWQ7XG59XG5cblxuLyoqXG4gKiBFbmFibGVzIHN1cHBvcnQgZm9yIG1hcC1iYXNlZCBzdHlsaW5nIGJpbmRpbmdzIChlLmcuIGBbc3R5bGVdYCBhbmQgYFtjbGFzc11gIGJpbmRpbmdzKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGFjdGl2YXRlU3R5bGluZ01hcEZlYXR1cmUoKSB7XG4gIHNldFN0eWxpbmdNYXBzU3luY0ZuKHN5bmNTdHlsaW5nTWFwKTtcbn1cblxuLyoqXG4gKiBVc2VkIHRvIGRldGVybWluZSB0aGUgbW9kZSBmb3IgdGhlIGlubmVyIHJlY3Vyc2l2ZSBjYWxsLlxuICpcbiAqIElmIGFuIGlubmVyIG1hcCBpcyBpdGVyYXRlZCBvbiB0aGVuIHRoaXMgaXMgZG9uZSBzbyBmb3Igb25lXG4gKiBvZiB0d28gcmVhc29uczpcbiAqXG4gKiAtIHZhbHVlIGlzIGJlaW5nIGFwcGxpZWQ6XG4gKiAgIGlmIHRoZSB2YWx1ZSBpcyBiZWluZyBhcHBsaWVkIGZyb20gdGhpcyBjdXJyZW50IHN0eWxpbmdcbiAqICAgbWFwIHRoZW4gdGhlcmUgaXMgbm8gbmVlZCB0byBhcHBseSBpdCBpbiBhIGRlZXBlciBtYXAuXG4gKlxuICogLSB2YWx1ZSBpcyBiZWluZyBub3QgYXBwbGllZDpcbiAqICAgYXBwbHkgdGhlIHZhbHVlIGlmIGl0IGlzIGZvdW5kIGluIGEgZGVlcGVyIG1hcC5cbiAqXG4gKiBXaGVuIHRoZXNlIHJlYXNvbnMgYXJlIGVuY291bnRlcmVkIHRoZSBmbGFncyB3aWxsIGZvciB0aGVcbiAqIGlubmVyIG1hcCBtb2RlIHdpbGwgYmUgY29uZmlndXJlZC5cbiAqL1xuZnVuY3Rpb24gcmVzb2x2ZUlubmVyTWFwTW9kZShcbiAgICBjdXJyZW50TW9kZTogbnVtYmVyLCB2YWx1ZUlzRGVmaW5lZDogYm9vbGVhbiwgaXNFeGFjdE1hdGNoOiBib29sZWFuKTogbnVtYmVyIHtcbiAgbGV0IGlubmVyTW9kZSA9IGN1cnJlbnRNb2RlO1xuICBpZiAoIXZhbHVlSXNEZWZpbmVkICYmICEoY3VycmVudE1vZGUgJiBTdHlsaW5nTWFwc1N5bmNNb2RlLlNraXBUYXJnZXRQcm9wKSAmJlxuICAgICAgKGlzRXhhY3RNYXRjaCB8fCAoY3VycmVudE1vZGUgJiBTdHlsaW5nTWFwc1N5bmNNb2RlLkFwcGx5QWxsVmFsdWVzKSkpIHtcbiAgICAvLyBjYXNlIDE6IHNldCB0aGUgbW9kZSB0byBhcHBseSB0aGUgdGFyZ2V0ZWQgcHJvcCB2YWx1ZSBpZiBpdFxuICAgIC8vIGVuZHMgdXAgYmVpbmcgZW5jb3VudGVyZWQgaW4gYW5vdGhlciBtYXAgdmFsdWVcbiAgICBpbm5lck1vZGUgfD0gU3R5bGluZ01hcHNTeW5jTW9kZS5BcHBseVRhcmdldFByb3A7XG4gICAgaW5uZXJNb2RlICY9IH5TdHlsaW5nTWFwc1N5bmNNb2RlLlNraXBUYXJnZXRQcm9wO1xuICB9IGVsc2Uge1xuICAgIC8vIGNhc2UgMjogc2V0IHRoZSBtb2RlIHRvIHNraXAgdGhlIHRhcmdldGVkIHByb3AgdmFsdWUgaWYgaXRcbiAgICAvLyBlbmRzIHVwIGJlaW5nIGVuY291bnRlcmVkIGluIGFub3RoZXIgbWFwIHZhbHVlXG4gICAgaW5uZXJNb2RlIHw9IFN0eWxpbmdNYXBzU3luY01vZGUuU2tpcFRhcmdldFByb3A7XG4gICAgaW5uZXJNb2RlICY9IH5TdHlsaW5nTWFwc1N5bmNNb2RlLkFwcGx5VGFyZ2V0UHJvcDtcbiAgfVxuXG4gIHJldHVybiBpbm5lck1vZGU7XG59XG5cbi8qKlxuICogRGVjaWRlcyB3aGV0aGVyIG9yIG5vdCBhIHByb3AvdmFsdWUgZW50cnkgd2lsbCBiZSBhcHBsaWVkIHRvIGFuIGVsZW1lbnQuXG4gKlxuICogVG8gZGV0ZXJtaW5lIHdoZXRoZXIgb3Igbm90IGEgdmFsdWUgaXMgdG8gYmUgYXBwbGllZCxcbiAqIHRoZSBmb2xsb3dpbmcgcHJvY2VkdXJlIGlzIGV2YWx1YXRlZDpcbiAqXG4gKiBGaXJzdCBjaGVjayB0byBzZWUgdGhlIGN1cnJlbnQgYG1vZGVgIHN0YXR1czpcbiAqICAxLiBJZiB0aGUgbW9kZSB2YWx1ZSBwZXJtaXRzIGFsbCBwcm9wcyB0byBiZSBhcHBsaWVkIHRoZW4gYWxsb3cuXG4gKiAgICAtIEJ1dCBkbyBub3QgYWxsb3cgaWYgdGhlIGN1cnJlbnQgcHJvcCBpcyBzZXQgdG8gYmUgc2tpcHBlZC5cbiAqICAyLiBPdGhlcndpc2UgaWYgdGhlIGN1cnJlbnQgcHJvcCBpcyBwZXJtaXR0ZWQgdGhlbiBhbGxvdy5cbiAqL1xuZnVuY3Rpb24gaXNWYWx1ZUFsbG93ZWRUb0JlQXBwbGllZChtb2RlOiBudW1iZXIsIGlzVGFyZ2V0UHJvcE1hdGNoZWQ6IGJvb2xlYW4pIHtcbiAgbGV0IGRvQXBwbHlWYWx1ZSA9IChtb2RlICYgU3R5bGluZ01hcHNTeW5jTW9kZS5BcHBseUFsbFZhbHVlcykgPiAwO1xuICBpZiAoIWRvQXBwbHlWYWx1ZSkge1xuICAgIGlmIChtb2RlICYgU3R5bGluZ01hcHNTeW5jTW9kZS5BcHBseVRhcmdldFByb3ApIHtcbiAgICAgIGRvQXBwbHlWYWx1ZSA9IGlzVGFyZ2V0UHJvcE1hdGNoZWQ7XG4gICAgfVxuICB9IGVsc2UgaWYgKChtb2RlICYgU3R5bGluZ01hcHNTeW5jTW9kZS5Ta2lwVGFyZ2V0UHJvcCkgJiYgaXNUYXJnZXRQcm9wTWF0Y2hlZCkge1xuICAgIGRvQXBwbHlWYWx1ZSA9IGZhbHNlO1xuICB9XG4gIHJldHVybiBkb0FwcGx5VmFsdWU7XG59XG5cbi8qKlxuICogVXNlZCB0byBrZWVwIHRyYWNrIG9mIGNvbmN1cnJlbnQgY3Vyc29yIHZhbHVlcyBmb3IgbXVsdGlwbGUgbWFwLWJhc2VkIHN0eWxpbmcgYmluZGluZ3MgcHJlc2VudCBvblxuICogYW4gZWxlbWVudC5cbiAqL1xuY29uc3QgTUFQX0NVUlNPUlM6IG51bWJlcltdID0gW107XG5cbi8qKlxuICogVXNlZCB0byByZXNldCB0aGUgc3RhdGUgb2YgZWFjaCBjdXJzb3IgdmFsdWUgYmVpbmcgdXNlZCB0byBpdGVyYXRlIG92ZXIgbWFwLWJhc2VkIHN0eWxpbmdcbiAqIGJpbmRpbmdzLlxuICovXG5mdW5jdGlvbiByZXNldFN5bmNDdXJzb3JzKCkge1xuICBmb3IgKGxldCBpID0gMDsgaSA8IE1BUF9DVVJTT1JTLmxlbmd0aDsgaSsrKSB7XG4gICAgTUFQX0NVUlNPUlNbaV0gPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5WYWx1ZXNTdGFydFBvc2l0aW9uO1xuICB9XG59XG5cbi8qKlxuICogUmV0dXJucyBhbiBhY3RpdmUgY3Vyc29yIHZhbHVlIGF0IGEgZ2l2ZW4gbWFwSW5kZXggbG9jYXRpb24uXG4gKi9cbmZ1bmN0aW9uIGdldEN1cnJlbnRTeW5jQ3Vyc29yKG1hcEluZGV4OiBudW1iZXIpIHtcbiAgaWYgKG1hcEluZGV4ID49IE1BUF9DVVJTT1JTLmxlbmd0aCkge1xuICAgIE1BUF9DVVJTT1JTLnB1c2goU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbik7XG4gIH1cbiAgcmV0dXJuIE1BUF9DVVJTT1JTW21hcEluZGV4XTtcbn1cblxuLyoqXG4gKiBTZXRzIGEgY3Vyc29yIHZhbHVlIGF0IGEgZ2l2ZW4gbWFwSW5kZXggbG9jYXRpb24uXG4gKi9cbmZ1bmN0aW9uIHNldEN1cnJlbnRTeW5jQ3Vyc29yKG1hcEluZGV4OiBudW1iZXIsIGluZGV4VmFsdWU6IG51bWJlcikge1xuICBNQVBfQ1VSU09SU1ttYXBJbmRleF0gPSBpbmRleFZhbHVlO1xufVxuXG4vKipcbiAqIFVzZWQgdG8gY29udmVydCBhIHtrZXk6dmFsdWV9IG1hcCBpbnRvIGEgYFN0eWxpbmdNYXBBcnJheWAgYXJyYXkuXG4gKlxuICogVGhpcyBmdW5jdGlvbiB3aWxsIGVpdGhlciBnZW5lcmF0ZSBhIG5ldyBgU3R5bGluZ01hcEFycmF5YCBpbnN0YW5jZVxuICogb3IgaXQgd2lsbCBwYXRjaCB0aGUgcHJvdmlkZWQgYG5ld1ZhbHVlc2AgbWFwIHZhbHVlIGludG8gYW5cbiAqIGV4aXN0aW5nIGBTdHlsaW5nTWFwQXJyYXlgIHZhbHVlICh0aGlzIG9ubHkgaGFwcGVucyBpZiBgYmluZGluZ1ZhbHVlYFxuICogaXMgYW4gaW5zdGFuY2Ugb2YgYFN0eWxpbmdNYXBBcnJheWApLlxuICpcbiAqIElmIGEgbmV3IGtleS92YWx1ZSBtYXAgaXMgcHJvdmlkZWQgd2l0aCBhbiBvbGQgYFN0eWxpbmdNYXBBcnJheWBcbiAqIHZhbHVlIHRoZW4gYWxsIHByb3BlcnRpZXMgd2lsbCBiZSBvdmVyd3JpdHRlbiB3aXRoIHRoZWlyIG5ld1xuICogdmFsdWVzIG9yIHdpdGggYG51bGxgLiBUaGlzIG1lYW5zIHRoYXQgdGhlIGFycmF5IHdpbGwgbmV2ZXJcbiAqIHNocmluayBpbiBzaXplIChidXQgaXQgd2lsbCBhbHNvIG5vdCBiZSBjcmVhdGVkIGFuZCB0aHJvd25cbiAqIGF3YXkgd2hlbmV2ZXIgdGhlIHtrZXk6dmFsdWV9IG1hcCBlbnRyaWVzIGNoYW5nZSkuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBub3JtYWxpemVJbnRvU3R5bGluZ01hcChcbiAgICBiaW5kaW5nVmFsdWU6IG51bGwgfCBTdHlsaW5nTWFwQXJyYXksXG4gICAgbmV3VmFsdWVzOiB7W2tleTogc3RyaW5nXTogYW55fSB8IHN0cmluZyB8IG51bGwgfCB1bmRlZmluZWQsXG4gICAgbm9ybWFsaXplUHJvcHM/OiBib29sZWFuKTogU3R5bGluZ01hcEFycmF5IHtcbiAgY29uc3Qgc3R5bGluZ01hcEFycjogU3R5bGluZ01hcEFycmF5ID0gQXJyYXkuaXNBcnJheShiaW5kaW5nVmFsdWUpID8gYmluZGluZ1ZhbHVlIDogW251bGxdO1xuICBzdHlsaW5nTWFwQXJyW1N0eWxpbmdNYXBBcnJheUluZGV4LlJhd1ZhbHVlUG9zaXRpb25dID0gbmV3VmFsdWVzIHx8IG51bGw7XG5cbiAgLy8gYmVjYXVzZSB0aGUgbmV3IHZhbHVlcyBtYXkgbm90IGluY2x1ZGUgYWxsIHRoZSBwcm9wZXJ0aWVzXG4gIC8vIHRoYXQgdGhlIG9sZCBvbmVzIGhhZCwgYWxsIHZhbHVlcyBhcmUgc2V0IHRvIGBudWxsYCBiZWZvcmVcbiAgLy8gdGhlIG5ldyB2YWx1ZXMgYXJlIGFwcGxpZWQuIFRoaXMgd2F5LCB3aGVuIGZsdXNoZWQsIHRoZVxuICAvLyBzdHlsaW5nIGFsZ29yaXRobSBrbm93cyBleGFjdGx5IHdoYXQgc3R5bGUvY2xhc3MgdmFsdWVzXG4gIC8vIHRvIHJlbW92ZSBmcm9tIHRoZSBlbGVtZW50IChzaW5jZSB0aGV5IGFyZSBgbnVsbGApLlxuICBmb3IgKGxldCBqID0gU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbjsgaiA8IHN0eWxpbmdNYXBBcnIubGVuZ3RoO1xuICAgICAgIGogKz0gU3R5bGluZ01hcEFycmF5SW5kZXguVHVwbGVTaXplKSB7XG4gICAgc2V0TWFwVmFsdWUoc3R5bGluZ01hcEFyciwgaiwgbnVsbCk7XG4gIH1cblxuICBsZXQgcHJvcHM6IHN0cmluZ1tdfG51bGwgPSBudWxsO1xuICBsZXQgbWFwOiB7W2tleTogc3RyaW5nXTogYW55fXx1bmRlZmluZWR8bnVsbDtcbiAgbGV0IGFsbFZhbHVlc1RydWUgPSBmYWxzZTtcbiAgaWYgKHR5cGVvZiBuZXdWYWx1ZXMgPT09ICdzdHJpbmcnKSB7ICAvLyBbY2xhc3NdIGJpbmRpbmdzIGFsbG93IHN0cmluZyB2YWx1ZXNcbiAgICBpZiAobmV3VmFsdWVzLmxlbmd0aCkge1xuICAgICAgcHJvcHMgPSBuZXdWYWx1ZXMuc3BsaXQoL1xccysvKTtcbiAgICAgIGFsbFZhbHVlc1RydWUgPSB0cnVlO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBwcm9wcyA9IG5ld1ZhbHVlcyA/IE9iamVjdC5rZXlzKG5ld1ZhbHVlcykgOiBudWxsO1xuICAgIG1hcCA9IG5ld1ZhbHVlcztcbiAgfVxuXG4gIGlmIChwcm9wcykge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgcHJvcHMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IHByb3AgPSBwcm9wc1tpXSBhcyBzdHJpbmc7XG4gICAgICBjb25zdCBuZXdQcm9wID0gbm9ybWFsaXplUHJvcHMgPyBoeXBoZW5hdGUocHJvcCkgOiBwcm9wO1xuICAgICAgY29uc3QgdmFsdWUgPSBhbGxWYWx1ZXNUcnVlID8gdHJ1ZSA6IG1hcCAhW3Byb3BdO1xuICAgICAgYWRkSXRlbVRvU3R5bGluZ01hcChzdHlsaW5nTWFwQXJyLCBuZXdQcm9wLCB2YWx1ZSwgdHJ1ZSk7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIHN0eWxpbmdNYXBBcnI7XG59XG5cbi8qKlxuICogSW5zZXJ0cyB0aGUgcHJvdmlkZWQgaXRlbSBpbnRvIHRoZSBwcm92aWRlZCBzdHlsaW5nIGFycmF5IGF0IHRoZSByaWdodCBzcG90LlxuICpcbiAqIFRoZSBgU3R5bGluZ01hcEFycmF5YCB0eXBlIGlzIGEgc29ydGVkIGtleS92YWx1ZSBhcnJheSBvZiBlbnRyaWVzLiBUaGlzIG1lYW5zXG4gKiB0aGF0IHdoZW4gYSBuZXcgZW50cnkgaXMgaW5zZXJ0ZWQgaXQgbXVzdCBiZSBwbGFjZWQgYXQgdGhlIHJpZ2h0IHNwb3QgaW4gdGhlXG4gKiBhcnJheS4gVGhpcyBmdW5jdGlvbiBmaWd1cmVzIG91dCBleGFjdGx5IHdoZXJlIHRvIHBsYWNlIGl0LlxuICovXG5leHBvcnQgZnVuY3Rpb24gYWRkSXRlbVRvU3R5bGluZ01hcChcbiAgICBzdHlsaW5nTWFwQXJyOiBTdHlsaW5nTWFwQXJyYXksIHByb3A6IHN0cmluZywgdmFsdWU6IHN0cmluZyB8IGJvb2xlYW4gfCBudWxsLFxuICAgIGFsbG93T3ZlcndyaXRlPzogYm9vbGVhbikge1xuICBmb3IgKGxldCBqID0gU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbjsgaiA8IHN0eWxpbmdNYXBBcnIubGVuZ3RoO1xuICAgICAgIGogKz0gU3R5bGluZ01hcEFycmF5SW5kZXguVHVwbGVTaXplKSB7XG4gICAgY29uc3QgcHJvcEF0SW5kZXggPSBnZXRNYXBQcm9wKHN0eWxpbmdNYXBBcnIsIGopO1xuICAgIGlmIChwcm9wIDw9IHByb3BBdEluZGV4KSB7XG4gICAgICBsZXQgYXBwbGllZCA9IGZhbHNlO1xuICAgICAgaWYgKHByb3BBdEluZGV4ID09PSBwcm9wKSB7XG4gICAgICAgIGNvbnN0IHZhbHVlQXRJbmRleCA9IHN0eWxpbmdNYXBBcnJbal07XG4gICAgICAgIGlmIChhbGxvd092ZXJ3cml0ZSB8fCAhaXNTdHlsaW5nVmFsdWVEZWZpbmVkKHZhbHVlQXRJbmRleCkpIHtcbiAgICAgICAgICBhcHBsaWVkID0gdHJ1ZTtcbiAgICAgICAgICBzZXRNYXBWYWx1ZShzdHlsaW5nTWFwQXJyLCBqLCB2YWx1ZSk7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGFwcGxpZWQgPSB0cnVlO1xuICAgICAgICBzdHlsaW5nTWFwQXJyLnNwbGljZShqLCAwLCBwcm9wLCB2YWx1ZSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gYXBwbGllZDtcbiAgICB9XG4gIH1cblxuICBzdHlsaW5nTWFwQXJyLnB1c2gocHJvcCwgdmFsdWUpO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuLyoqXG4gKiBDb252ZXJ0cyB0aGUgcHJvdmlkZWQgc3R5bGluZyBtYXAgYXJyYXkgaW50byBhIHN0cmluZy5cbiAqXG4gKiBDbGFzc2VzID0+IGBvbmUgdHdvIHRocmVlYFxuICogU3R5bGVzID0+IGBwcm9wOnZhbHVlOyBwcm9wMjp2YWx1ZTJgXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBzdHlsaW5nTWFwVG9TdHJpbmcobWFwOiBTdHlsaW5nTWFwQXJyYXksIGlzQ2xhc3NCYXNlZDogYm9vbGVhbik6IHN0cmluZyB7XG4gIGxldCBzdHIgPSAnJztcbiAgZm9yIChsZXQgaSA9IFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb247IGkgPCBtYXAubGVuZ3RoO1xuICAgICAgIGkgKz0gU3R5bGluZ01hcEFycmF5SW5kZXguVHVwbGVTaXplKSB7XG4gICAgY29uc3QgcHJvcCA9IGdldE1hcFByb3AobWFwLCBpKTtcbiAgICBjb25zdCB2YWx1ZSA9IGdldE1hcFZhbHVlKG1hcCwgaSkgYXMgc3RyaW5nO1xuICAgIGNvbnN0IGF0dHJWYWx1ZSA9IGNvbmNhdFN0cmluZyhwcm9wLCBpc0NsYXNzQmFzZWQgPyAnJyA6IHZhbHVlLCAnOicpO1xuICAgIHN0ciA9IGNvbmNhdFN0cmluZyhzdHIsIGF0dHJWYWx1ZSwgaXNDbGFzc0Jhc2VkID8gJyAnIDogJzsgJyk7XG4gIH1cbiAgcmV0dXJuIHN0cjtcbn1cblxuLyoqXG4gKiBDb252ZXJ0cyB0aGUgcHJvdmlkZWQgc3R5bGluZyBtYXAgYXJyYXkgaW50byBhIGtleSB2YWx1ZSBtYXAuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBzdHlsaW5nTWFwVG9TdHJpbmdNYXAobWFwOiBTdHlsaW5nTWFwQXJyYXkgfCBudWxsKToge1trZXk6IHN0cmluZ106IGFueX0ge1xuICBsZXQgc3RyaW5nTWFwOiB7W2tleTogc3RyaW5nXTogYW55fSA9IHt9O1xuICBpZiAobWFwKSB7XG4gICAgZm9yIChsZXQgaSA9IFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb247IGkgPCBtYXAubGVuZ3RoO1xuICAgICAgICAgaSArPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5UdXBsZVNpemUpIHtcbiAgICAgIGNvbnN0IHByb3AgPSBnZXRNYXBQcm9wKG1hcCwgaSk7XG4gICAgICBjb25zdCB2YWx1ZSA9IGdldE1hcFZhbHVlKG1hcCwgaSkgYXMgc3RyaW5nO1xuICAgICAgc3RyaW5nTWFwW3Byb3BdID0gdmFsdWU7XG4gICAgfVxuICB9XG4gIHJldHVybiBzdHJpbmdNYXA7XG59XG4iXX0=