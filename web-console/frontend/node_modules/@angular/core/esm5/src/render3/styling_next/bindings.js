import { RendererStyleFlags3, isProceduralRenderer } from '../interfaces/renderer';
import { BIT_MASK_START_VALUE, deleteStylingStateFromStorage, getStylingState, resetStylingState, storeStylingState } from './state';
import { allowStylingFlush, getBindingValue, getGuardMask, getMapProp, getMapValue, getProp, getPropValuesStartPosition, getStylingMapArray, getValuesCount, hasValueChanged, isContextLocked, isSanitizationRequired, isStylingValueDefined, lockContext, setGuardMask, stateIsPersisted } from './util';
/**
 * --------
 *
 * This file contains the core logic for styling in Angular.
 *
 * All styling bindings (i.e. `[style]`, `[style.prop]`, `[class]` and `[class.name]`)
 * will have their values be applied through the logic in this file.
 *
 * When a binding is encountered (e.g. `<div [style.width]="w">`) then
 * the binding data will be populated into a `TStylingContext` data-structure.
 * There is only one `TStylingContext` per `TNode` and each element instance
 * will update its style/class binding values in concert with the styling
 * context.
 *
 * To learn more about the algorithm see `TStylingContext`.
 *
 * --------
 */
// The first bit value reflects a map-based binding value's bit.
// The reason why it's always activated for every entry in the map
// is so that if any map-binding values update then all other prop
// based bindings will pass the guard check automatically without
// any extra code or flags.
export var DEFAULT_GUARD_MASK_VALUE = 1;
/**
 * The guard/update mask bit index location for map-based bindings.
 *
 * All map-based bindings (i.e. `[style]` and `[class]` )
 */
var STYLING_INDEX_FOR_MAP_BINDING = 0;
/**
 * Default fallback value for a styling binding.
 *
 * A value of `null` is used here which signals to the styling algorithm that
 * the styling value is not present. This way if there are no other values
 * detected then it will be removed once the style/class property is dirty and
 * diffed within the styling algorithm present in `flushStyling`.
 */
var DEFAULT_BINDING_VALUE = null;
/**
 * Default size count value for a new entry in a context.
 *
 * A value of `1` is used here because each entry in the context has a default
 * property.
 */
var DEFAULT_SIZE_VALUE = 1;
var deferredBindingQueue = [];
/**
 * Visits a class-based binding and updates the new value (if changed).
 *
 * This function is called each time a class-based styling instruction
 * is executed. It's important that it's always called (even if the value
 * has not changed) so that the inner counter index value is incremented.
 * This way, each instruction is always guaranteed to get the same counter
 * state each time it's called (which then allows the `TStylingContext`
 * and the bit mask values to be in sync).
 */
export function updateClassBinding(context, data, element, prop, bindingIndex, value, deferRegistration, forceUpdate) {
    var isMapBased = !prop;
    var state = getStylingState(element, stateIsPersisted(context));
    var index = isMapBased ? STYLING_INDEX_FOR_MAP_BINDING : state.classesIndex++;
    var updated = updateBindingData(context, data, index, prop, bindingIndex, value, deferRegistration, forceUpdate, false);
    if (updated || forceUpdate) {
        // We flip the bit in the bitMask to reflect that the binding
        // at the `index` slot has changed. This identifies to the flushing
        // phase that the bindings for this particular CSS class need to be
        // applied again because on or more of the bindings for the CSS
        // class have changed.
        state.classesBitMask |= 1 << index;
        return true;
    }
    return false;
}
/**
 * Visits a style-based binding and updates the new value (if changed).
 *
 * This function is called each time a style-based styling instruction
 * is executed. It's important that it's always called (even if the value
 * has not changed) so that the inner counter index value is incremented.
 * This way, each instruction is always guaranteed to get the same counter
 * state each time it's called (which then allows the `TStylingContext`
 * and the bit mask values to be in sync).
 */
export function updateStyleBinding(context, data, element, prop, bindingIndex, value, sanitizer, deferRegistration, forceUpdate) {
    var isMapBased = !prop;
    var state = getStylingState(element, stateIsPersisted(context));
    var index = isMapBased ? STYLING_INDEX_FOR_MAP_BINDING : state.stylesIndex++;
    var sanitizationRequired = isMapBased ?
        true :
        (sanitizer ? sanitizer(prop, null, 1 /* ValidateProperty */) : false);
    var updated = updateBindingData(context, data, index, prop, bindingIndex, value, deferRegistration, forceUpdate, sanitizationRequired);
    if (updated || forceUpdate) {
        // We flip the bit in the bitMask to reflect that the binding
        // at the `index` slot has changed. This identifies to the flushing
        // phase that the bindings for this particular property need to be
        // applied again because on or more of the bindings for the CSS
        // property have changed.
        state.stylesBitMask |= 1 << index;
        return true;
    }
    return false;
}
/**
 * Called each time a binding value has changed within the provided `TStylingContext`.
 *
 * This function is designed to be called from `updateStyleBinding` and `updateClassBinding`.
 * If called during the first update pass, the binding will be registered in the context.
 * If the binding does get registered and the `deferRegistration` flag is true then the
 * binding data will be queued up until the context is later flushed in `applyStyling`.
 *
 * This function will also update binding slot in the provided `LStylingData` with the
 * new binding entry (if it has changed).
 *
 * @returns whether or not the binding value was updated in the `LStylingData`.
 */
function updateBindingData(context, data, counterIndex, prop, bindingIndex, value, deferRegistration, forceUpdate, sanitizationRequired) {
    if (!isContextLocked(context)) {
        if (deferRegistration) {
            deferBindingRegistration(context, counterIndex, prop, bindingIndex, sanitizationRequired);
        }
        else {
            deferredBindingQueue.length && flushDeferredBindings();
            // this will only happen during the first update pass of the
            // context. The reason why we can't use `tNode.firstTemplatePass`
            // here is because its not guaranteed to be true when the first
            // update pass is executed (remember that all styling instructions
            // are run in the update phase, and, as a result, are no more
            // styling instructions that are run in the creation phase).
            registerBinding(context, counterIndex, prop, bindingIndex, sanitizationRequired);
        }
    }
    var changed = forceUpdate || hasValueChanged(data[bindingIndex], value);
    if (changed) {
        data[bindingIndex] = value;
    }
    return changed;
}
/**
 * Schedules a binding registration to be run at a later point.
 *
 * The reasoning for this feature is to ensure that styling
 * bindings are registered in the correct order for when
 * directives/components have a super/sub class inheritance
 * chains. Each directive's styling bindings must be
 * registered into the context in reverse order. Therefore all
 * bindings will be buffered in reverse order and then applied
 * after the inheritance chain exits.
 */
function deferBindingRegistration(context, counterIndex, prop, bindingIndex, sanitizationRequired) {
    deferredBindingQueue.unshift(context, counterIndex, prop, bindingIndex, sanitizationRequired);
}
/**
 * Flushes the collection of deferred bindings and causes each entry
 * to be registered into the context.
 */
function flushDeferredBindings() {
    var i = 0;
    while (i < deferredBindingQueue.length) {
        var context = deferredBindingQueue[i++];
        var count = deferredBindingQueue[i++];
        var prop = deferredBindingQueue[i++];
        var bindingIndex = deferredBindingQueue[i++];
        var sanitizationRequired = deferredBindingQueue[i++];
        registerBinding(context, count, prop, bindingIndex, sanitizationRequired);
    }
    deferredBindingQueue.length = 0;
}
/**
 * Registers the provided binding (prop + bindingIndex) into the context.
 *
 * This function is shared between bindings that are assigned immediately
 * (via `updateBindingData`) and at a deferred stage. When called, it will
 * figure out exactly where to place the binding data in the context.
 *
 * It is needed because it will either update or insert a styling property
 * into the context at the correct spot.
 *
 * When called, one of two things will happen:
 *
 * 1) If the property already exists in the context then it will just add
 *    the provided `bindingValue` to the end of the binding sources region
 *    for that particular property.
 *
 *    - If the binding value is a number then it will be added as a new
 *      binding index source next to the other binding sources for the property.
 *
 *    - Otherwise, if the binding value is a string/boolean/null type then it will
 *      replace the default value for the property if the default value is `null`.
 *
 * 2) If the property does not exist then it will be inserted into the context.
 *    The styling context relies on all properties being stored in alphabetical
 *    order, so it knows exactly where to store it.
 *
 *    When inserted, a default `null` value is created for the property which exists
 *    as the default value for the binding. If the bindingValue property is inserted
 *    and it is either a string, number or null value then that will replace the default
 *    value.
 *
 * Note that this function is also used for map-based styling bindings. They are treated
 * much the same as prop-based bindings, but, because they do not have a property value
 * (since it's a map), all map-based entries are stored in an already populated area of
 * the context at the top (which is reserved for map-based entries).
 */
export function registerBinding(context, countId, prop, bindingValue, sanitizationRequired) {
    var registered = false;
    if (prop) {
        // prop-based bindings (e.g `<div [style.width]="w" [class.foo]="f">`)
        var found = false;
        var i = getPropValuesStartPosition(context);
        while (i < context.length) {
            var valuesCount = getValuesCount(context, i);
            var p = getProp(context, i);
            found = prop <= p;
            if (found) {
                // all style/class bindings are sorted by property name
                if (prop < p) {
                    allocateNewContextEntry(context, i, prop, sanitizationRequired);
                }
                addBindingIntoContext(context, false, i, bindingValue, countId);
                break;
            }
            i += 3 /* BindingsStartOffset */ + valuesCount;
        }
        if (!found) {
            allocateNewContextEntry(context, context.length, prop, sanitizationRequired);
            addBindingIntoContext(context, false, i, bindingValue, countId);
            registered = true;
        }
    }
    else {
        // map-based bindings (e.g `<div [style]="s" [class]="{className:true}">`)
        // there is no need to allocate the map-based binding region into the context
        // since it is already there when the context is first created.
        addBindingIntoContext(context, true, 3 /* MapBindingsPosition */, bindingValue, countId);
        registered = true;
    }
    return registered;
}
function allocateNewContextEntry(context, index, prop, sanitizationRequired) {
    // 1,2: splice index locations
    // 3: each entry gets a config value (guard mask + flags)
    // 4. each entry gets a size value (which is always one because there is always a default binding
    // value)
    // 5. the property that is getting allocated into the context
    // 6. the default binding value (usually `null`)
    var config = sanitizationRequired ? 1 /* SanitizationRequired */ :
        0 /* Default */;
    context.splice(index, 0, config, DEFAULT_SIZE_VALUE, prop, DEFAULT_BINDING_VALUE);
    setGuardMask(context, index, DEFAULT_GUARD_MASK_VALUE);
}
/**
 * Inserts a new binding value into a styling property tuple in the `TStylingContext`.
 *
 * A bindingValue is inserted into a context during the first update pass
 * of a template or host bindings function. When this occurs, two things
 * happen:
 *
 * - If the bindingValue value is a number then it is treated as a bindingIndex
 *   value (a index in the `LView`) and it will be inserted next to the other
 *   binding index entries.
 *
 * - Otherwise the binding value will update the default value for the property
 *   and this will only happen if the default value is `null`.
 *
 * Note that this function also handles map-based bindings and will insert them
 * at the top of the context.
 */
function addBindingIntoContext(context, isMapBased, index, bindingValue, countId) {
    var valuesCount = getValuesCount(context, index);
    var firstValueIndex = index + 3 /* BindingsStartOffset */;
    var lastValueIndex = firstValueIndex + valuesCount;
    if (!isMapBased) {
        // prop-based values all have default values, but map-based entries do not.
        // we want to access the index for the default value in this case and not just
        // the bindings...
        lastValueIndex--;
    }
    if (typeof bindingValue === 'number') {
        // the loop here will check to see if the binding already exists
        // for the property in the context. Why? The reason for this is
        // because the styling context is not "locked" until the first
        // flush has occurred. This means that if a repeated element
        // registers its styling bindings then it will register each
        // binding more than once (since its duplicated). This check
        // will prevent that from happening. Note that this only happens
        // when a binding is first encountered and not each time it is
        // updated.
        for (var i = firstValueIndex; i <= lastValueIndex; i++) {
            var indexAtPosition = context[i];
            if (indexAtPosition === bindingValue)
                return;
        }
        context.splice(lastValueIndex, 0, bindingValue);
        context[index + 1 /* ValuesCountOffset */]++;
        // now that a new binding index has been added to the property
        // the guard mask bit value (at the `countId` position) needs
        // to be included into the existing mask value.
        var guardMask = getGuardMask(context, index) | (1 << countId);
        setGuardMask(context, index, guardMask);
    }
    else if (bindingValue !== null && context[lastValueIndex] == null) {
        context[lastValueIndex] = bindingValue;
    }
}
/**
 * Applies all pending style and class bindings to the provided element.
 *
 * This function will attempt to flush styling via the provided `classesContext`
 * and `stylesContext` context values. This function is designed to be run from
 * the `stylingApply()` instruction (which is run at the very end of styling
 * change detection) and will rely on any state values that are set from when
 * any styling bindings update.
 *
 * This function may be called multiple times on the same element because it can
 * be called from the template code as well as from host bindings. In order for
 * styling to be successfully flushed to the element (which will only happen once
 * despite this being called multiple times), the following criteria must be met:
 *
 * - `flushStyling` is called from the very last directive that has styling for
 *    the element (see `allowStylingFlush()`).
 * - one or more bindings for classes or styles has updated (this is checked by
 *   examining the classes or styles bit mask).
 *
 * If the style and class values are successfully applied to the element then
 * the temporary state values for the element will be cleared. Otherwise, if
 * this did not occur then the styling state is persisted (see `state.ts` for
 * more information on how this works).
 */
export function flushStyling(renderer, data, classesContext, stylesContext, element, directiveIndex, styleSanitizer) {
    ngDevMode && ngDevMode.flushStyling++;
    var persistState = classesContext ? stateIsPersisted(classesContext) :
        (stylesContext ? stateIsPersisted(stylesContext) : false);
    var allowFlushClasses = allowStylingFlush(classesContext, directiveIndex);
    var allowFlushStyles = allowStylingFlush(stylesContext, directiveIndex);
    // deferred bindings are bindings which are scheduled to register with
    // the context at a later point. These bindings can only registered when
    // the context will be 100% flushed to the element.
    if (deferredBindingQueue.length && (allowFlushClasses || allowFlushStyles)) {
        flushDeferredBindings();
    }
    var state = getStylingState(element, persistState);
    var classesFlushed = maybeApplyStyling(renderer, element, data, classesContext, allowFlushClasses, state.classesBitMask, setClass, null);
    var stylesFlushed = maybeApplyStyling(renderer, element, data, stylesContext, allowFlushStyles, state.stylesBitMask, setStyle, styleSanitizer);
    if (classesFlushed && stylesFlushed) {
        resetStylingState();
        if (persistState) {
            deleteStylingStateFromStorage(element);
        }
    }
    else if (persistState) {
        storeStylingState(element, state);
    }
}
function maybeApplyStyling(renderer, element, data, context, allowFlush, bitMask, styleSetter, styleSanitizer) {
    if (allowFlush && context) {
        lockAndFinalizeContext(context);
        if (contextHasUpdates(context, bitMask)) {
            ngDevMode && (styleSanitizer ? ngDevMode.stylesApplied++ : ngDevMode.classesApplied++);
            applyStyling(context, renderer, element, data, bitMask, styleSetter, styleSanitizer);
            return true;
        }
    }
    return allowFlush;
}
function contextHasUpdates(context, bitMask) {
    return context && bitMask > BIT_MASK_START_VALUE;
}
/**
 * Locks the context (so no more bindings can be added) and also copies over initial class/style
 * values into their binding areas.
 *
 * There are two main actions that take place in this function:
 *
 * - Locking the context:
 *   Locking the context is required so that the style/class instructions know NOT to
 *   register a binding again after the first update pass has run. If a locking bit was
 *   not used then it would need to scan over the context each time an instruction is run
 *   (which is expensive).
 *
 * - Patching initial values:
 *   Directives and component host bindings may include static class/style values which are
 *   bound to the host element. When this happens, the styling context will need to be informed
 *   so it can use these static styling values as defaults when a matching binding is falsy.
 *   These initial styling values are read from the initial styling values slot within the
 *   provided `TStylingContext` (which is an instance of a `StylingMapArray`). This inner map will
 *   be updated each time a host binding applies its static styling values (via `elementHostAttrs`)
 *   so these values are only read at this point because this is the very last point before the
 *   first style/class values are flushed to the element.
 */
function lockAndFinalizeContext(context) {
    if (!isContextLocked(context)) {
        var initialValues = getStylingMapArray(context);
        if (initialValues) {
            updateInitialStylingOnContext(context, initialValues);
        }
        lockContext(context);
    }
}
/**
 * Runs through the provided styling context and applies each value to
 * the provided element (via the renderer) if one or more values are present.
 *
 * This function will iterate over all entries present in the provided
 * `TStylingContext` array (both prop-based and map-based bindings).-
 *
 * Each entry, within the `TStylingContext` array, is stored alphabetically
 * and this means that each prop/value entry will be applied in order
 * (so long as it is marked dirty in the provided `bitMask` value).
 *
 * If there are any map-based entries present (which are applied to the
 * element via the `[style]` and `[class]` bindings) then those entries
 * will be applied as well. However, the code for that is not a part of
 * this function. Instead, each time a property is visited, then the
 * code below will call an external function called `stylingMapsSyncFn`
 * and, if present, it will keep the application of styling values in
 * map-based bindings up to sync with the application of prop-based
 * bindings.
 *
 * Visit `styling_next/map_based_bindings.ts` to learn more about how the
 * algorithm works for map-based styling bindings.
 *
 * Note that this function is not designed to be called in isolation (use
 * `applyClasses` and `applyStyles` to actually apply styling values).
 */
export function applyStyling(context, renderer, element, bindingData, bitMaskValue, applyStylingFn, sanitizer) {
    var bitMask = normalizeBitMaskValue(bitMaskValue);
    var stylingMapsSyncFn = getStylingMapsSyncFn();
    var mapsGuardMask = getGuardMask(context, 3 /* MapBindingsPosition */);
    var applyAllValues = (bitMask & mapsGuardMask) > 0;
    var mapsMode = applyAllValues ? 1 /* ApplyAllValues */ : 0 /* TraverseValues */;
    var i = getPropValuesStartPosition(context);
    while (i < context.length) {
        var valuesCount = getValuesCount(context, i);
        var guardMask = getGuardMask(context, i);
        if (bitMask & guardMask) {
            var valueApplied = false;
            var prop = getProp(context, i);
            var valuesCountUpToDefault = valuesCount - 1;
            var defaultValue = getBindingValue(context, i, valuesCountUpToDefault);
            // case 1: apply prop-based values
            // try to apply the binding values and see if a non-null
            // value gets set for the styling binding
            for (var j = 0; j < valuesCountUpToDefault; j++) {
                var bindingIndex = getBindingValue(context, i, j);
                var value = bindingData[bindingIndex];
                if (isStylingValueDefined(value)) {
                    var finalValue = sanitizer && isSanitizationRequired(context, i) ?
                        sanitizer(prop, value, 2 /* SanitizeOnly */) :
                        value;
                    applyStylingFn(renderer, element, prop, finalValue, bindingIndex);
                    valueApplied = true;
                    break;
                }
            }
            // case 2: apply map-based values
            // traverse through each map-based styling binding and update all values up to
            // the provided `prop` value. If the property was not applied in the loop above
            // then it will be attempted to be applied in the maps sync code below.
            if (stylingMapsSyncFn) {
                // determine whether or not to apply the target property or to skip it
                var mode = mapsMode | (valueApplied ? 4 /* SkipTargetProp */ :
                    2 /* ApplyTargetProp */);
                var valueAppliedWithinMap = stylingMapsSyncFn(context, renderer, element, bindingData, applyStylingFn, sanitizer, mode, prop, defaultValue);
                valueApplied = valueApplied || valueAppliedWithinMap;
            }
            // case 3: apply the default value
            // if the value has not yet been applied then a truthy value does not exist in the
            // prop-based or map-based bindings code. If and when this happens, just apply the
            // default value (even if the default value is `null`).
            if (!valueApplied) {
                applyStylingFn(renderer, element, prop, defaultValue);
            }
        }
        i += 3 /* BindingsStartOffset */ + valuesCount;
    }
    // the map-based styling entries may have not applied all their
    // values. For this reason, one more call to the sync function
    // needs to be issued at the end.
    if (stylingMapsSyncFn) {
        stylingMapsSyncFn(context, renderer, element, bindingData, applyStylingFn, sanitizer, mapsMode);
    }
}
function normalizeBitMaskValue(value) {
    // if pass => apply all values (-1 implies that all bits are flipped to true)
    if (value === true)
        return -1;
    // if pass => skip all values
    if (value === false)
        return 0;
    // return the bit mask value as is
    return value;
}
var _activeStylingMapApplyFn = null;
export function getStylingMapsSyncFn() {
    return _activeStylingMapApplyFn;
}
export function setStylingMapsSyncFn(fn) {
    _activeStylingMapApplyFn = fn;
}
/**
 * Assigns a style value to a style property for the given element.
 */
var setStyle = function (renderer, native, prop, value) {
    // the reason why this may be `null` is either because
    // it's a container element or it's a part of a test
    // environment that doesn't have styling. In either
    // case it's safe not to apply styling to the element.
    var nativeStyle = native.style;
    if (value) {
        // opacity, z-index and flexbox all have number values
        // and these need to be converted into strings so that
        // they can be assigned properly.
        value = value.toString();
        ngDevMode && ngDevMode.rendererSetStyle++;
        renderer && isProceduralRenderer(renderer) ?
            renderer.setStyle(native, prop, value, RendererStyleFlags3.DashCase) :
            (nativeStyle && nativeStyle.setProperty(prop, value));
    }
    else {
        ngDevMode && ngDevMode.rendererRemoveStyle++;
        renderer && isProceduralRenderer(renderer) ?
            renderer.removeStyle(native, prop, RendererStyleFlags3.DashCase) :
            (nativeStyle && nativeStyle.removeProperty(prop));
    }
};
var ɵ0 = setStyle;
/**
 * Adds/removes the provided className value to the provided element.
 */
var setClass = function (renderer, native, className, value) {
    if (className !== '') {
        // the reason why this may be `null` is either because
        // it's a container element or it's a part of a test
        // environment that doesn't have styling. In either
        // case it's safe not to apply styling to the element.
        var classList = native.classList;
        if (value) {
            ngDevMode && ngDevMode.rendererAddClass++;
            renderer && isProceduralRenderer(renderer) ? renderer.addClass(native, className) :
                (classList && classList.add(className));
        }
        else {
            ngDevMode && ngDevMode.rendererRemoveClass++;
            renderer && isProceduralRenderer(renderer) ? renderer.removeClass(native, className) :
                (classList && classList.remove(className));
        }
    }
};
var ɵ1 = setClass;
/**
 * Iterates over all provided styling entries and renders them on the element.
 *
 * This function is used alongside a `StylingMapArray` entry. This entry is not
 * the same as the `TStylingContext` and is only really used when an element contains
 * initial styling values (e.g. `<div style="width:200px">`), but no style/class bindings
 * are present. If and when that happens then this function will be called to render all
 * initial styling values on an element.
 */
export function renderStylingMap(renderer, element, stylingValues, isClassBased) {
    var stylingMapArr = getStylingMapArray(stylingValues);
    if (stylingMapArr) {
        for (var i = 1 /* ValuesStartPosition */; i < stylingMapArr.length; i += 2 /* TupleSize */) {
            var prop = getMapProp(stylingMapArr, i);
            var value = getMapValue(stylingMapArr, i);
            if (isClassBased) {
                setClass(renderer, element, prop, value, null);
            }
            else {
                setStyle(renderer, element, prop, value, null);
            }
        }
    }
}
/**
 * Registers all initial styling entries into the provided context.
 *
 * This function will iterate over all entries in the provided `initialStyling` ar}ray and register
 * them as default (initial) values in the provided context. Initial styling values in a context are
 * the default values that are to be applied unless overwritten by a binding.
 *
 * The reason why this function exists and isn't a part of the context construction is because
 * host binding is evaluated at a later stage after the element is created. This means that
 * if a directive or component contains any initial styling code (i.e. `<div class="foo">`)
 * then that initial styling data can only be applied once the styling for that element
 * is first applied (at the end of the update phase). Once that happens then the context will
 * update itself with the complete initial styling for the element.
 */
function updateInitialStylingOnContext(context, initialStyling) {
    // `-1` is used here because all initial styling data is not a spart
    // of a binding (since it's static)
    var INITIAL_STYLING_COUNT_ID = -1;
    for (var i = 1 /* ValuesStartPosition */; i < initialStyling.length; i += 2 /* TupleSize */) {
        var value = getMapValue(initialStyling, i);
        if (value) {
            var prop = getMapProp(initialStyling, i);
            registerBinding(context, INITIAL_STYLING_COUNT_ID, prop, value, false);
        }
    }
}
export { ɵ0, ɵ1 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYmluZGluZ3MuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL3N0eWxpbmdfbmV4dC9iaW5kaW5ncy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFRQSxPQUFPLEVBQTJDLG1CQUFtQixFQUFFLG9CQUFvQixFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFHM0gsT0FBTyxFQUFDLG9CQUFvQixFQUFFLDZCQUE2QixFQUFFLGVBQWUsRUFBRSxpQkFBaUIsRUFBRSxpQkFBaUIsRUFBQyxNQUFNLFNBQVMsQ0FBQztBQUNuSSxPQUFPLEVBQUMsaUJBQWlCLEVBQUUsZUFBZSxFQUFFLFlBQVksRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLE9BQU8sRUFBRSwwQkFBMEIsRUFBRSxrQkFBa0IsRUFBRSxjQUFjLEVBQUUsZUFBZSxFQUFFLGVBQWUsRUFBRSxzQkFBc0IsRUFBRSxxQkFBcUIsRUFBRSxXQUFXLEVBQUUsWUFBWSxFQUFFLGdCQUFnQixFQUFDLE1BQU0sUUFBUSxDQUFDO0FBSXhTOzs7Ozs7Ozs7Ozs7Ozs7OztHQWlCRztBQUVILGdFQUFnRTtBQUNoRSxrRUFBa0U7QUFDbEUsa0VBQWtFO0FBQ2xFLGlFQUFpRTtBQUNqRSwyQkFBMkI7QUFDM0IsTUFBTSxDQUFDLElBQU0sd0JBQXdCLEdBQUcsQ0FBRyxDQUFDO0FBRTVDOzs7O0dBSUc7QUFDSCxJQUFNLDZCQUE2QixHQUFHLENBQUMsQ0FBQztBQUV4Qzs7Ozs7OztHQU9HO0FBQ0gsSUFBTSxxQkFBcUIsR0FBRyxJQUFJLENBQUM7QUFFbkM7Ozs7O0dBS0c7QUFDSCxJQUFNLGtCQUFrQixHQUFHLENBQUMsQ0FBQztBQUU3QixJQUFJLG9CQUFvQixHQUEyRCxFQUFFLENBQUM7QUFFdEY7Ozs7Ozs7OztHQVNHO0FBQ0gsTUFBTSxVQUFVLGtCQUFrQixDQUM5QixPQUF3QixFQUFFLElBQWtCLEVBQUUs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bUNBQW1DO0lBQ25DLElBQU0sd0JBQXdCLEdBQUcsQ0FBQyxDQUFDLENBQUM7SUFFcEMsS0FBSyxJQUFJLENBQUMsOEJBQTJDLEVBQUUsQ0FBQyxHQUFHLGNBQWMsQ0FBQyxNQUFNLEVBQzNFLENBQUMscUJBQWtDLEVBQUU7UUFDeEMsSUFBTSxLQUFLLEdBQUcsV0FBVyxDQUFDLGNBQWMsRUFBRSxDQUFDLENBQUMsQ0FBQztRQUM3QyxJQUFJLEtBQUssRUFBRTtZQUNULElBQU0sSUFBSSxHQUFHLFVBQVUsQ0FBQyxjQUFjLEVBQUUsQ0FBQyxDQUFDLENBQUM7WUFDM0MsZUFBZSxDQUFDLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLEtBQUssQ0FBQyxDQUFDO1NBQ3hFO0tBQ0Y7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4qIEBsaWNlbnNlXG4qIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuKlxuKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4qL1xuaW1wb3J0IHtTdHlsZVNhbml0aXplRm4sIFN0eWxlU2FuaXRpemVNb2RlfSBmcm9tICcuLi8uLi9zYW5pdGl6YXRpb24vc3R5bGVfc2FuaXRpemVyJztcbmltcG9ydCB7UHJvY2VkdXJhbFJlbmRlcmVyMywgUkVsZW1lbnQsIFJlbmRlcmVyMywgUmVuZGVyZXJTdHlsZUZsYWdzMywgaXNQcm9jZWR1cmFsUmVuZGVyZXJ9IGZyb20gJy4uL2ludGVyZmFjZXMvcmVuZGVyZXInO1xuXG5pbXBvcnQge0FwcGx5U3R5bGluZ0ZuLCBMU3R5bGluZ0RhdGEsIFN0eWxpbmdNYXBBcnJheSwgU3R5bGluZ01hcEFycmF5SW5kZXgsIFN0eWxpbmdNYXBzU3luY01vZGUsIFN5bmNTdHlsaW5nTWFwc0ZuLCBUU3R5bGluZ0NvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4LCBUU3R5bGluZ0NvbnRleHRQcm9wQ29uZmlnRmxhZ3N9IGZyb20gJy4vaW50ZXJmYWNlcyc7XG5pbXBvcnQge0JJVF9NQVNLX1NUQVJUX1ZBTFVFLCBkZWxldGVTdHlsaW5nU3RhdGVGcm9tU3RvcmFnZSwgZ2V0U3R5bGluZ1N0YXRlLCByZXNldFN0eWxpbmdTdGF0ZSwgc3RvcmVTdHlsaW5nU3RhdGV9IGZyb20gJy4vc3RhdGUnO1xuaW1wb3J0IHthbGxvd1N0eWxpbmdGbHVzaCwgZ2V0QmluZGluZ1ZhbHVlLCBnZXRHdWFyZE1hc2ssIGdldE1hcFByb3AsIGdldE1hcFZhbHVlLCBnZXRQcm9wLCBnZXRQcm9wVmFsdWVzU3RhcnRQb3NpdGlvbiwgZ2V0U3R5bGluZ01hcEFycmF5LCBnZXRWYWx1ZXNDb3VudCwgaGFzVmFsdWVDaGFuZ2VkLCBpc0NvbnRleHRMb2NrZWQsIGlzU2FuaXRpemF0aW9uUmVxdWlyZWQsIGlzU3R5bGluZ1ZhbHVlRGVmaW5lZCwgbG9ja0NvbnRleHQsIHNldEd1YXJkTWFzaywgc3RhdGVJc1BlcnNpc3RlZH0gZnJvbSAnLi91dGlsJztcblxuXG5cbi8qKlxuICogLS0tLS0tLS1cbiAqXG4gKiBUaGlzIGZpbGUgY29udGFpbnMgdGhlIGNvcmUgbG9naWMgZm9yIHN0eWxpbmcgaW4gQW5ndWxhci5cbiAqXG4gKiBBbGwgc3R5bGluZyBiaW5kaW5ncyAoaS5lLiBgW3N0eWxlXWAsIGBbc3R5bGUucHJvcF1gLCBgW2NsYXNzXWAgYW5kIGBbY2xhc3MubmFtZV1gKVxuICogd2lsbCBoYXZlIHRoZWlyIHZhbHVlcyBiZSBhcHBsaWVkIHRocm91Z2ggdGhlIGxvZ2ljIGluIHRoaXMgZmlsZS5cbiAqXG4gKiBXaGVuIGEgYmluZGluZyBpcyBlbmNvdW50ZXJlZCAoZS5nLiBgPGRpdiBbc3R5bGUud2lkdGhdPVwid1wiPmApIHRoZW5cbiAqIHRoZSBiaW5kaW5nIGRhdGEgd2lsbCBiZSBwb3B1bGF0ZWQgaW50byBhIGBUU3R5bGluZ0NvbnRleHRgIGRhdGEtc3RydWN0dXJlLlxuICogVGhlcmUgaXMgb25seSBvbmUgYFRTdHlsaW5nQ29udGV4dGAgcGVyIGBUTm9kZWAgYW5kIGVhY2ggZWxlbWVudCBpbnN0YW5jZVxuICogd2lsbCB1cGRhdGUgaXRzIHN0eWxlL2NsYXNzIGJpbmRpbmcgdmFsdWVzIGluIGNvbmNlcnQgd2l0aCB0aGUgc3R5bGluZ1xuICogY29udGV4dC5cbiAqXG4gKiBUbyBsZWFybiBtb3JlIGFib3V0IHRoZSBhbGdvcml0aG0gc2VlIGBUU3R5bGluZ0NvbnRleHRgLlxuICpcbiAqIC0tLS0tLS0tXG4gKi9cblxuLy8gVGhlIGZpcnN0IGJpdCB2YWx1ZSByZWZsZWN0cyBhIG1hcC1iYXNlZCBiaW5kaW5nIHZhbHVlJ3MgYml0LlxuLy8gVGhlIHJlYXNvbiB3aHkgaXQncyBhbHdheXMgYWN0aXZhdGVkIGZvciBldmVyeSBlbnRyeSBpbiB0aGUgbWFwXG4vLyBpcyBzbyB0aGF0IGlmIGFueSBtYXAtYmluZGluZyB2YWx1ZXMgdXBkYXRlIHRoZW4gYWxsIG90aGVyIHByb3Bcbi8vIGJhc2VkIGJpbmRpbmdzIHdpbGwgcGFzcyB0aGUgZ3VhcmQgY2hlY2sgYXV0b21hdGljYWxseSB3aXRob3V0XG4vLyBhbnkgZXh0cmEgY29kZSBvciBmbGFncy5cbmV4cG9ydCBjb25zdCBERUZBVUxUX0dVQVJEX01BU0tfVkFMVUUgPSAwYjE7XG5cbi8qKlxuICogVGhlIGd1YXJkL3VwZGF0ZSBtYXNrIGJpdCBpbmRleCBsb2NhdGlvbiBmb3IgbWFwLWJhc2VkIGJpbmRpbmdzLlxuICpcbiAqIEFsbCBtYXAtYmFzZWQgYmluZGluZ3MgKGkuZS4gYFtzdHlsZV1gIGFuZCBgW2NsYXNzXWAgKVxuICovXG5jb25zdCBTVFlMSU5HX0lOREVYX0ZPUl9NQVBfQklORElORyA9IDA7XG5cbi8qKlxuICogRGVmYXVsdCBmYWxsYmFjayB2YWx1ZSBmb3IgYSBzdHlsaW5nIGJpbmRpbmcuXG4gKlxuICogQSB2YWx1ZSBvZiBgbnVsbGAgaXMgdXNlZCBoZXJlIHdoaWNoIHNpZ25hbHMgdG8gdGhlIHN0eWxpbmcgYWxnb3JpdGhtIHRoYXRcbiAqIHRoZSBzdHlsaW5nIHZhbHVlIGlzIG5vdCBwcmVzZW50LiBUaGlzIHdheSBpZiB0aGVyZSBhcmUgbm8gb3RoZXIgdmFsdWVzXG4gKiBkZXRlY3RlZCB0aGVuIGl0IHdpbGwgYmUgcmVtb3ZlZCBvbmNlIHRoZSBzdHlsZS9jbGFzcyBwcm9wZXJ0eSBpcyBkaXJ0eSBhbmRcbiAqIGRpZmZlZCB3aXRoaW4gdGhlIHN0eWxpbmcgYWxnb3JpdGhtIHByZXNlbnQgaW4gYGZsdXNoU3R5bGluZ2AuXG4gKi9cbmNvbnN0IERFRkFVTFRfQklORElOR19WQUxVRSA9IG51bGw7XG5cbi8qKlxuICogRGVmYXVsdCBzaXplIGNvdW50IHZhbHVlIGZvciBhIG5ldyBlbnRyeSBpbiBhIGNvbnRleHQuXG4gKlxuICogQSB2YWx1ZSBvZiBgMWAgaXMgdXNlZCBoZXJlIGJlY2F1c2UgZWFjaCBlbnRyeSBpbiB0aGUgY29udGV4dCBoYXMgYSBkZWZhdWx0XG4gKiBwcm9wZXJ0eS5cbiAqL1xuY29uc3QgREVGQVVMVF9TSVpFX1ZBTFVFID0gMTtcblxubGV0IGRlZmVycmVkQmluZGluZ1F1ZXVlOiAoVFN0eWxpbmdDb250ZXh0IHwgbnVtYmVyIHwgc3RyaW5nIHwgbnVsbCB8IGJvb2xlYW4pW10gPSBbXTtcblxuLyoqXG4gKiBWaXNpdHMgYSBjbGFzcy1iYXNlZCBiaW5kaW5nIGFuZCB1cGRhdGVzIHRoZSBuZXcgdmFsdWUgKGlmIGNoYW5nZWQpLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgY2FsbGVkIGVhY2ggdGltZSBhIGNsYXNzLWJhc2VkIHN0eWxpbmcgaW5zdHJ1Y3Rpb25cbiAqIGlzIGV4ZWN1dGVkLiBJdCdzIGltcG9ydGFudCB0aGF0IGl0J3MgYWx3YXlzIGNhbGxlZCAoZXZlbiBpZiB0aGUgdmFsdWVcbiAqIGhhcyBub3QgY2hhbmdlZCkgc28gdGhhdCB0aGUgaW5uZXIgY291bnRlciBpbmRleCB2YWx1ZSBpcyBpbmNyZW1lbnRlZC5cbiAqIFRoaXMgd2F5LCBlYWNoIGluc3RydWN0aW9uIGlzIGFsd2F5cyBndWFyYW50ZWVkIHRvIGdldCB0aGUgc2FtZSBjb3VudGVyXG4gKiBzdGF0ZSBlYWNoIHRpbWUgaXQncyBjYWxsZWQgKHdoaWNoIHRoZW4gYWxsb3dzIHRoZSBgVFN0eWxpbmdDb250ZXh0YFxuICogYW5kIHRoZSBiaXQgbWFzayB2YWx1ZXMgdG8gYmUgaW4gc3luYykuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiB1cGRhdGVDbGFzc0JpbmRpbmcoXG4gICAgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBkYXRhOiBMU3R5bGluZ0RhdGEsIGVsZW1lbnQ6IFJFbGVtZW50LCBwcm9wOiBzdHJpbmcgfCBudWxsLFxuICAgIGJpbmRpbmdJbmRleDogbnVtYmVyLCB2YWx1ZTogYm9vbGVhbiB8IHN0cmluZyB8IG51bGwgfCB1bmRlZmluZWQgfCBTdHlsaW5nTWFwQXJyYXksXG4gICAgZGVmZXJSZWdpc3RyYXRpb246IGJvb2xlYW4sIGZvcmNlVXBkYXRlOiBib29sZWFuKTogYm9vbGVhbiB7XG4gIGNvbnN0IGlzTWFwQmFzZWQgPSAhcHJvcDtcbiAgY29uc3Qgc3RhdGUgPSBnZXRTdHlsaW5nU3RhdGUoZWxlbWVudCwgc3RhdGVJc1BlcnNpc3RlZChjb250ZXh0KSk7XG4gIGNvbnN0IGluZGV4ID0gaXNNYXBCYXNlZCA/IFNUWUxJTkdfSU5ERVhfRk9SX01BUF9CSU5ESU5HIDogc3RhdGUuY2xhc3Nlc0luZGV4Kys7XG4gIGNvbnN0IHVwZGF0ZWQgPSB1cGRhdGVCaW5kaW5nRGF0YShcbiAgICAgIGNvbnRleHQsIGRhdGEsIGluZGV4LCBwcm9wLCBiaW5kaW5nSW5kZXgsIHZhbHVlLCBkZWZlclJlZ2lzdHJhdGlvbiwgZm9yY2VVcGRhdGUsIGZhbHNlKTtcbiAgaWYgKHVwZGF0ZWQgfHwgZm9yY2VVcGRhdGUpIHtcbiAgICAvLyBXZSBmbGlwIHRoZSBiaXQgaW4gdGhlIGJpdE1hc2sgdG8gcmVmbGVjdCB0aGF0IHRoZSBiaW5kaW5nXG4gICAgLy8gYXQgdGhlIGBpbmRleGAgc2xvdCBoYXMgY2hhbmdlZC4gVGhpcyBpZGVudGlmaWVzIHRvIHRoZSBmbHVzaGluZ1xuICAgIC8vIHBoYXNlIHRoYXQgdGhlIGJpbmRpbmdzIGZvciB0aGlzIHBhcnRpY3VsYXIgQ1NTIGNsYXNzIG5lZWQgdG8gYmVcbiAgICAvLyBhcHBsaWVkIGFnYWluIGJlY2F1c2Ugb24gb3IgbW9yZSBvZiB0aGUgYmluZGluZ3MgZm9yIHRoZSBDU1NcbiAgICAvLyBjbGFzcyBoYXZlIGNoYW5nZWQuXG4gICAgc3RhdGUuY2xhc3Nlc0JpdE1hc2sgfD0gMSA8PCBpbmRleDtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbi8qKlxuICogVmlzaXRzIGEgc3R5bGUtYmFzZWQgYmluZGluZyBhbmQgdXBkYXRlcyB0aGUgbmV3IHZhbHVlIChpZiBjaGFuZ2VkKS5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIGNhbGxlZCBlYWNoIHRpbWUgYSBzdHlsZS1iYXNlZCBzdHlsaW5nIGluc3RydWN0aW9uXG4gKiBpcyBleGVjdXRlZC4gSXQncyBpbXBvcnRhbnQgdGhhdCBpdCdzIGFsd2F5cyBjYWxsZWQgKGV2ZW4gaWYgdGhlIHZhbHVlXG4gKiBoYXMgbm90IGNoYW5nZWQpIHNvIHRoYXQgdGhlIGlubmVyIGNvdW50ZXIgaW5kZXggdmFsdWUgaXMgaW5jcmVtZW50ZWQuXG4gKiBUaGlzIHdheSwgZWFjaCBpbnN0cnVjdGlvbiBpcyBhbHdheXMgZ3VhcmFudGVlZCB0byBnZXQgdGhlIHNhbWUgY291bnRlclxuICogc3RhdGUgZWFjaCB0aW1lIGl0J3MgY2FsbGVkICh3aGljaCB0aGVuIGFsbG93cyB0aGUgYFRTdHlsaW5nQ29udGV4dGBcbiAqIGFuZCB0aGUgYml0IG1hc2sgdmFsdWVzIHRvIGJlIGluIHN5bmMpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gdXBkYXRlU3R5bGVCaW5kaW5nKFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgZGF0YTogTFN0eWxpbmdEYXRhLCBlbGVtZW50OiBSRWxlbWVudCwgcHJvcDogc3RyaW5nIHwgbnVsbCxcbiAgICBiaW5kaW5nSW5kZXg6IG51bWJlciwgdmFsdWU6IFN0cmluZyB8IHN0cmluZyB8IG51bWJlciB8IG51bGwgfCB1bmRlZmluZWQgfCBTdHlsaW5nTWFwQXJyYXksXG4gICAgc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm4gfCBudWxsLCBkZWZlclJlZ2lzdHJhdGlvbjogYm9vbGVhbiwgZm9yY2VVcGRhdGU6IGJvb2xlYW4pOiBib29sZWFuIHtcbiAgY29uc3QgaXNNYXBCYXNlZCA9ICFwcm9wO1xuICBjb25zdCBzdGF0ZSA9IGdldFN0eWxpbmdTdGF0ZShlbGVtZW50LCBzdGF0ZUlzUGVyc2lzdGVkKGNvbnRleHQpKTtcbiAgY29uc3QgaW5kZXggPSBpc01hcEJhc2VkID8gU1RZTElOR19JTkRFWF9GT1JfTUFQX0JJTkRJTkcgOiBzdGF0ZS5zdHlsZXNJbmRleCsrO1xuICBjb25zdCBzYW5pdGl6YXRpb25SZXF1aXJlZCA9IGlzTWFwQmFzZWQgP1xuICAgICAgdHJ1ZSA6XG4gICAgICAoc2FuaXRpemVyID8gc2FuaXRpemVyKHByb3AgISwgbnVsbCwgU3R5bGVTYW5pdGl6ZU1vZGUuVmFsaWRhdGVQcm9wZXJ0eSkgOiBmYWxzZSk7XG4gIGNvbnN0IHVwZGF0ZWQgPSB1cGRhdGVCaW5kaW5nRGF0YShcbiAgICAgIGNvbnRleHQsIGRhdGEsIGluZGV4LCBwcm9wLCBiaW5kaW5nSW5kZXgsIHZhbHVlLCBkZWZlclJlZ2lzdHJhdGlvbiwgZm9yY2VVcGRhdGUsXG4gICAgICBzYW5pdGl6YXRpb25SZXF1aXJlZCk7XG4gIGlmICh1cGRhdGVkIHx8IGZvcmNlVXBkYXRlKSB7XG4gICAgLy8gV2UgZmxpcCB0aGUgYml0IGluIHRoZSBiaXRNYXNrIHRvIHJlZmxlY3QgdGhhdCB0aGUgYmluZGluZ1xuICAgIC8vIGF0IHRoZSBgaW5kZXhgIHNsb3QgaGFzIGNoYW5nZWQuIFRoaXMgaWRlbnRpZmllcyB0byB0aGUgZmx1c2hpbmdcbiAgICAvLyBwaGFzZSB0aGF0IHRoZSBiaW5kaW5ncyBmb3IgdGhpcyBwYXJ0aWN1bGFyIHByb3BlcnR5IG5lZWQgdG8gYmVcbiAgICAvLyBhcHBsaWVkIGFnYWluIGJlY2F1c2Ugb24gb3IgbW9yZSBvZiB0aGUgYmluZGluZ3MgZm9yIHRoZSBDU1NcbiAgICAvLyBwcm9wZXJ0eSBoYXZlIGNoYW5nZWQuXG4gICAgc3RhdGUuc3R5bGVzQml0TWFzayB8PSAxIDw8IGluZGV4O1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuLyoqXG4gKiBDYWxsZWQgZWFjaCB0aW1lIGEgYmluZGluZyB2YWx1ZSBoYXMgY2hhbmdlZCB3aXRoaW4gdGhlIHByb3ZpZGVkIGBUU3R5bGluZ0NvbnRleHRgLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgZGVzaWduZWQgdG8gYmUgY2FsbGVkIGZyb20gYHVwZGF0ZVN0eWxlQmluZGluZ2AgYW5kIGB1cGRhdGVDbGFzc0JpbmRpbmdgLlxuICogSWYgY2FsbGVkIGR1cmluZyB0aGUgZmlyc3QgdXBkYXRlIHBhc3MsIHRoZSBiaW5kaW5nIHdpbGwgYmUgcmVnaXN0ZXJlZCBpbiB0aGUgY29udGV4dC5cbiAqIElmIHRoZSBiaW5kaW5nIGRvZXMgZ2V0IHJlZ2lzdGVyZWQgYW5kIHRoZSBgZGVmZXJSZWdpc3RyYXRpb25gIGZsYWcgaXMgdHJ1ZSB0aGVuIHRoZVxuICogYmluZGluZyBkYXRhIHdpbGwgYmUgcXVldWVkIHVwIHVudGlsIHRoZSBjb250ZXh0IGlzIGxhdGVyIGZsdXNoZWQgaW4gYGFwcGx5U3R5bGluZ2AuXG4gKlxuICogVGhpcyBmdW5jdGlvbiB3aWxsIGFsc28gdXBkYXRlIGJpbmRpbmcgc2xvdCBpbiB0aGUgcHJvdmlkZWQgYExTdHlsaW5nRGF0YWAgd2l0aCB0aGVcbiAqIG5ldyBiaW5kaW5nIGVudHJ5IChpZiBpdCBoYXMgY2hhbmdlZCkuXG4gKlxuICogQHJldHVybnMgd2hldGhlciBvciBub3QgdGhlIGJpbmRpbmcgdmFsdWUgd2FzIHVwZGF0ZWQgaW4gdGhlIGBMU3R5bGluZ0RhdGFgLlxuICovXG5mdW5jdGlvbiB1cGRhdGVCaW5kaW5nRGF0YShcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGRhdGE6IExTdHlsaW5nRGF0YSwgY291bnRlckluZGV4OiBudW1iZXIsIHByb3A6IHN0cmluZyB8IG51bGwsXG4gICAgYmluZGluZ0luZGV4OiBudW1iZXIsXG4gICAgdmFsdWU6IHN0cmluZyB8IFN0cmluZyB8IG51bWJlciB8IGJvb2xlYW4gfCBudWxsIHwgdW5kZWZpbmVkIHwgU3R5bGluZ01hcEFycmF5LFxuICAgIGRlZmVyUmVnaXN0cmF0aW9uOiBib29sZWFuLCBmb3JjZVVwZGF0ZTogYm9vbGVhbiwgc2FuaXRpemF0aW9uUmVxdWlyZWQ6IGJvb2xlYW4pOiBib29sZWFuIHtcbiAgaWYgKCFpc0NvbnRleHRMb2NrZWQoY29udGV4dCkpIHtcbiAgICBpZiAoZGVmZXJSZWdpc3RyYXRpb24pIHtcbiAgICAgIGRlZmVyQmluZGluZ1JlZ2lzdHJhdGlvbihjb250ZXh0LCBjb3VudGVySW5kZXgsIHByb3AsIGJpbmRpbmdJbmRleCwgc2FuaXRpemF0aW9uUmVxdWlyZWQpO1xuICAgIH0gZWxzZSB7XG4gICAgICBkZWZlcnJlZEJpbmRpbmdRdWV1ZS5sZW5ndGggJiYgZmx1c2hEZWZlcnJlZEJpbmRpbmdzKCk7XG5cbiAgICAgIC8vIHRoaXMgd2lsbCBvbmx5IGhhcHBlbiBkdXJpbmcgdGhlIGZpcnN0IHVwZGF0ZSBwYXNzIG9mIHRoZVxuICAgICAgLy8gY29udGV4dC4gVGhlIHJlYXNvbiB3aHkgd2UgY2FuJ3QgdXNlIGB0Tm9kZS5maXJzdFRlbXBsYXRlUGFzc2BcbiAgICAgIC8vIGhlcmUgaXMgYmVjYXVzZSBpdHMgbm90IGd1YXJhbnRlZWQgdG8gYmUgdHJ1ZSB3aGVuIHRoZSBmaXJzdFxuICAgICAgLy8gdXBkYXRlIHBhc3MgaXMgZXhlY3V0ZWQgKHJlbWVtYmVyIHRoYXQgYWxsIHN0eWxpbmcgaW5zdHJ1Y3Rpb25zXG4gICAgICAvLyBhcmUgcnVuIGluIHRoZSB1cGRhdGUgcGhhc2UsIGFuZCwgYXMgYSByZXN1bHQsIGFyZSBubyBtb3JlXG4gICAgICAvLyBzdHlsaW5nIGluc3RydWN0aW9ucyB0aGF0IGFyZSBydW4gaW4gdGhlIGNyZWF0aW9uIHBoYXNlKS5cbiAgICAgIHJlZ2lzdGVyQmluZGluZyhjb250ZXh0LCBjb3VudGVySW5kZXgsIHByb3AsIGJpbmRpbmdJbmRleCwgc2FuaXRpemF0aW9uUmVxdWlyZWQpO1xuICAgIH1cbiAgfVxuXG4gIGNvbnN0IGNoYW5nZWQgPSBmb3JjZVVwZGF0ZSB8fCBoYXNWYWx1ZUNoYW5nZWQoZGF0YVtiaW5kaW5nSW5kZXhdLCB2YWx1ZSk7XG4gIGlmIChjaGFuZ2VkKSB7XG4gICAgZGF0YVtiaW5kaW5nSW5kZXhdID0gdmFsdWU7XG4gIH1cbiAgcmV0dXJuIGNoYW5nZWQ7XG59XG5cbi8qKlxuICogU2NoZWR1bGVzIGEgYmluZGluZyByZWdpc3RyYXRpb24gdG8gYmUgcnVuIGF0IGEgbGF0ZXIgcG9pbnQuXG4gKlxuICogVGhlIHJlYXNvbmluZyBmb3IgdGhpcyBmZWF0dXJlIGlzIHRvIGVuc3VyZSB0aGF0IHN0eWxpbmdcbiAqIGJpbmRpbmdzIGFyZSByZWdpc3RlcmVkIGluIHRoZSBjb3JyZWN0IG9yZGVyIGZvciB3aGVuXG4gKiBkaXJlY3RpdmVzL2NvbXBvbmVudHMgaGF2ZSBhIHN1cGVyL3N1YiBjbGFzcyBpbmhlcml0YW5jZVxuICogY2hhaW5zLiBFYWNoIGRpcmVjdGl2ZSdzIHN0eWxpbmcgYmluZGluZ3MgbXVzdCBiZVxuICogcmVnaXN0ZXJlZCBpbnRvIHRoZSBjb250ZXh0IGluIHJldmVyc2Ugb3JkZXIuIFRoZXJlZm9yZSBhbGxcbiAqIGJpbmRpbmdzIHdpbGwgYmUgYnVmZmVyZWQgaW4gcmV2ZXJzZSBvcmRlciBhbmQgdGhlbiBhcHBsaWVkXG4gKiBhZnRlciB0aGUgaW5oZXJpdGFuY2UgY2hhaW4gZXhpdHMuXG4gKi9cbmZ1bmN0aW9uIGRlZmVyQmluZGluZ1JlZ2lzdHJhdGlvbihcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGNvdW50ZXJJbmRleDogbnVtYmVyLCBwcm9wOiBzdHJpbmcgfCBudWxsLCBiaW5kaW5nSW5kZXg6IG51bWJlcixcbiAgICBzYW5pdGl6YXRpb25SZXF1aXJlZDogYm9vbGVhbikge1xuICBkZWZlcnJlZEJpbmRpbmdRdWV1ZS51bnNoaWZ0KGNvbnRleHQsIGNvdW50ZXJJbmRleCwgcHJvcCwgYmluZGluZ0luZGV4LCBzYW5pdGl6YXRpb25SZXF1aXJlZCk7XG59XG5cbi8qKlxuICogRmx1c2hlcyB0aGUgY29sbGVjdGlvbiBvZiBkZWZlcnJlZCBiaW5kaW5ncyBhbmQgY2F1c2VzIGVhY2ggZW50cnlcbiAqIHRvIGJlIHJlZ2lzdGVyZWQgaW50byB0aGUgY29udGV4dC5cbiAqL1xuZnVuY3Rpb24gZmx1c2hEZWZlcnJlZEJpbmRpbmdzKCkge1xuICBsZXQgaSA9IDA7XG4gIHdoaWxlIChpIDwgZGVmZXJyZWRCaW5kaW5nUXVldWUubGVuZ3RoKSB7XG4gICAgY29uc3QgY29udGV4dCA9IGRlZmVycmVkQmluZGluZ1F1ZXVlW2krK10gYXMgVFN0eWxpbmdDb250ZXh0O1xuICAgIGNvbnN0IGNvdW50ID0gZGVmZXJyZWRCaW5kaW5nUXVldWVbaSsrXSBhcyBudW1iZXI7XG4gICAgY29uc3QgcHJvcCA9IGRlZmVycmVkQmluZGluZ1F1ZXVlW2krK10gYXMgc3RyaW5nO1xuICAgIGNvbnN0IGJpbmRpbmdJbmRleCA9IGRlZmVycmVkQmluZGluZ1F1ZXVlW2krK10gYXMgbnVtYmVyIHwgbnVsbDtcbiAgICBjb25zdCBzYW5pdGl6YXRpb25SZXF1aXJlZCA9IGRlZmVycmVkQmluZGluZ1F1ZXVlW2krK10gYXMgYm9vbGVhbjtcbiAgICByZWdpc3RlckJpbmRpbmcoY29udGV4dCwgY291bnQsIHByb3AsIGJpbmRpbmdJbmRleCwgc2FuaXRpemF0aW9uUmVxdWlyZWQpO1xuICB9XG4gIGRlZmVycmVkQmluZGluZ1F1ZXVlLmxlbmd0aCA9IDA7XG59XG5cbi8qKlxuICogUmVnaXN0ZXJzIHRoZSBwcm92aWRlZCBiaW5kaW5nIChwcm9wICsgYmluZGluZ0luZGV4KSBpbnRvIHRoZSBjb250ZXh0LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgc2hhcmVkIGJldHdlZW4gYmluZGluZ3MgdGhhdCBhcmUgYXNzaWduZWQgaW1tZWRpYXRlbHlcbiAqICh2aWEgYHVwZGF0ZUJpbmRpbmdEYXRhYCkgYW5kIGF0IGEgZGVmZXJyZWQgc3RhZ2UuIFdoZW4gY2FsbGVkLCBpdCB3aWxsXG4gKiBmaWd1cmUgb3V0IGV4YWN0bHkgd2hlcmUgdG8gcGxhY2UgdGhlIGJpbmRpbmcgZGF0YSBpbiB0aGUgY29udGV4dC5cbiAqXG4gKiBJdCBpcyBuZWVkZWQgYmVjYXVzZSBpdCB3aWxsIGVpdGhlciB1cGRhdGUgb3IgaW5zZXJ0IGEgc3R5bGluZyBwcm9wZXJ0eVxuICogaW50byB0aGUgY29udGV4dCBhdCB0aGUgY29ycmVjdCBzcG90LlxuICpcbiAqIFdoZW4gY2FsbGVkLCBvbmUgb2YgdHdvIHRoaW5ncyB3aWxsIGhhcHBlbjpcbiAqXG4gKiAxKSBJZiB0aGUgcHJvcGVydHkgYWxyZWFkeSBleGlzdHMgaW4gdGhlIGNvbnRleHQgdGhlbiBpdCB3aWxsIGp1c3QgYWRkXG4gKiAgICB0aGUgcHJvdmlkZWQgYGJpbmRpbmdWYWx1ZWAgdG8gdGhlIGVuZCBvZiB0aGUgYmluZGluZyBzb3VyY2VzIHJlZ2lvblxuICogICAgZm9yIHRoYXQgcGFydGljdWxhciBwcm9wZXJ0eS5cbiAqXG4gKiAgICAtIElmIHRoZSBiaW5kaW5nIHZhbHVlIGlzIGEgbnVtYmVyIHRoZW4gaXQgd2lsbCBiZSBhZGRlZCBhcyBhIG5ld1xuICogICAgICBiaW5kaW5nIGluZGV4IHNvdXJjZSBuZXh0IHRvIHRoZSBvdGhlciBiaW5kaW5nIHNvdXJjZXMgZm9yIHRoZSBwcm9wZXJ0eS5cbiAqXG4gKiAgICAtIE90aGVyd2lzZSwgaWYgdGhlIGJpbmRpbmcgdmFsdWUgaXMgYSBzdHJpbmcvYm9vbGVhbi9udWxsIHR5cGUgdGhlbiBpdCB3aWxsXG4gKiAgICAgIHJlcGxhY2UgdGhlIGRlZmF1bHQgdmFsdWUgZm9yIHRoZSBwcm9wZXJ0eSBpZiB0aGUgZGVmYXVsdCB2YWx1ZSBpcyBgbnVsbGAuXG4gKlxuICogMikgSWYgdGhlIHByb3BlcnR5IGRvZXMgbm90IGV4aXN0IHRoZW4gaXQgd2lsbCBiZSBpbnNlcnRlZCBpbnRvIHRoZSBjb250ZXh0LlxuICogICAgVGhlIHN0eWxpbmcgY29udGV4dCByZWxpZXMgb24gYWxsIHByb3BlcnRpZXMgYmVpbmcgc3RvcmVkIGluIGFscGhhYmV0aWNhbFxuICogICAgb3JkZXIsIHNvIGl0IGtub3dzIGV4YWN0bHkgd2hlcmUgdG8gc3RvcmUgaXQuXG4gKlxuICogICAgV2hlbiBpbnNlcnRlZCwgYSBkZWZhdWx0IGBudWxsYCB2YWx1ZSBpcyBjcmVhdGVkIGZvciB0aGUgcHJvcGVydHkgd2hpY2ggZXhpc3RzXG4gKiAgICBhcyB0aGUgZGVmYXVsdCB2YWx1ZSBmb3IgdGhlIGJpbmRpbmcuIElmIHRoZSBiaW5kaW5nVmFsdWUgcHJvcGVydHkgaXMgaW5zZXJ0ZWRcbiAqICAgIGFuZCBpdCBpcyBlaXRoZXIgYSBzdHJpbmcsIG51bWJlciBvciBudWxsIHZhbHVlIHRoZW4gdGhhdCB3aWxsIHJlcGxhY2UgdGhlIGRlZmF1bHRcbiAqICAgIHZhbHVlLlxuICpcbiAqIE5vdGUgdGhhdCB0aGlzIGZ1bmN0aW9uIGlzIGFsc28gdXNlZCBmb3IgbWFwLWJhc2VkIHN0eWxpbmcgYmluZGluZ3MuIFRoZXkgYXJlIHRyZWF0ZWRcbiAqIG11Y2ggdGhlIHNhbWUgYXMgcHJvcC1iYXNlZCBiaW5kaW5ncywgYnV0LCBiZWNhdXNlIHRoZXkgZG8gbm90IGhhdmUgYSBwcm9wZXJ0eSB2YWx1ZVxuICogKHNpbmNlIGl0J3MgYSBtYXApLCBhbGwgbWFwLWJhc2VkIGVudHJpZXMgYXJlIHN0b3JlZCBpbiBhbiBhbHJlYWR5IHBvcHVsYXRlZCBhcmVhIG9mXG4gKiB0aGUgY29udGV4dCBhdCB0aGUgdG9wICh3aGljaCBpcyByZXNlcnZlZCBmb3IgbWFwLWJhc2VkIGVudHJpZXMpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVnaXN0ZXJCaW5kaW5nKFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgY291bnRJZDogbnVtYmVyLCBwcm9wOiBzdHJpbmcgfCBudWxsLFxuICAgIGJpbmRpbmdWYWx1ZTogbnVtYmVyIHwgbnVsbCB8IHN0cmluZyB8IGJvb2xlYW4sIHNhbml0aXphdGlvblJlcXVpcmVkPzogYm9vbGVhbik6IGJvb2xlYW4ge1xuICBsZXQgcmVnaXN0ZXJlZCA9IGZhbHNlO1xuICBpZiAocHJvcCkge1xuICAgIC8vIHByb3AtYmFzZWQgYmluZGluZ3MgKGUuZyBgPGRpdiBbc3R5bGUud2lkdGhdPVwid1wiIFtjbGFzcy5mb29dPVwiZlwiPmApXG4gICAgbGV0IGZvdW5kID0gZmFsc2U7XG4gICAgbGV0IGkgPSBnZXRQcm9wVmFsdWVzU3RhcnRQb3NpdGlvbihjb250ZXh0KTtcbiAgICB3aGlsZSAoaSA8IGNvbnRleHQubGVuZ3RoKSB7XG4gICAgICBjb25zdCB2YWx1ZXNDb3VudCA9IGdldFZhbHVlc0NvdW50KGNvbnRleHQsIGkpO1xuICAgICAgY29uc3QgcCA9IGdldFByb3AoY29udGV4dCwgaSk7XG4gICAgICBmb3VuZCA9IHByb3AgPD0gcDtcbiAgICAgIGlmIChmb3VuZCkge1xuICAgICAgICAvLyBhbGwgc3R5bGUvY2xhc3MgYmluZGluZ3MgYXJlIHNvcnRlZCBieSBwcm9wZXJ0eSBuYW1lXG4gICAgICAgIGlmIChwcm9wIDwgcCkge1xuICAgICAgICAgIGFsbG9jYXRlTmV3Q29udGV4dEVudHJ5KGNvbnRleHQsIGksIHByb3AsIHNhbml0aXphdGlvblJlcXVpcmVkKTtcbiAgICAgICAgfVxuICAgICAgICBhZGRCaW5kaW5nSW50b0NvbnRleHQoY29udGV4dCwgZmFsc2UsIGksIGJpbmRpbmdWYWx1ZSwgY291bnRJZCk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgICAgaSArPSBUU3R5bGluZ0NvbnRleHRJbmRleC5CaW5kaW5nc1N0YXJ0T2Zmc2V0ICsgdmFsdWVzQ291bnQ7XG4gICAgfVxuXG4gICAgaWYgKCFmb3VuZCkge1xuICAgICAgYWxsb2NhdGVOZXdDb250ZXh0RW50cnkoY29udGV4dCwgY29udGV4dC5sZW5ndGgsIHByb3AsIHNhbml0aXphdGlvblJlcXVpcmVkKTtcbiAgICAgIGFkZEJpbmRpbmdJbnRvQ29udGV4dChjb250ZXh0LCBmYWxzZSwgaSwgYmluZGluZ1ZhbHVlLCBjb3VudElkKTtcbiAgICAgIHJlZ2lzdGVyZWQgPSB0cnVlO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICAvLyBtYXAtYmFzZWQgYmluZGluZ3MgKGUuZyBgPGRpdiBbc3R5bGVdPVwic1wiIFtjbGFzc109XCJ7Y2xhc3NOYW1lOnRydWV9XCI+YClcbiAgICAvLyB0aGVyZSBpcyBubyBuZWVkIHRvIGFsbG9jYXRlIHRoZSBtYXAtYmFzZWQgYmluZGluZyByZWdpb24gaW50byB0aGUgY29udGV4dFxuICAgIC8vIHNpbmNlIGl0IGlzIGFscmVhZHkgdGhlcmUgd2hlbiB0aGUgY29udGV4dCBpcyBmaXJzdCBjcmVhdGVkLlxuICAgIGFkZEJpbmRpbmdJbnRvQ29udGV4dChcbiAgICAgICAgY29udGV4dCwgdHJ1ZSwgVFN0eWxpbmdDb250ZXh0SW5kZXguTWFwQmluZGluZ3NQb3NpdGlvbiwgYmluZGluZ1ZhbHVlLCBjb3VudElkKTtcbiAgICByZWdpc3RlcmVkID0gdHJ1ZTtcbiAgfVxuICByZXR1cm4gcmVnaXN0ZXJlZDtcbn1cblxuZnVuY3Rpb24gYWxsb2NhdGVOZXdDb250ZXh0RW50cnkoXG4gICAgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBpbmRleDogbnVtYmVyLCBwcm9wOiBzdHJpbmcsIHNhbml0aXphdGlvblJlcXVpcmVkPzogYm9vbGVhbikge1xuICAvLyAxLDI6IHNwbGljZSBpbmRleCBsb2NhdGlvbnNcbiAgLy8gMzogZWFjaCBlbnRyeSBnZXRzIGEgY29uZmlnIHZhbHVlIChndWFyZCBtYXNrICsgZmxhZ3MpXG4gIC8vIDQuIGVhY2ggZW50cnkgZ2V0cyBhIHNpemUgdmFsdWUgKHdoaWNoIGlzIGFsd2F5cyBvbmUgYmVjYXVzZSB0aGVyZSBpcyBhbHdheXMgYSBkZWZhdWx0IGJpbmRpbmdcbiAgLy8gdmFsdWUpXG4gIC8vIDUuIHRoZSBwcm9wZXJ0eSB0aGF0IGlzIGdldHRpbmcgYWxsb2NhdGVkIGludG8gdGhlIGNvbnRleHRcbiAgLy8gNi4gdGhlIGRlZmF1bHQgYmluZGluZyB2YWx1ZSAodXN1YWxseSBgbnVsbGApXG4gIGNvbnN0IGNvbmZpZyA9IHNhbml0aXphdGlvblJlcXVpcmVkID8gVFN0eWxpbmdDb250ZXh0UHJvcENvbmZpZ0ZsYWdzLlNhbml0aXphdGlvblJlcXVpcmVkIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBUU3R5bGluZ0NvbnRleHRQcm9wQ29uZmlnRmxhZ3MuRGVmYXVsdDtcbiAgY29udGV4dC5zcGxpY2UoaW5kZXgsIDAsIGNvbmZpZywgREVGQVVMVF9TSVpFX1ZBTFVFLCBwcm9wLCBERUZBVUxUX0JJTkRJTkdfVkFMVUUpO1xuICBzZXRHdWFyZE1hc2soY29udGV4dCwgaW5kZXgsIERFRkFVTFRfR1VBUkRfTUFTS19WQUxVRSk7XG59XG5cbi8qKlxuICogSW5zZXJ0cyBhIG5ldyBiaW5kaW5nIHZhbHVlIGludG8gYSBzdHlsaW5nIHByb3BlcnR5IHR1cGxlIGluIHRoZSBgVFN0eWxpbmdDb250ZXh0YC5cbiAqXG4gKiBBIGJpbmRpbmdWYWx1ZSBpcyBpbnNlcnRlZCBpbnRvIGEgY29udGV4dCBkdXJpbmcgdGhlIGZpcnN0IHVwZGF0ZSBwYXNzXG4gKiBvZiBhIHRlbXBsYXRlIG9yIGhvc3QgYmluZGluZ3MgZnVuY3Rpb24uIFdoZW4gdGhpcyBvY2N1cnMsIHR3byB0aGluZ3NcbiAqIGhhcHBlbjpcbiAqXG4gKiAtIElmIHRoZSBiaW5kaW5nVmFsdWUgdmFsdWUgaXMgYSBudW1iZXIgdGhlbiBpdCBpcyB0cmVhdGVkIGFzIGEgYmluZGluZ0luZGV4XG4gKiAgIHZhbHVlIChhIGluZGV4IGluIHRoZSBgTFZpZXdgKSBhbmQgaXQgd2lsbCBiZSBpbnNlcnRlZCBuZXh0IHRvIHRoZSBvdGhlclxuICogICBiaW5kaW5nIGluZGV4IGVudHJpZXMuXG4gKlxuICogLSBPdGhlcndpc2UgdGhlIGJpbmRpbmcgdmFsdWUgd2lsbCB1cGRhdGUgdGhlIGRlZmF1bHQgdmFsdWUgZm9yIHRoZSBwcm9wZXJ0eVxuICogICBhbmQgdGhpcyB3aWxsIG9ubHkgaGFwcGVuIGlmIHRoZSBkZWZhdWx0IHZhbHVlIGlzIGBudWxsYC5cbiAqXG4gKiBOb3RlIHRoYXQgdGhpcyBmdW5jdGlvbiBhbHNvIGhhbmRsZXMgbWFwLWJhc2VkIGJpbmRpbmdzIGFuZCB3aWxsIGluc2VydCB0aGVtXG4gKiBhdCB0aGUgdG9wIG9mIHRoZSBjb250ZXh0LlxuICovXG5mdW5jdGlvbiBhZGRCaW5kaW5nSW50b0NvbnRleHQoXG4gICAgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBpc01hcEJhc2VkOiBib29sZWFuLCBpbmRleDogbnVtYmVyLFxuICAgIGJpbmRpbmdWYWx1ZTogbnVtYmVyIHwgc3RyaW5nIHwgYm9vbGVhbiB8IG51bGwsIGNvdW50SWQ6IG51bWJlcikge1xuICBjb25zdCB2YWx1ZXNDb3VudCA9IGdldFZhbHVlc0NvdW50KGNvbnRleHQsIGluZGV4KTtcblxuICBjb25zdCBmaXJzdFZhbHVlSW5kZXggPSBpbmRleCArIFRTdHlsaW5nQ29udGV4dEluZGV4LkJpbmRpbmdzU3RhcnRPZmZzZXQ7XG4gIGxldCBsYXN0VmFsdWVJbmRleCA9IGZpcnN0VmFsdWVJbmRleCArIHZhbHVlc0NvdW50O1xuICBpZiAoIWlzTWFwQmFzZWQpIHtcbiAgICAvLyBwcm9wLWJhc2VkIHZhbHVlcyBhbGwgaGF2ZSBkZWZhdWx0IHZhbHVlcywgYnV0IG1hcC1iYXNlZCBlbnRyaWVzIGRvIG5vdC5cbiAgICAvLyB3ZSB3YW50IHRvIGFjY2VzcyB0aGUgaW5kZXggZm9yIHRoZSBkZWZhdWx0IHZhbHVlIGluIHRoaXMgY2FzZSBhbmQgbm90IGp1c3RcbiAgICAvLyB0aGUgYmluZGluZ3MuLi5cbiAgICBsYXN0VmFsdWVJbmRleC0tO1xuICB9XG5cbiAgaWYgKHR5cGVvZiBiaW5kaW5nVmFsdWUgPT09ICdudW1iZXInKSB7XG4gICAgLy8gdGhlIGxvb3AgaGVyZSB3aWxsIGNoZWNrIHRvIHNlZSBpZiB0aGUgYmluZGluZyBhbHJlYWR5IGV4aXN0c1xuICAgIC8vIGZvciB0aGUgcHJvcGVydHkgaW4gdGhlIGNvbnRleHQuIFdoeT8gVGhlIHJlYXNvbiBmb3IgdGhpcyBpc1xuICAgIC8vIGJlY2F1c2UgdGhlIHN0eWxpbmcgY29udGV4dCBpcyBub3QgXCJsb2NrZWRcIiB1bnRpbCB0aGUgZmlyc3RcbiAgICAvLyBmbHVzaCBoYXMgb2NjdXJyZWQuIFRoaXMgbWVhbnMgdGhhdCBpZiBhIHJlcGVhdGVkIGVsZW1lbnRcbiAgICAvLyByZWdpc3RlcnMgaXRzIHN0eWxpbmcgYmluZGluZ3MgdGhlbiBpdCB3aWxsIHJlZ2lzdGVyIGVhY2hcbiAgICAvLyBiaW5kaW5nIG1vcmUgdGhhbiBvbmNlIChzaW5jZSBpdHMgZHVwbGljYXRlZCkuIFRoaXMgY2hlY2tcbiAgICAvLyB3aWxsIHByZXZlbnQgdGhhdCBmcm9tIGhhcHBlbmluZy4gTm90ZSB0aGF0IHRoaXMgb25seSBoYXBwZW5zXG4gICAgLy8gd2hlbiBhIGJpbmRpbmcgaXMgZmlyc3QgZW5jb3VudGVyZWQgYW5kIG5vdCBlYWNoIHRpbWUgaXQgaXNcbiAgICAvLyB1cGRhdGVkLlxuICAgIGZvciAobGV0IGkgPSBmaXJzdFZhbHVlSW5kZXg7IGkgPD0gbGFzdFZhbHVlSW5kZXg7IGkrKykge1xuICAgICAgY29uc3QgaW5kZXhBdFBvc2l0aW9uID0gY29udGV4dFtpXTtcbiAgICAgIGlmIChpbmRleEF0UG9zaXRpb24gPT09IGJpbmRpbmdWYWx1ZSkgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnRleHQuc3BsaWNlKGxhc3RWYWx1ZUluZGV4LCAwLCBiaW5kaW5nVmFsdWUpO1xuICAgIChjb250ZXh0W2luZGV4ICsgVFN0eWxpbmdDb250ZXh0SW5kZXguVmFsdWVzQ291bnRPZmZzZXRdIGFzIG51bWJlcikrKztcblxuICAgIC8vIG5vdyB0aGF0IGEgbmV3IGJpbmRpbmcgaW5kZXggaGFzIGJlZW4gYWRkZWQgdG8gdGhlIHByb3BlcnR5XG4gICAgLy8gdGhlIGd1YXJkIG1hc2sgYml0IHZhbHVlIChhdCB0aGUgYGNvdW50SWRgIHBvc2l0aW9uKSBuZWVkc1xuICAgIC8vIHRvIGJlIGluY2x1ZGVkIGludG8gdGhlIGV4aXN0aW5nIG1hc2sgdmFsdWUuXG4gICAgY29uc3QgZ3VhcmRNYXNrID0gZ2V0R3VhcmRNYXNrKGNvbnRleHQsIGluZGV4KSB8ICgxIDw8IGNvdW50SWQpO1xuICAgIHNldEd1YXJkTWFzayhjb250ZXh0LCBpbmRleCwgZ3VhcmRNYXNrKTtcbiAgfSBlbHNlIGlmIChiaW5kaW5nVmFsdWUgIT09IG51bGwgJiYgY29udGV4dFtsYXN0VmFsdWVJbmRleF0gPT0gbnVsbCkge1xuICAgIGNvbnRleHRbbGFzdFZhbHVlSW5kZXhdID0gYmluZGluZ1ZhbHVlO1xuICB9XG59XG5cbi8qKlxuICogQXBwbGllcyBhbGwgcGVuZGluZyBzdHlsZSBhbmQgY2xhc3MgYmluZGluZ3MgdG8gdGhlIHByb3ZpZGVkIGVsZW1lbnQuXG4gKlxuICogVGhpcyBmdW5jdGlvbiB3aWxsIGF0dGVtcHQgdG8gZmx1c2ggc3R5bGluZyB2aWEgdGhlIHByb3ZpZGVkIGBjbGFzc2VzQ29udGV4dGBcbiAqIGFuZCBgc3R5bGVzQ29udGV4dGAgY29udGV4dCB2YWx1ZXMuIFRoaXMgZnVuY3Rpb24gaXMgZGVzaWduZWQgdG8gYmUgcnVuIGZyb21cbiAqIHRoZSBgc3R5bGluZ0FwcGx5KClgIGluc3RydWN0aW9uICh3aGljaCBpcyBydW4gYXQgdGhlIHZlcnkgZW5kIG9mIHN0eWxpbmdcbiAqIGNoYW5nZSBkZXRlY3Rpb24pIGFuZCB3aWxsIHJlbHkgb24gYW55IHN0YXRlIHZhbHVlcyB0aGF0IGFyZSBzZXQgZnJvbSB3aGVuXG4gKiBhbnkgc3R5bGluZyBiaW5kaW5ncyB1cGRhdGUuXG4gKlxuICogVGhpcyBmdW5jdGlvbiBtYXkgYmUgY2FsbGVkIG11bHRpcGxlIHRpbWVzIG9uIHRoZSBzYW1lIGVsZW1lbnQgYmVjYXVzZSBpdCBjYW5cbiAqIGJlIGNhbGxlZCBmcm9tIHRoZSB0ZW1wbGF0ZSBjb2RlIGFzIHdlbGwgYXMgZnJvbSBob3N0IGJpbmRpbmdzLiBJbiBvcmRlciBmb3JcbiAqIHN0eWxpbmcgdG8gYmUgc3VjY2Vzc2Z1bGx5IGZsdXNoZWQgdG8gdGhlIGVsZW1lbnQgKHdoaWNoIHdpbGwgb25seSBoYXBwZW4gb25jZVxuICogZGVzcGl0ZSB0aGlzIGJlaW5nIGNhbGxlZCBtdWx0aXBsZSB0aW1lcyksIHRoZSBmb2xsb3dpbmcgY3JpdGVyaWEgbXVzdCBiZSBtZXQ6XG4gKlxuICogLSBgZmx1c2hTdHlsaW5nYCBpcyBjYWxsZWQgZnJvbSB0aGUgdmVyeSBsYXN0IGRpcmVjdGl2ZSB0aGF0IGhhcyBzdHlsaW5nIGZvclxuICogICAgdGhlIGVsZW1lbnQgKHNlZSBgYWxsb3dTdHlsaW5nRmx1c2goKWApLlxuICogLSBvbmUgb3IgbW9yZSBiaW5kaW5ncyBmb3IgY2xhc3NlcyBvciBzdHlsZXMgaGFzIHVwZGF0ZWQgKHRoaXMgaXMgY2hlY2tlZCBieVxuICogICBleGFtaW5pbmcgdGhlIGNsYXNzZXMgb3Igc3R5bGVzIGJpdCBtYXNrKS5cbiAqXG4gKiBJZiB0aGUgc3R5bGUgYW5kIGNsYXNzIHZhbHVlcyBhcmUgc3VjY2Vzc2Z1bGx5IGFwcGxpZWQgdG8gdGhlIGVsZW1lbnQgdGhlblxuICogdGhlIHRlbXBvcmFyeSBzdGF0ZSB2YWx1ZXMgZm9yIHRoZSBlbGVtZW50IHdpbGwgYmUgY2xlYXJlZC4gT3RoZXJ3aXNlLCBpZlxuICogdGhpcyBkaWQgbm90IG9jY3VyIHRoZW4gdGhlIHN0eWxpbmcgc3RhdGUgaXMgcGVyc2lzdGVkIChzZWUgYHN0YXRlLnRzYCBmb3JcbiAqIG1vcmUgaW5mb3JtYXRpb24gb24gaG93IHRoaXMgd29ya3MpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZmx1c2hTdHlsaW5nKFxuICAgIHJlbmRlcmVyOiBSZW5kZXJlcjMgfCBQcm9jZWR1cmFsUmVuZGVyZXIzIHwgbnVsbCwgZGF0YTogTFN0eWxpbmdEYXRhLFxuICAgIGNsYXNzZXNDb250ZXh0OiBUU3R5bGluZ0NvbnRleHQgfCBudWxsLCBzdHlsZXNDb250ZXh0OiBUU3R5bGluZ0NvbnRleHQgfCBudWxsLFxuICAgIGVsZW1lbnQ6IFJFbGVtZW50LCBkaXJlY3RpdmVJbmRleDogbnVtYmVyLCBzdHlsZVNhbml0aXplcjogU3R5bGVTYW5pdGl6ZUZuIHwgbnVsbCk6IHZvaWQge1xuICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLmZsdXNoU3R5bGluZysrO1xuXG4gIGNvbnN0IHBlcnNpc3RTdGF0ZSA9IGNsYXNzZXNDb250ZXh0ID8gc3RhdGVJc1BlcnNpc3RlZChjbGFzc2VzQ29udGV4dCkgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIChzdHlsZXNDb250ZXh0ID8gc3RhdGVJc1BlcnNpc3RlZChzdHlsZXNDb250ZXh0KSA6IGZhbHNlKTtcbiAgY29uc3QgYWxsb3dGbHVzaENsYXNzZXMgPSBhbGxvd1N0eWxpbmdGbHVzaChjbGFzc2VzQ29udGV4dCwgZGlyZWN0aXZlSW5kZXgpO1xuICBjb25zdCBhbGxvd0ZsdXNoU3R5bGVzID0gYWxsb3dTdHlsaW5nRmx1c2goc3R5bGVzQ29udGV4dCwgZGlyZWN0aXZlSW5kZXgpO1xuXG4gIC8vIGRlZmVycmVkIGJpbmRpbmdzIGFyZSBiaW5kaW5ncyB3aGljaCBhcmUgc2NoZWR1bGVkIHRvIHJlZ2lzdGVyIHdpdGhcbiAgLy8gdGhlIGNvbnRleHQgYXQgYSBsYXRlciBwb2ludC4gVGhlc2UgYmluZGluZ3MgY2FuIG9ubHkgcmVnaXN0ZXJlZCB3aGVuXG4gIC8vIHRoZSBjb250ZXh0IHdpbGwgYmUgMTAwJSBmbHVzaGVkIHRvIHRoZSBlbGVtZW50LlxuICBpZiAoZGVmZXJyZWRCaW5kaW5nUXVldWUubGVuZ3RoICYmIChhbGxvd0ZsdXNoQ2xhc3NlcyB8fCBhbGxvd0ZsdXNoU3R5bGVzKSkge1xuICAgIGZsdXNoRGVmZXJyZWRCaW5kaW5ncygpO1xuICB9XG5cbiAgY29uc3Qgc3RhdGUgPSBnZXRTdHlsaW5nU3RhdGUoZWxlbWVudCwgcGVyc2lzdFN0YXRlKTtcbiAgY29uc3QgY2xhc3Nlc0ZsdXNoZWQgPSBtYXliZUFwcGx5U3R5bGluZyhcbiAgICAgIHJlbmRlcmVyLCBlbGVtZW50LCBkYXRhLCBjbGFzc2VzQ29udGV4dCwgYWxsb3dGbHVzaENsYXNzZXMsIHN0YXRlLmNsYXNzZXNCaXRNYXNrLCBzZXRDbGFzcyxcbiAgICAgIG51bGwpO1xuICBjb25zdCBzdHlsZXNGbHVzaGVkID0gbWF5YmVBcHBseVN0eWxpbmcoXG4gICAgICByZW5kZXJlciwgZWxlbWVudCwgZGF0YSwgc3R5bGVzQ29udGV4dCwgYWxsb3dGbHVzaFN0eWxlcywgc3RhdGUuc3R5bGVzQml0TWFzaywgc2V0U3R5bGUsXG4gICAgICBzdHlsZVNhbml0aXplcik7XG5cbiAgaWYgKGNsYXNzZXNGbHVzaGVkICYmIHN0eWxlc0ZsdXNoZWQpIHtcbiAgICByZXNldFN0eWxpbmdTdGF0ZSgpO1xuICAgIGlmIChwZXJzaXN0U3RhdGUpIHtcbiAgICAgIGRlbGV0ZVN0eWxpbmdTdGF0ZUZyb21TdG9yYWdlKGVsZW1lbnQpO1xuICAgIH1cbiAgfSBlbHNlIGlmIChwZXJzaXN0U3RhdGUpIHtcbiAgICBzdG9yZVN0eWxpbmdTdGF0ZShlbGVtZW50LCBzdGF0ZSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gbWF5YmVBcHBseVN0eWxpbmcoXG4gICAgcmVuZGVyZXI6IFJlbmRlcmVyMyB8IFByb2NlZHVyYWxSZW5kZXJlcjMgfCBudWxsLCBlbGVtZW50OiBSRWxlbWVudCwgZGF0YTogTFN0eWxpbmdEYXRhLFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCB8IG51bGwsIGFsbG93Rmx1c2g6IGJvb2xlYW4sIGJpdE1hc2s6IG51bWJlcixcbiAgICBzdHlsZVNldHRlcjogQXBwbHlTdHlsaW5nRm4sIHN0eWxlU2FuaXRpemVyOiBhbnkgfCBudWxsKTogYm9vbGVhbiB7XG4gIGlmIChhbGxvd0ZsdXNoICYmIGNvbnRleHQpIHtcbiAgICBsb2NrQW5kRmluYWxpemVDb250ZXh0KGNvbnRleHQpO1xuICAgIGlmIChjb250ZXh0SGFzVXBkYXRlcyhjb250ZXh0LCBiaXRNYXNrKSkge1xuICAgICAgbmdEZXZNb2RlICYmIChzdHlsZVNhbml0aXplciA/IG5nRGV2TW9kZS5zdHlsZXNBcHBsaWVkKysgOiBuZ0Rldk1vZGUuY2xhc3Nlc0FwcGxpZWQrKyk7XG4gICAgICBhcHBseVN0eWxpbmcoY29udGV4dCAhLCByZW5kZXJlciwgZWxlbWVudCwgZGF0YSwgYml0TWFzaywgc3R5bGVTZXR0ZXIsIHN0eWxlU2FuaXRpemVyKTtcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gYWxsb3dGbHVzaDtcbn1cblxuZnVuY3Rpb24gY29udGV4dEhhc1VwZGF0ZXMoY29udGV4dDogVFN0eWxpbmdDb250ZXh0IHwgbnVsbCwgYml0TWFzazogbnVtYmVyKSB7XG4gIHJldHVybiBjb250ZXh0ICYmIGJpdE1hc2sgPiBCSVRfTUFTS19TVEFSVF9WQUxVRTtcbn1cblxuLyoqXG4gKiBMb2NrcyB0aGUgY29udGV4dCAoc28gbm8gbW9yZSBiaW5kaW5ncyBjYW4gYmUgYWRkZWQpIGFuZCBhbHNvIGNvcGllcyBvdmVyIGluaXRpYWwgY2xhc3Mvc3R5bGVcbiAqIHZhbHVlcyBpbnRvIHRoZWlyIGJpbmRpbmcgYXJlYXMuXG4gKlxuICogVGhlcmUgYXJlIHR3byBtYWluIGFjdGlvbnMgdGhhdCB0YWtlIHBsYWNlIGluIHRoaXMgZnVuY3Rpb246XG4gKlxuICogLSBMb2NraW5nIHRoZSBjb250ZXh0OlxuICogICBMb2NraW5nIHRoZSBjb250ZXh0IGlzIHJlcXVpcmVkIHNvIHRoYXQgdGhlIHN0eWxlL2NsYXNzIGluc3RydWN0aW9ucyBrbm93IE5PVCB0b1xuICogICByZWdpc3RlciBhIGJpbmRpbmcgYWdhaW4gYWZ0ZXIgdGhlIGZpcnN0IHVwZGF0ZSBwYXNzIGhhcyBydW4uIElmIGEgbG9ja2luZyBiaXQgd2FzXG4gKiAgIG5vdCB1c2VkIHRoZW4gaXQgd291bGQgbmVlZCB0byBzY2FuIG92ZXIgdGhlIGNvbnRleHQgZWFjaCB0aW1lIGFuIGluc3RydWN0aW9uIGlzIHJ1blxuICogICAod2hpY2ggaXMgZXhwZW5zaXZlKS5cbiAqXG4gKiAtIFBhdGNoaW5nIGluaXRpYWwgdmFsdWVzOlxuICogICBEaXJlY3RpdmVzIGFuZCBjb21wb25lbnQgaG9zdCBiaW5kaW5ncyBtYXkgaW5jbHVkZSBzdGF0aWMgY2xhc3Mvc3R5bGUgdmFsdWVzIHdoaWNoIGFyZVxuICogICBib3VuZCB0byB0aGUgaG9zdCBlbGVtZW50LiBXaGVuIHRoaXMgaGFwcGVucywgdGhlIHN0eWxpbmcgY29udGV4dCB3aWxsIG5lZWQgdG8gYmUgaW5mb3JtZWRcbiAqICAgc28gaXQgY2FuIHVzZSB0aGVzZSBzdGF0aWMgc3R5bGluZyB2YWx1ZXMgYXMgZGVmYXVsdHMgd2hlbiBhIG1hdGNoaW5nIGJpbmRpbmcgaXMgZmFsc3kuXG4gKiAgIFRoZXNlIGluaXRpYWwgc3R5bGluZyB2YWx1ZXMgYXJlIHJlYWQgZnJvbSB0aGUgaW5pdGlhbCBzdHlsaW5nIHZhbHVlcyBzbG90IHdpdGhpbiB0aGVcbiAqICAgcHJvdmlkZWQgYFRTdHlsaW5nQ29udGV4dGAgKHdoaWNoIGlzIGFuIGluc3RhbmNlIG9mIGEgYFN0eWxpbmdNYXBBcnJheWApLiBUaGlzIGlubmVyIG1hcCB3aWxsXG4gKiAgIGJlIHVwZGF0ZWQgZWFjaCB0aW1lIGEgaG9zdCBiaW5kaW5nIGFwcGxpZXMgaXRzIHN0YXRpYyBzdHlsaW5nIHZhbHVlcyAodmlhIGBlbGVtZW50SG9zdEF0dHJzYClcbiAqICAgc28gdGhlc2UgdmFsdWVzIGFyZSBvbmx5IHJlYWQgYXQgdGhpcyBwb2ludCBiZWNhdXNlIHRoaXMgaXMgdGhlIHZlcnkgbGFzdCBwb2ludCBiZWZvcmUgdGhlXG4gKiAgIGZpcnN0IHN0eWxlL2NsYXNzIHZhbHVlcyBhcmUgZmx1c2hlZCB0byB0aGUgZWxlbWVudC5cbiAqL1xuZnVuY3Rpb24gbG9ja0FuZEZpbmFsaXplQ29udGV4dChjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQpOiB2b2lkIHtcbiAgaWYgKCFpc0NvbnRleHRMb2NrZWQoY29udGV4dCkpIHtcbiAgICBjb25zdCBpbml0aWFsVmFsdWVzID0gZ2V0U3R5bGluZ01hcEFycmF5KGNvbnRleHQpO1xuICAgIGlmIChpbml0aWFsVmFsdWVzKSB7XG4gICAgICB1cGRhdGVJbml0aWFsU3R5bGluZ09uQ29udGV4dChjb250ZXh0LCBpbml0aWFsVmFsdWVzKTtcbiAgICB9XG4gICAgbG9ja0NvbnRleHQoY29udGV4dCk7XG4gIH1cbn1cblxuLyoqXG4gKiBSdW5zIHRocm91Z2ggdGhlIHByb3ZpZGVkIHN0eWxpbmcgY29udGV4dCBhbmQgYXBwbGllcyBlYWNoIHZhbHVlIHRvXG4gKiB0aGUgcHJvdmlkZWQgZWxlbWVudCAodmlhIHRoZSByZW5kZXJlcikgaWYgb25lIG9yIG1vcmUgdmFsdWVzIGFyZSBwcmVzZW50LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gd2lsbCBpdGVyYXRlIG92ZXIgYWxsIGVudHJpZXMgcHJlc2VudCBpbiB0aGUgcHJvdmlkZWRcbiAqIGBUU3R5bGluZ0NvbnRleHRgIGFycmF5IChib3RoIHByb3AtYmFzZWQgYW5kIG1hcC1iYXNlZCBiaW5kaW5ncykuLVxuICpcbiAqIEVhY2ggZW50cnksIHdpdGhpbiB0aGUgYFRTdHlsaW5nQ29udGV4dGAgYXJyYXksIGlzIHN0b3JlZCBhbHBoYWJldGljYWxseVxuICogYW5kIHRoaXMgbWVhbnMgdGhhdCBlYWNoIHByb3AvdmFsdWUgZW50cnkgd2lsbCBiZSBhcHBsaWVkIGluIG9yZGVyXG4gKiAoc28gbG9uZyBhcyBpdCBpcyBtYXJrZWQgZGlydHkgaW4gdGhlIHByb3ZpZGVkIGBiaXRNYXNrYCB2YWx1ZSkuXG4gKlxuICogSWYgdGhlcmUgYXJlIGFueSBtYXAtYmFzZWQgZW50cmllcyBwcmVzZW50ICh3aGljaCBhcmUgYXBwbGllZCB0byB0aGVcbiAqIGVsZW1lbnQgdmlhIHRoZSBgW3N0eWxlXWAgYW5kIGBbY2xhc3NdYCBiaW5kaW5ncykgdGhlbiB0aG9zZSBlbnRyaWVzXG4gKiB3aWxsIGJlIGFwcGxpZWQgYXMgd2VsbC4gSG93ZXZlciwgdGhlIGNvZGUgZm9yIHRoYXQgaXMgbm90IGEgcGFydCBvZlxuICogdGhpcyBmdW5jdGlvbi4gSW5zdGVhZCwgZWFjaCB0aW1lIGEgcHJvcGVydHkgaXMgdmlzaXRlZCwgdGhlbiB0aGVcbiAqIGNvZGUgYmVsb3cgd2lsbCBjYWxsIGFuIGV4dGVybmFsIGZ1bmN0aW9uIGNhbGxlZCBgc3R5bGluZ01hcHNTeW5jRm5gXG4gKiBhbmQsIGlmIHByZXNlbnQsIGl0IHdpbGwga2VlcCB0aGUgYXBwbGljYXRpb24gb2Ygc3R5bGluZyB2YWx1ZXMgaW5cbiAqIG1hcC1iYXNlZCBiaW5kaW5ncyB1cCB0byBzeW5jIHdpdGggdGhlIGFwcGxpY2F0aW9uIG9mIHByb3AtYmFzZWRcbiAqIGJpbmRpbmdzLlxuICpcbiAqIFZpc2l0IGBzdHlsaW5nX25leHQvbWFwX2Jhc2VkX2JpbmRpbmdzLnRzYCB0byBsZWFybiBtb3JlIGFib3V0IGhvdyB0aGVcbiAqIGFsZ29yaXRobSB3b3JrcyBmb3IgbWFwLWJhc2VkIHN0eWxpbmcgYmluZGluZ3MuXG4gKlxuICogTm90ZSB0aGF0IHRoaXMgZnVuY3Rpb24gaXMgbm90IGRlc2lnbmVkIHRvIGJlIGNhbGxlZCBpbiBpc29sYXRpb24gKHVzZVxuICogYGFwcGx5Q2xhc3Nlc2AgYW5kIGBhcHBseVN0eWxlc2AgdG8gYWN0dWFsbHkgYXBwbHkgc3R5bGluZyB2YWx1ZXMpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gYXBwbHlTdHlsaW5nKFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgcmVuZGVyZXI6IFJlbmRlcmVyMyB8IFByb2NlZHVyYWxSZW5kZXJlcjMgfCBudWxsLCBlbGVtZW50OiBSRWxlbWVudCxcbiAgICBiaW5kaW5nRGF0YTogTFN0eWxpbmdEYXRhLCBiaXRNYXNrVmFsdWU6IG51bWJlciB8IGJvb2xlYW4sIGFwcGx5U3R5bGluZ0ZuOiBBcHBseVN0eWxpbmdGbixcbiAgICBzYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbiB8IG51bGwpIHtcbiAgY29uc3QgYml0TWFzayA9IG5vcm1hbGl6ZUJpdE1hc2tWYWx1ZShiaXRNYXNrVmFsdWUpO1xuICBjb25zdCBzdHlsaW5nTWFwc1N5bmNGbiA9IGdldFN0eWxpbmdNYXBzU3luY0ZuKCk7XG4gIGNvbnN0IG1hcHNHdWFyZE1hc2sgPSBnZXRHdWFyZE1hc2soY29udGV4dCwgVFN0eWxpbmdDb250ZXh0SW5kZXguTWFwQmluZGluZ3NQb3NpdGlvbik7XG4gIGNvbnN0IGFwcGx5QWxsVmFsdWVzID0gKGJpdE1hc2sgJiBtYXBzR3VhcmRNYXNrKSA+IDA7XG4gIGNvbnN0IG1hcHNNb2RlID1cbiAgICAgIGFwcGx5QWxsVmFsdWVzID8gU3R5bGluZ01hcHNTeW5jTW9kZS5BcHBseUFsbFZhbHVlcyA6IFN0eWxpbmdNYXBzU3luY01vZGUuVHJhdmVyc2VWYWx1ZXM7XG5cbiAgbGV0IGkgPSBnZXRQcm9wVmFsdWVzU3RhcnRQb3NpdGlvbihjb250ZXh0KTtcbiAgd2hpbGUgKGkgPCBjb250ZXh0Lmxlbmd0aCkge1xuICAgIGNvbnN0IHZhbHVlc0NvdW50ID0gZ2V0VmFsdWVzQ291bnQoY29udGV4dCwgaSk7XG4gICAgY29uc3QgZ3VhcmRNYXNrID0gZ2V0R3VhcmRNYXNrKGNvbnRleHQsIGkpO1xuICAgIGlmIChiaXRNYXNrICYgZ3VhcmRNYXNrKSB7XG4gICAgICBsZXQgdmFsdWVBcHBsaWVkID0gZmFsc2U7XG4gICAgICBjb25zdCBwcm9wID0gZ2V0UHJvcChjb250ZXh0LCBpKTtcbiAgICAgIGNvbnN0IHZhbHVlc0NvdW50VXBUb0RlZmF1bHQgPSB2YWx1ZXNDb3VudCAtIDE7XG4gICAgICBjb25zdCBkZWZhdWx0VmFsdWUgPSBnZXRCaW5kaW5nVmFsdWUoY29udGV4dCwgaSwgdmFsdWVzQ291bnRVcFRvRGVmYXVsdCkgYXMgc3RyaW5nIHwgbnVsbDtcblxuICAgICAgLy8gY2FzZSAxOiBhcHBseSBwcm9wLWJhc2VkIHZhbHVlc1xuICAgICAgLy8gdHJ5IHRvIGFwcGx5IHRoZSBiaW5kaW5nIHZhbHVlcyBhbmQgc2VlIGlmIGEgbm9uLW51bGxcbiAgICAgIC8vIHZhbHVlIGdldHMgc2V0IGZvciB0aGUgc3R5bGluZyBiaW5kaW5nXG4gICAgICBmb3IgKGxldCBqID0gMDsgaiA8IHZhbHVlc0NvdW50VXBUb0RlZmF1bHQ7IGorKykge1xuICAgICAgICBjb25zdCBiaW5kaW5nSW5kZXggPSBnZXRCaW5kaW5nVmFsdWUoY29udGV4dCwgaSwgaikgYXMgbnVtYmVyO1xuICAgICAgICBjb25zdCB2YWx1ZSA9IGJpbmRpbmdEYXRhW2JpbmRpbmdJbmRleF07XG4gICAgICAgIGlmIChpc1N0eWxpbmdWYWx1ZURlZmluZWQodmFsdWUpKSB7XG4gICAgICAgICAgY29uc3QgZmluYWxWYWx1ZSA9IHNhbml0aXplciAmJiBpc1Nhbml0aXphdGlvblJlcXVpcmVkKGNvbnRleHQsIGkpID9cbiAgICAgICAgICAgICAgc2FuaXRpemVyKHByb3AsIHZhbHVlLCBTdHlsZVNhbml0aXplTW9kZS5TYW5pdGl6ZU9ubHkpIDpcbiAgICAgICAgICAgICAgdmFsdWU7XG4gICAgICAgICAgYXBwbHlTdHlsaW5nRm4ocmVuZGVyZXIsIGVsZW1lbnQsIHByb3AsIGZpbmFsVmFsdWUsIGJpbmRpbmdJbmRleCk7XG4gICAgICAgICAgdmFsdWVBcHBsaWVkID0gdHJ1ZTtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICAvLyBjYXNlIDI6IGFwcGx5IG1hcC1iYXNlZCB2YWx1ZXNcbiAgICAgIC8vIHRyYXZlcnNlIHRocm91Z2ggZWFjaCBtYXAtYmFzZWQgc3R5bGluZyBiaW5kaW5nIGFuZCB1cGRhdGUgYWxsIHZhbHVlcyB1cCB0b1xuICAgICAgLy8gdGhlIHByb3ZpZGVkIGBwcm9wYCB2YWx1ZS4gSWYgdGhlIHByb3BlcnR5IHdhcyBub3QgYXBwbGllZCBpbiB0aGUgbG9vcCBhYm92ZVxuICAgICAgLy8gdGhlbiBpdCB3aWxsIGJlIGF0dGVtcHRlZCB0byBiZSBhcHBsaWVkIGluIHRoZSBtYXBzIHN5bmMgY29kZSBiZWxvdy5cbiAgICAgIGlmIChzdHlsaW5nTWFwc1N5bmNGbikge1xuICAgICAgICAvLyBkZXRlcm1pbmUgd2hldGhlciBvciBub3QgdG8gYXBwbHkgdGhlIHRhcmdldCBwcm9wZXJ0eSBvciB0byBza2lwIGl0XG4gICAgICAgIGNvbnN0IG1vZGUgPSBtYXBzTW9kZSB8ICh2YWx1ZUFwcGxpZWQgPyBTdHlsaW5nTWFwc1N5bmNNb2RlLlNraXBUYXJnZXRQcm9wIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIFN0eWxpbmdNYXBzU3luY01vZGUuQXBwbHlUYXJnZXRQcm9wKTtcbiAgICAgICAgY29uc3QgdmFsdWVBcHBsaWVkV2l0aGluTWFwID0gc3R5bGluZ01hcHNTeW5jRm4oXG4gICAgICAgICAgICBjb250ZXh0LCByZW5kZXJlciwgZWxlbWVudCwgYmluZGluZ0RhdGEsIGFwcGx5U3R5bGluZ0ZuLCBzYW5pdGl6ZXIsIG1vZGUsIHByb3AsXG4gICAgICAgICAgICBkZWZhdWx0VmFsdWUpO1xuICAgICAgICB2YWx1ZUFwcGxpZWQgPSB2YWx1ZUFwcGxpZWQgfHwgdmFsdWVBcHBsaWVkV2l0aGluTWFwO1xuICAgICAgfVxuXG4gICAgICAvLyBjYXNlIDM6IGFwcGx5IHRoZSBkZWZhdWx0IHZhbHVlXG4gICAgICAvLyBpZiB0aGUgdmFsdWUgaGFzIG5vdCB5ZXQgYmVlbiBhcHBsaWVkIHRoZW4gYSB0cnV0aHkgdmFsdWUgZG9lcyBub3QgZXhpc3QgaW4gdGhlXG4gICAgICAvLyBwcm9wLWJhc2VkIG9yIG1hcC1iYXNlZCBiaW5kaW5ncyBjb2RlLiBJZiBhbmQgd2hlbiB0aGlzIGhhcHBlbnMsIGp1c3QgYXBwbHkgdGhlXG4gICAgICAvLyBkZWZhdWx0IHZhbHVlIChldmVuIGlmIHRoZSBkZWZhdWx0IHZhbHVlIGlzIGBudWxsYCkuXG4gICAgICBpZiAoIXZhbHVlQXBwbGllZCkge1xuICAgICAgICBhcHBseVN0eWxpbmdGbihyZW5kZXJlciwgZWxlbWVudCwgcHJvcCwgZGVmYXVsdFZhbHVlKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpICs9IFRTdHlsaW5nQ29udGV4dEluZGV4LkJpbmRpbmdzU3RhcnRPZmZzZXQgKyB2YWx1ZXNDb3VudDtcbiAgfVxuXG4gIC8vIHRoZSBtYXAtYmFzZWQgc3R5bGluZyBlbnRyaWVzIG1heSBoYXZlIG5vdCBhcHBsaWVkIGFsbCB0aGVpclxuICAvLyB2YWx1ZXMuIEZvciB0aGlzIHJlYXNvbiwgb25lIG1vcmUgY2FsbCB0byB0aGUgc3luYyBmdW5jdGlvblxuICAvLyBuZWVkcyB0byBiZSBpc3N1ZWQgYXQgdGhlIGVuZC5cbiAgaWYgKHN0eWxpbmdNYXBzU3luY0ZuKSB7XG4gICAgc3R5bGluZ01hcHNTeW5jRm4oY29udGV4dCwgcmVuZGVyZXIsIGVsZW1lbnQsIGJpbmRpbmdEYXRhLCBhcHBseVN0eWxpbmdGbiwgc2FuaXRpemVyLCBtYXBzTW9kZSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gbm9ybWFsaXplQml0TWFza1ZhbHVlKHZhbHVlOiBudW1iZXIgfCBib29sZWFuKTogbnVtYmVyIHtcbiAgLy8gaWYgcGFzcyA9PiBhcHBseSBhbGwgdmFsdWVzICgtMSBpbXBsaWVzIHRoYXQgYWxsIGJpdHMgYXJlIGZsaXBwZWQgdG8gdHJ1ZSlcbiAgaWYgKHZhbHVlID09PSB0cnVlKSByZXR1cm4gLTE7XG5cbiAgLy8gaWYgcGFzcyA9PiBza2lwIGFsbCB2YWx1ZXNcbiAgaWYgKHZhbHVlID09PSBmYWxzZSkgcmV0dXJuIDA7XG5cbiAgLy8gcmV0dXJuIHRoZSBiaXQgbWFzayB2YWx1ZSBhcyBpc1xuICByZXR1cm4gdmFsdWU7XG59XG5cbmxldCBfYWN0aXZlU3R5bGluZ01hcEFwcGx5Rm46IFN5bmNTdHlsaW5nTWFwc0ZufG51bGwgPSBudWxsO1xuZXhwb3J0IGZ1bmN0aW9uIGdldFN0eWxpbmdNYXBzU3luY0ZuKCkge1xuICByZXR1cm4gX2FjdGl2ZVN0eWxpbmdNYXBBcHBseUZuO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2V0U3R5bGluZ01hcHNTeW5jRm4oZm46IFN5bmNTdHlsaW5nTWFwc0ZuKSB7XG4gIF9hY3RpdmVTdHlsaW5nTWFwQXBwbHlGbiA9IGZuO1xufVxuXG4vKipcbiAqIEFzc2lnbnMgYSBzdHlsZSB2YWx1ZSB0byBhIHN0eWxlIHByb3BlcnR5IGZvciB0aGUgZ2l2ZW4gZWxlbWVudC5cbiAqL1xuY29uc3Qgc2V0U3R5bGU6IEFwcGx5U3R5bGluZ0ZuID1cbiAgICAocmVuZGVyZXI6IFJlbmRlcmVyMyB8IG51bGwsIG5hdGl2ZTogUkVsZW1lbnQsIHByb3A6IHN0cmluZywgdmFsdWU6IHN0cmluZyB8IG51bGwpID0+IHtcbiAgICAgIC8vIHRoZSByZWFzb24gd2h5IHRoaXMgbWF5IGJlIGBudWxsYCBpcyBlaXRoZXIgYmVjYXVzZVxuICAgICAgLy8gaXQncyBhIGNvbnRhaW5lciBlbGVtZW50IG9yIGl0J3MgYSBwYXJ0IG9mIGEgdGVzdFxuICAgICAgLy8gZW52aXJvbm1lbnQgdGhhdCBkb2Vzbid0IGhhdmUgc3R5bGluZy4gSW4gZWl0aGVyXG4gICAgICAvLyBjYXNlIGl0J3Mgc2FmZSBub3QgdG8gYXBwbHkgc3R5bGluZyB0byB0aGUgZWxlbWVudC5cbiAgICAgIGNvbnN0IG5hdGl2ZVN0eWxlID0gbmF0aXZlLnN0eWxlO1xuICAgICAgaWYgKHZhbHVlKSB7XG4gICAgICAgIC8vIG9wYWNpdHksIHotaW5kZXggYW5kIGZsZXhib3ggYWxsIGhhdmUgbnVtYmVyIHZhbHVlc1xuICAgICAgICAvLyBhbmQgdGhlc2UgbmVlZCB0byBiZSBjb252ZXJ0ZWQgaW50byBzdHJpbmdzIHNvIHRoYXRcbiAgICAgICAgLy8gdGhleSBjYW4gYmUgYXNzaWduZWQgcHJvcGVybHkuXG4gICAgICAgIHZhbHVlID0gdmFsdWUudG9TdHJpbmcoKTtcbiAgICAgICAgbmdEZXZNb2RlICYmIG5nRGV2TW9kZS5yZW5kZXJlclNldFN0eWxlKys7XG4gICAgICAgIHJlbmRlcmVyICYmIGlzUHJvY2VkdXJhbFJlbmRlcmVyKHJlbmRlcmVyKSA/XG4gICAgICAgICAgICByZW5kZXJlci5zZXRTdHlsZShuYXRpdmUsIHByb3AsIHZhbHVlLCBSZW5kZXJlclN0eWxlRmxhZ3MzLkRhc2hDYXNlKSA6XG4gICAgICAgICAgICAobmF0aXZlU3R5bGUgJiYgbmF0aXZlU3R5bGUuc2V0UHJvcGVydHkocHJvcCwgdmFsdWUpKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIG5nRGV2TW9kZSAmJiBuZ0Rldk1vZGUucmVuZGVyZXJSZW1vdmVTdHlsZSsrO1xuICAgICAgICByZW5kZXJlciAmJiBpc1Byb2NlZHVyYWxSZW5kZXJlcihyZW5kZXJlcikgP1xuICAgICAgICAgICAgcmVuZGVyZXIucmVtb3ZlU3R5bGUobmF0aXZlLCBwcm9wLCBSZW5kZXJlclN0eWxlRmxhZ3MzLkRhc2hDYXNlKSA6XG4gICAgICAgICAgICAobmF0aXZlU3R5bGUgJiYgbmF0aXZlU3R5bGUucmVtb3ZlUHJvcGVydHkocHJvcCkpO1xuICAgICAgfVxuICAgIH07XG5cbi8qKlxuICogQWRkcy9yZW1vdmVzIHRoZSBwcm92aWRlZCBjbGFzc05hbWUgdmFsdWUgdG8gdGhlIHByb3ZpZGVkIGVsZW1lbnQuXG4gKi9cbmNvbnN0IHNldENsYXNzOiBBcHBseVN0eWxpbmdGbiA9XG4gICAgKHJlbmRlcmVyOiBSZW5kZXJlcjMgfCBudWxsLCBuYXRpdmU6IFJFbGVtZW50LCBjbGFzc05hbWU6IHN0cmluZywgdmFsdWU6IGFueSkgPT4ge1xuICAgICAgaWYgKGNsYXNzTmFtZSAhPT0gJycpIHtcbiAgICAgICAgLy8gdGhlIHJlYXNvbiB3aHkgdGhpcyBtYXkgYmUgYG51bGxgIGlzIGVpdGhlciBiZWNhdXNlXG4gICAgICAgIC8vIGl0J3MgYSBjb250YWluZXIgZWxlbWVudCBvciBpdCdzIGEgcGFydCBvZiBhIHRlc3RcbiAgICAgICAgLy8gZW52aXJvbm1lbnQgdGhhdCBkb2Vzbid0IGhhdmUgc3R5bGluZy4gSW4gZWl0aGVyXG4gICAgICAgIC8vIGNhc2UgaXQncyBzYWZlIG5vdCB0byBhcHBseSBzdHlsaW5nIHRvIHRoZSBlbGVtZW50LlxuICAgICAgICBjb25zdCBjbGFzc0xpc3QgPSBuYXRpdmUuY2xhc3NMaXN0O1xuICAgICAgICBpZiAodmFsdWUpIHtcbiAgICAgICAgICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLnJlbmRlcmVyQWRkQ2xhc3MrKztcbiAgICAgICAgICByZW5kZXJlciAmJiBpc1Byb2NlZHVyYWxSZW5kZXJlcihyZW5kZXJlcikgPyByZW5kZXJlci5hZGRDbGFzcyhuYXRpdmUsIGNsYXNzTmFtZSkgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIChjbGFzc0xpc3QgJiYgY2xhc3NMaXN0LmFkZChjbGFzc05hbWUpKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLnJlbmRlcmVyUmVtb3ZlQ2xhc3MrKztcbiAgICAgICAgICByZW5kZXJlciAmJiBpc1Byb2NlZHVyYWxSZW5kZXJlcihyZW5kZXJlcikgPyByZW5kZXJlci5yZW1vdmVDbGFzcyhuYXRpdmUsIGNsYXNzTmFtZSkgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIChjbGFzc0xpc3QgJiYgY2xhc3NMaXN0LnJlbW92ZShjbGFzc05hbWUpKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH07XG5cbi8qKlxuICogSXRlcmF0ZXMgb3ZlciBhbGwgcHJvdmlkZWQgc3R5bGluZyBlbnRyaWVzIGFuZCByZW5kZXJzIHRoZW0gb24gdGhlIGVsZW1lbnQuXG4gKlxuICogVGhpcyBmdW5jdGlvbiBpcyB1c2VkIGFsb25nc2lkZSBhIGBTdHlsaW5nTWFwQXJyYXlgIGVudHJ5LiBUaGlzIGVudHJ5IGlzIG5vdFxuICogdGhlIHNhbWUgYXMgdGhlIGBUU3R5bGluZ0NvbnRleHRgIGFuZCBpcyBvbmx5IHJlYWxseSB1c2VkIHdoZW4gYW4gZWxlbWVudCBjb250YWluc1xuICogaW5pdGlhbCBzdHlsaW5nIHZhbHVlcyAoZS5nLiBgPGRpdiBzdHlsZT1cIndpZHRoOjIwMHB4XCI+YCksIGJ1dCBubyBzdHlsZS9jbGFzcyBiaW5kaW5nc1xuICogYXJlIHByZXNlbnQuIElmIGFuZCB3aGVuIHRoYXQgaGFwcGVucyB0aGVuIHRoaXMgZnVuY3Rpb24gd2lsbCBiZSBjYWxsZWQgdG8gcmVuZGVyIGFsbFxuICogaW5pdGlhbCBzdHlsaW5nIHZhbHVlcyBvbiBhbiBlbGVtZW50LlxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVuZGVyU3R5bGluZ01hcChcbiAgICByZW5kZXJlcjogUmVuZGVyZXIzLCBlbGVtZW50OiBSRWxlbWVudCwgc3R5bGluZ1ZhbHVlczogVFN0eWxpbmdDb250ZXh0IHwgU3R5bGluZ01hcEFycmF5IHwgbnVsbCxcbiAgICBpc0NsYXNzQmFzZWQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgY29uc3Qgc3R5bGluZ01hcEFyciA9IGdldFN0eWxpbmdNYXBBcnJheShzdHlsaW5nVmFsdWVzKTtcbiAgaWYgKHN0eWxpbmdNYXBBcnIpIHtcbiAgICBmb3IgKGxldCBpID0gU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbjsgaSA8IHN0eWxpbmdNYXBBcnIubGVuZ3RoO1xuICAgICAgICAgaSArPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5UdXBsZVNpemUpIHtcbiAgICAgIGNvbnN0IHByb3AgPSBnZXRNYXBQcm9wKHN0eWxpbmdNYXBBcnIsIGkpO1xuICAgICAgY29uc3QgdmFsdWUgPSBnZXRNYXBWYWx1ZShzdHlsaW5nTWFwQXJyLCBpKTtcbiAgICAgIGlmIChpc0NsYXNzQmFzZWQpIHtcbiAgICAgICAgc2V0Q2xhc3MocmVuZGVyZXIsIGVsZW1lbnQsIHByb3AsIHZhbHVlLCBudWxsKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHNldFN0eWxlKHJlbmRlcmVyLCBlbGVtZW50LCBwcm9wLCB2YWx1ZSwgbnVsbCk7XG4gICAgICB9XG4gICAgfVxuICB9XG59XG5cbi8qKlxuICogUmVnaXN0ZXJzIGFsbCBpbml0aWFsIHN0eWxpbmcgZW50cmllcyBpbnRvIHRoZSBwcm92aWRlZCBjb250ZXh0LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gd2lsbCBpdGVyYXRlIG92ZXIgYWxsIGVudHJpZXMgaW4gdGhlIHByb3ZpZGVkIGBpbml0aWFsU3R5bGluZ2AgYXJ9cmF5IGFuZCByZWdpc3RlclxuICogdGhlbSBhcyBkZWZhdWx0IChpbml0aWFsKSB2YWx1ZXMgaW4gdGhlIHByb3ZpZGVkIGNvbnRleHQuIEluaXRpYWwgc3R5bGluZyB2YWx1ZXMgaW4gYSBjb250ZXh0IGFyZVxuICogdGhlIGRlZmF1bHQgdmFsdWVzIHRoYXQgYXJlIHRvIGJlIGFwcGxpZWQgdW5sZXNzIG92ZXJ3cml0dGVuIGJ5IGEgYmluZGluZy5cbiAqXG4gKiBUaGUgcmVhc29uIHdoeSB0aGlzIGZ1bmN0aW9uIGV4aXN0cyBhbmQgaXNuJ3QgYSBwYXJ0IG9mIHRoZSBjb250ZXh0IGNvbnN0cnVjdGlvbiBpcyBiZWNhdXNlXG4gKiBob3N0IGJpbmRpbmcgaXMgZXZhbHVhdGVkIGF0IGEgbGF0ZXIgc3RhZ2UgYWZ0ZXIgdGhlIGVsZW1lbnQgaXMgY3JlYXRlZC4gVGhpcyBtZWFucyB0aGF0XG4gKiBpZiBhIGRpcmVjdGl2ZSBvciBjb21wb25lbnQgY29udGFpbnMgYW55IGluaXRpYWwgc3R5bGluZyBjb2RlIChpLmUuIGA8ZGl2IGNsYXNzPVwiZm9vXCI+YClcbiAqIHRoZW4gdGhhdCBpbml0aWFsIHN0eWxpbmcgZGF0YSBjYW4gb25seSBiZSBhcHBsaWVkIG9uY2UgdGhlIHN0eWxpbmcgZm9yIHRoYXQgZWxlbWVudFxuICogaXMgZmlyc3QgYXBwbGllZCAoYXQgdGhlIGVuZCBvZiB0aGUgdXBkYXRlIHBoYXNlKS4gT25jZSB0aGF0IGhhcHBlbnMgdGhlbiB0aGUgY29udGV4dCB3aWxsXG4gKiB1cGRhdGUgaXRzZWxmIHdpdGggdGhlIGNvbXBsZXRlIGluaXRpYWwgc3R5bGluZyBmb3IgdGhlIGVsZW1lbnQuXG4gKi9cbmZ1bmN0aW9uIHVwZGF0ZUluaXRpYWxTdHlsaW5nT25Db250ZXh0KFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgaW5pdGlhbFN0eWxpbmc6IFN0eWxpbmdNYXBBcnJheSk6IHZvaWQge1xuICAvLyBgLTFgIGlzIHVzZWQgaGVyZSBiZWNhdXNlIGFsbCBpbml0aWFsIHN0eWxpbmcgZGF0YSBpcyBub3QgYSBzcGFydFxuICAvLyBvZiBhIGJpbmRpbmcgKHNpbmNlIGl0J3Mgc3RhdGljKVxuICBjb25zdCBJTklUSUFMX1NUWUxJTkdfQ09VTlRfSUQgPSAtMTtcblxuICBmb3IgKGxldCBpID0gU3R5bGluZ01hcEFycmF5SW5kZXguVmFsdWVzU3RhcnRQb3NpdGlvbjsgaSA8IGluaXRpYWxTdHlsaW5nLmxlbmd0aDtcbiAgICAgICBpICs9IFN0eWxpbmdNYXBBcnJheUluZGV4LlR1cGxlU2l6ZSkge1xuICAgIGNvbnN0IHZhbHVlID0gZ2V0TWFwVmFsdWUoaW5pdGlhbFN0eWxpbmcsIGkpO1xuICAgIGlmICh2YWx1ZSkge1xuICAgICAgY29uc3QgcHJvcCA9IGdldE1hcFByb3AoaW5pdGlhbFN0eWxpbmcsIGkpO1xuICAgICAgcmVnaXN0ZXJCaW5kaW5nKGNvbnRleHQsIElOSVRJQUxfU1RZTElOR19DT1VOVF9JRCwgcHJvcCwgdmFsdWUsIGZhbHNlKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==