/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { RendererStyleFlags3, isProceduralRenderer } from '../interfaces/renderer';
import { BIT_MASK_START_VALUE, deleteStylingStateFromStorage, getStylingState, resetStylingState, storeStylingState } from './state';
import { allowStylingFlush, getBindingValue, getGuardMask, getMapProp, getMapValue, getProp, getPropValuesStartPosition, getStylingMapArray, getValuesCount, hasValueChanged, isContextLocked, isSanitizationRequired, isStylingValueDefined, lockContext, setGuardMask, stateIsPersisted } from './util';
// The first bit value reflects a map-based binding value's bit.
// The reason why it's always activated for every entry in the map
// is so that if any map-binding values update then all other prop
// based bindings will pass the guard check automatically without
// any extra code or flags.
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
 * @type {?}
 */
export const DEFAULT_GUARD_MASK_VALUE = 0b1;
/**
 * The guard/update mask bit index location for map-based bindings.
 *
 * All map-based bindings (i.e. `[style]` and `[class]` )
 * @type {?}
 */
const STYLING_INDEX_FOR_MAP_BINDING = 0;
/**
 * Default fallback value for a styling binding.
 *
 * A value of `null` is used here which signals to the styling algorithm that
 * the styling value is not present. This way if there are no other values
 * detected then it will be removed once the style/class property is dirty and
 * diffed within the styling algorithm present in `flushStyling`.
 * @type {?}
 */
const DEFAULT_BINDING_VALUE = null;
/**
 * Default size count value for a new entry in a context.
 *
 * A value of `1` is used here because each entry in the context has a default
 * property.
 * @type {?}
 */
const DEFAULT_SIZE_VALUE = 1;
/** @type {?} */
let deferredBindingQueue = [];
/**
 * Visits a class-based binding and updates the new value (if changed).
 *
 * This function is called each time a class-based styling instruction
 * is executed. It's important that it's always called (even if the value
 * has not changed) so that the inner counter index value is incremented.
 * This way, each instruction is always guaranteed to get the same counter
 * state each time it's called (which then allows the `TStylingContext`
 * and the bit mask values to be in sync).
 * @param {?} context
 * @param {?} data
 * @param {?} element
 * @param {?} prop
 * @param {?} bindingIndex
 * @param {?} value
 * @param {?} deferRegistration
 * @param {?} forceUpdate
 * @return {?}
 */
export function updateClassBinding(context, data, element, prop, bindingIndex, value, deferRegistration, forceUpdate) {
    /** @type {?} */
    const isMapBased = !prop;
    /** @type {?} */
    const state = getStylingState(element, stateIsPersisted(context));
    /** @type {?} */
    const index = isMapBased ? STYLING_INDEX_FOR_MAP_BINDING : state.classesIndex++;
    /** @type {?} */
    const updated = updateBindingData(context, data, index, prop, bindingIndex, value, deferRegistration, forceUpdate, false);
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
 * @param {?} context
 * @param {?} data
 * @param {?} element
 * @param {?} prop
 * @param {?} bindingIndex
 * @param {?} value
 * @param {?} sanitizer
 * @param {?} deferRegistration
 * @param {?} forceUpdate
 * @return {?}
 */
export function updateStyleBinding(context, data, element, prop, bindingIndex, value, sanitizer, deferRegistration, forceUpdate) {
    /** @type {?} */
    const isMapBased = !prop;
    /** @type {?} */
    const state = getStylingState(element, stateIsPersisted(context));
    /** @type {?} */
    const index = isMapBased ? STYLING_INDEX_FOR_MAP_BINDING : state.stylesIndex++;
    /** @type {?} */
    const sanitizationRequired = isMapBased ?
        true :
        (sanitizer ? sanitizer((/** @type {?} */ (prop)), null, 1 /* ValidateProperty */) : false);
    /** @type {?} */
    const updated = updateBindingData(context, data, index, prop, bindingIndex, value, deferRegistration, forceUpdate, sanitizationRequired);
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
 * @param {?} context
 * @param {?} data
 * @param {?} counterIndex
 * @param {?} prop
 * @param {?} bindingIndex
 * @param {?} value
 * @param {?} deferRegistration
 * @param {?} forceUpdate
 * @param {?} sanitizationRequired
 * @return {?} whether or not the binding value was updated in the `LStylingData`.
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
    /** @type {?} */
    const changed = forceUpdate || hasValueChanged(data[bindingIndex], value);
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
 * @param {?} context
 * @param {?} counterIndex
 * @param {?} prop
 * @param {?} bindingIndex
 * @param {?} sanitizationRequired
 * @return {?}
 */
function deferBindingRegistration(context, counterIndex, prop, bindingIndex, sanitizationRequired) {
    deferredBindingQueue.unshift(context, counterIndex, prop, bindingIndex, sanitizationRequired);
}
/**
 * Flushes the collection of deferred bindings and causes each entry
 * to be registered into the context.
 * @return {?}
 */
function flushDeferredBindings() {
    /** @type {?} */
    let i = 0;
    while (i < deferredBindingQueue.length) {
        /** @type {?} */
        const context = (/** @type {?} */ (deferredBindingQueue[i++]));
        /** @type {?} */
        const count = (/** @type {?} */ (deferredBindingQueue[i++]));
        /** @type {?} */
        const prop = (/** @type {?} */ (deferredBindingQueue[i++]));
        /** @type {?} */
        const bindingIndex = (/** @type {?} */ (deferredBindingQueue[i++]));
        /** @type {?} */
        const sanitizationRequired = (/** @type {?} */ (deferredBindingQueue[i++]));
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
 * @param {?} context
 * @param {?} countId
 * @param {?} prop
 * @param {?} bindingValue
 * @param {?=} sanitizationRequired
 * @return {?}
 */
export function registerBinding(context, countId, prop, bindingValue, sanitizationRequired) {
    /** @type {?} */
    let registered = false;
    if (prop) {
        // prop-based bindings (e.g `<div [style.width]="w" [class.foo]="f">`)
        /** @type {?} */
        let found = false;
        /** @type {?} */
        let i = getPropValuesStartPosition(context);
        while (i < context.length) {
            /** @type {?} */
            const valuesCount = getValuesCount(context, i);
            /** @type {?} */
            const p = getProp(context, i);
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
/**
 * @param {?} context
 * @param {?} index
 * @param {?} prop
 * @param {?=} sanitizationRequired
 * @return {?}
 */
function allocateNewContextEntry(context, index, prop, sanitizationRequired) {
    // 1,2: splice index locations
    // 3: each entry gets a config value (guard mask + flags)
    // 4. each entry gets a size value (which is always one because there is always a default binding
    // value)
    // 5. the property that is getting allocated into the context
    // 6. the default binding value (usually `null`)
    /** @type {?} */
    const config = sanitizationRequired ? 1 /* SanitizationRequired */ :
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
 * @param {?} context
 * @param {?} isMapBased
 * @param {?} index
 * @param {?} bindingValue
 * @param {?} countId
 * @return {?}
 */
function addBindingIntoContext(context, isMapBased, index, bindingValue, countId) {
    /** @type {?} */
    const valuesCount = getValuesCount(context, index);
    /** @type {?} */
    const firstValueIndex = index + 3 /* BindingsStartOffset */;
    /** @type {?} */
    let lastValueIndex = firstValueIndex + valuesCount;
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
        for (let i = firstValueIndex; i <= lastValueIndex; i++) {
            /** @type {?} */
            const indexAtPosition = context[i];
            if (indexAtPosition === bindingValue)
                return;
        }
        context.splice(lastValueIndex, 0, bindingValue);
        ((/** @type {?} */ (context[index + 1 /* ValuesCountOffset */])))++;
        // now that a new binding index has been added to the property
        // the guard mask bit value (at the `countId` position) needs
        // to be included into the existing mask value.
        /** @type {?} */
        const guardMask = getGuardMask(context, index) | (1 << countId);
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
 * @param {?} renderer
 * @param {?} data
 * @param {?} classesContext
 * @param {?} stylesContext
 * @param {?} element
 * @param {?} directiveIndex
 * @param {?} styleSanitizer
 * @return {?}
 */
export function flushStyling(renderer, data, classesContext, stylesContext, element, directiveIndex, styleSanitizer) {
    ngDevMode && ngDevMode.flushStyling++;
    /** @type {?} */
    const persistState = classesContext ? stateIsPersisted(classesContext) :
        (stylesContext ? stateIsPersisted(stylesContext) : false);
    /** @type {?} */
    const allowFlushClasses = allowStylingFlush(classesContext, directiveIndex);
    /** @type {?} */
    const allowFlushStyles = allowStylingFlush(stylesContext, directiveIndex);
    // deferred bindings are bindings which are scheduled to register with
    // the context at a later point. These bindings can only registered when
    // the context will be 100% flushed to the element.
    if (deferredBindingQueue.length && (allowFlushClasses || allowFlushStyles)) {
        flushDeferredBindings();
    }
    /** @type {?} */
    const state = getStylingState(element, persistState);
    /** @type {?} */
    const classesFlushed = maybeApplyStyling(renderer, element, data, classesContext, allowFlushClasses, state.classesBitMask, setClass, null);
    /** @type {?} */
    const stylesFlushed = maybeApplyStyling(renderer, element, data, stylesContext, allowFlushStyles, state.stylesBitMask, setStyle, styleSanitizer);
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
/**
 * @param {?} renderer
 * @param {?} element
 * @param {?} data
 * @param {?} context
 * @param {?} allowFlush
 * @param {?} bitMask
 * @param {?} styleSetter
 * @param {?} styleSanitizer
 * @return {?}
 */
function maybeApplyStyling(renderer, element, data, context, allowFlush, bitMask, styleSetter, styleSanitizer) {
    if (allowFlush && context) {
        lockAndFinalizeContext(context);
        if (contextHasUpdates(context, bitMask)) {
            ngDevMode && (styleSanitizer ? ngDevMode.stylesApplied++ : ngDevMode.classesApplied++);
            applyStyling((/** @type {?} */ (context)), renderer, element, data, bitMask, styleSetter, styleSanitizer);
            return true;
        }
    }
    return allowFlush;
}
/**
 * @param {?} context
 * @param {?} bitMask
 * @return {?}
 */
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
 * @param {?} context
 * @return {?}
 */
function lockAndFinalizeContext(context) {
    if (!isContextLocked(context)) {
        /** @type {?} */
        const initialValues = getStylingMapArray(context);
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
 * @param {?} context
 * @param {?} renderer
 * @param {?} element
 * @param {?} bindingData
 * @param {?} bitMaskValue
 * @param {?} applyStylingFn
 * @param {?} sanitizer
 * @return {?}
 */
export function applyStyling(context, renderer, element, bindingData, bitMaskValue, applyStylingFn, sanitizer) {
    /** @type {?} */
    const bitMask = normalizeBitMaskValue(bitMaskValue);
    /** @type {?} */
    const stylingMapsSyncFn = getStylingMapsSyncFn();
    /** @type {?} */
    const mapsGuardMask = getGuardMask(context, 3 /* MapBindingsPosition */);
    /** @type {?} */
    const applyAllValues = (bitMask & mapsGuardMask) > 0;
    /** @type {?} */
    const mapsMode = applyAllValues ? 1 /* ApplyAllValues */ : 0 /* TraverseValues */;
    /** @type {?} */
    let i = getPropValuesStartPosition(context);
    while (i < context.length) {
        /** @type {?} */
        const valuesCount = getValuesCount(context, i);
        /** @type {?} */
        const guardMask = getGuardMask(context, i);
        if (bitMask & guardMask) {
            /** @type {?} */
            let valueApplied = false;
            /** @type {?} */
            const prop = getProp(context, i);
            /** @type {?} */
            const valuesCountUpToDefault = valuesCount - 1;
            /** @type {?} */
            const defaultValue = (/** @type {?} */ (getBindingValue(context, i, valuesCountUpToDefault)));
            // case 1: apply prop-based values
            // try to apply the binding values and see if a non-null
            // value gets set for the styling binding
            for (let j = 0; j < valuesCountUpToDefault; j++) {
                /** @type {?} */
                const bindingIndex = (/** @type {?} */ (getBindingValue(context, i, j)));
                /** @type {?} */
                const value = bindingData[bindingIndex];
                if (isStylingValueDefined(value)) {
                    /** @type {?} */
                    const finalValue = sanitizer && isSanitizationRequired(context, i) ?
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
                /** @type {?} */
                const mode = mapsMode | (valueApplied ? 4 /* SkipTargetProp */ :
                    2 /* ApplyTargetProp */);
                /** @type {?} */
                const valueAppliedWithinMap = stylingMapsSyncFn(context, renderer, element, bindingData, applyStylingFn, sanitizer, mode, prop, defaultValue);
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
/**
 * @param {?} value
 * @return {?}
 */
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
/** @type {?} */
let _activeStylingMapApplyFn = null;
/**
 * @return {?}
 */
export function getStylingMapsSyncFn() {
    return _activeStylingMapApplyFn;
}
/**
 * @param {?} fn
 * @return {?}
 */
export function setStylingMapsSyncFn(fn) {
    _activeStylingMapApplyFn = fn;
}
/**
 * Assigns a style value to a style property for the given element.
 * @type {?}
 */
const setStyle = (/**
 * @param {?} renderer
 * @param {?} native
 * @param {?} prop
 * @param {?} value
 * @return {?}
 */
(renderer, native, prop, value) => {
    // the reason why this may be `null` is either because
    // it's a container element or it's a part of a test
    // environment that doesn't have styling. In either
    // case it's safe not to apply styling to the element.
    /** @type {?} */
    const nativeStyle = native.style;
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
});
const ɵ0 = setStyle;
/**
 * Adds/removes the provided className value to the provided element.
 * @type {?}
 */
const setClass = (/**
 * @param {?} renderer
 * @param {?} native
 * @param {?} className
 * @param {?} value
 * @return {?}
 */
(renderer, native, className, value) => {
    if (className !== '') {
        // the reason why this may be `null` is either because
        // it's a container element or it's a part of a test
        // environment that doesn't have styling. In either
        // case it's safe not to apply styling to the element.
        /** @type {?} */
        const classList = native.classList;
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
});
const ɵ1 = setClass;
/**
 * Iterates over all provided styling entries and renders them on the element.
 *
 * This function is used alongside a `StylingMapArray` entry. This entry is not
 * the same as the `TStylingContext` and is only really used when an element contains
 * initial styling values (e.g. `<div style="width:200px">`), but no style/class bindings
 * are present. If and when that happens then this function will be called to render all
 * initial styling values on an element.
 * @param {?} renderer
 * @param {?} element
 * @param {?} stylingValues
 * @param {?} isClassBased
 * @return {?}
 */
export function renderStylingMap(renderer, element, stylingValues, isClassBased) {
    /** @type {?} */
    const stylingMapArr = getStylingMapArray(stylingValues);
    if (stylingMapArr) {
        for (let i = 1 /* ValuesStartPosition */; i < stylingMapArr.length; i += 2 /* TupleSize */) {
            /** @type {?} */
            const prop = getMapProp(stylingMapArr, i);
            /** @type {?} */
            const value = getMapValue(stylingMapArr, i);
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
 * @param {?} context
 * @param {?} initialStyling
 * @return {?}
 */
function updateInitialStylingOnContext(context, initialStyling) {
    // `-1` is used here because all initial styling data is not a spart
    // of a binding (since it's static)
    /** @type {?} */
    const INITIAL_STYLING_COUNT_ID = -1;
    for (let i = 1 /* ValuesStartPosition */; i < initialStyling.length; i += 2 /* TupleSize */) {
        /** @type {?} */
        const value = getMapValue(initialStyling, i);
        if (value) {
            /** @type {?} */
            const prop = getMapProp(initialStyling, i);
            registerBinding(context, INITIAL_STYLING_COUNT_ID, prop, value, false);
        }
    }
}
export { ɵ0, ɵ1 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYmluZGluZ3MuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL3N0eWxpbmdfbmV4dC9iaW5kaW5ncy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBUUEsT0FBTyxFQUEyQyxtQkFBbUIsRUFBRSxvQkFBb0IsRUFBQyxNQUFNLHdCQUF3QixDQUFDO0FBRzNILE9BQU8sRUFBQyxvQkFBb0IsRUFBRSw2QkFBNkIsRUFBRSxlQUFlLEVBQUUsaUJBQWlCLEVBQUUsaUJBQWlCLEVBQUMsTUFBTSxTQUFTLENBQUM7QUFDbkksT0FBTyxFQUFDLGlCQUFpQixFQUFFLGVBQWUsRUFBRSxZQUFZLEVBQUUsVUFBVSxFQUFFLFdBQVcsRUFBRSxPQUFPLEVBQUUsMEJBQTBCLEVBQUUsa0JBQWtCLEVBQUUsY0FBYyxFQUFFLGVBQWUsRUFBRSxlQUFlLEVBQUUsc0JBQXNCLEVBQUUscUJBQXFCLEVBQUUsV0FBVyxFQUFFLFlBQVksRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLFFBQVEsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQTRCeFMsTUFBTSxPQUFPLHdCQUF3QixHQUFHLEdBQUc7Ozs7Ozs7TUFPckMsNkJBQTZCLEdBQUcsQ0FBQzs7Ozs7Ozs7OztNQVVqQyxxQkFBcUIsR0FBRyxJQUFJOzs7Ozs7OztNQVE1QixrQkFBa0IsR0FBRyxDQUFDOztJQUV4QixvQkFBb0IsR0FBMkQsRUFBRTs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFZckYsTUFBTSxVQUFVLGtCQUFrQixDQUM5QixPQUF3QixFQUFFLElBQWtCLEVBQUUsT0FBaUIsRUFBRSxJQUFtQixFQUNwRixZQUFvQixFQUFFLEtBQTRELEVBQ2xGLGlCQUEwQixFQUFFLFdBQW9COztVQUM1QyxVQUFVLEdBQUcsQ0FBQyxJQUFJOztVQUNsQixLQUFLLEdBQUcsZUFBZSxDQUFDLE9BQU8sRUFBRSxnQkFBZ0IsQ0FBQyxP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USxDQUFDLFFBQVEsRUFBRSxPQUFPLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBRSxJQUFJLENBQUMsQ0FBQzthQUNoRDtTQUNGO0tBQ0Y7QUFDSCxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFnQkQsU0FBUyw2QkFBNkIsQ0FDbEMsT0FBd0IsRUFBRSxjQUErQjs7OztVQUdyRCx3QkFBd0IsR0FBRyxDQUFDLENBQUM7SUFFbkMsS0FBSyxJQUFJLENBQUMsOEJBQTJDLEVBQUUsQ0FBQyxHQUFHLGNBQWMsQ0FBQyxNQUFNLEVBQzNFLENBQUMscUJBQWtDLEVBQUU7O2NBQ2xDLEtBQUssR0FBRyxXQUFXLENBQUMsY0FBYyxFQUFFLENBQUMsQ0FBQztRQUM1QyxJQUFJLEtBQUssRUFBRTs7a0JBQ0gsSUFBSSxHQUFHLFVBQVUsQ0FBQyxjQUFjLEVBQUUsQ0FBQyxDQUFDO1lBQzFDLGVBQWUsQ0FBQyxPQUFPLEVBQUUsd0JBQXdCLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQztTQUN4RTtLQUNGO0FBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuKiBAbGljZW5zZVxuKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbipcbiogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuKi9cbmltcG9ydCB7U3R5bGVTYW5pdGl6ZUZuLCBTdHlsZVNhbml0aXplTW9kZX0gZnJvbSAnLi4vLi4vc2FuaXRpemF0aW9uL3N0eWxlX3Nhbml0aXplcic7XG5pbXBvcnQge1Byb2NlZHVyYWxSZW5kZXJlcjMsIFJFbGVtZW50LCBSZW5kZXJlcjMsIFJlbmRlcmVyU3R5bGVGbGFnczMsIGlzUHJvY2VkdXJhbFJlbmRlcmVyfSBmcm9tICcuLi9pbnRlcmZhY2VzL3JlbmRlcmVyJztcblxuaW1wb3J0IHtBcHBseVN0eWxpbmdGbiwgTFN0eWxpbmdEYXRhLCBTdHlsaW5nTWFwQXJyYXksIFN0eWxpbmdNYXBBcnJheUluZGV4LCBTdHlsaW5nTWFwc1N5bmNNb2RlLCBTeW5jU3R5bGluZ01hcHNGbiwgVFN0eWxpbmdDb250ZXh0LCBUU3R5bGluZ0NvbnRleHRJbmRleCwgVFN0eWxpbmdDb250ZXh0UHJvcENvbmZpZ0ZsYWdzfSBmcm9tICcuL2ludGVyZmFjZXMnO1xuaW1wb3J0IHtCSVRfTUFTS19TVEFSVF9WQUxVRSwgZGVsZXRlU3R5bGluZ1N0YXRlRnJvbVN0b3JhZ2UsIGdldFN0eWxpbmdTdGF0ZSwgcmVzZXRTdHlsaW5nU3RhdGUsIHN0b3JlU3R5bGluZ1N0YXRlfSBmcm9tICcuL3N0YXRlJztcbmltcG9ydCB7YWxsb3dTdHlsaW5nRmx1c2gsIGdldEJpbmRpbmdWYWx1ZSwgZ2V0R3VhcmRNYXNrLCBnZXRNYXBQcm9wLCBnZXRNYXBWYWx1ZSwgZ2V0UHJvcCwgZ2V0UHJvcFZhbHVlc1N0YXJ0UG9zaXRpb24sIGdldFN0eWxpbmdNYXBBcnJheSwgZ2V0VmFsdWVzQ291bnQsIGhhc1ZhbHVlQ2hhbmdlZCwgaXNDb250ZXh0TG9ja2VkLCBpc1Nhbml0aXphdGlvblJlcXVpcmVkLCBpc1N0eWxpbmdWYWx1ZURlZmluZWQsIGxvY2tDb250ZXh0LCBzZXRHdWFyZE1hc2ssIHN0YXRlSXNQZXJzaXN0ZWR9IGZyb20gJy4vdXRpbCc7XG5cblxuXG4vKipcbiAqIC0tLS0tLS0tXG4gKlxuICogVGhpcyBmaWxlIGNvbnRhaW5zIHRoZSBjb3JlIGxvZ2ljIGZvciBzdHlsaW5nIGluIEFuZ3VsYXIuXG4gKlxuICogQWxsIHN0eWxpbmcgYmluZGluZ3MgKGkuZS4gYFtzdHlsZV1gLCBgW3N0eWxlLnByb3BdYCwgYFtjbGFzc11gIGFuZCBgW2NsYXNzLm5hbWVdYClcbiAqIHdpbGwgaGF2ZSB0aGVpciB2YWx1ZXMgYmUgYXBwbGllZCB0aHJvdWdoIHRoZSBsb2dpYyBpbiB0aGlzIGZpbGUuXG4gKlxuICogV2hlbiBhIGJpbmRpbmcgaXMgZW5jb3VudGVyZWQgKGUuZy4gYDxkaXYgW3N0eWxlLndpZHRoXT1cIndcIj5gKSB0aGVuXG4gKiB0aGUgYmluZGluZyBkYXRhIHdpbGwgYmUgcG9wdWxhdGVkIGludG8gYSBgVFN0eWxpbmdDb250ZXh0YCBkYXRhLXN0cnVjdHVyZS5cbiAqIFRoZXJlIGlzIG9ubHkgb25lIGBUU3R5bGluZ0NvbnRleHRgIHBlciBgVE5vZGVgIGFuZCBlYWNoIGVsZW1lbnQgaW5zdGFuY2VcbiAqIHdpbGwgdXBkYXRlIGl0cyBzdHlsZS9jbGFzcyBiaW5kaW5nIHZhbHVlcyBpbiBjb25jZXJ0IHdpdGggdGhlIHN0eWxpbmdcbiAqIGNvbnRleHQuXG4gKlxuICogVG8gbGVhcm4gbW9yZSBhYm91dCB0aGUgYWxnb3JpdGhtIHNlZSBgVFN0eWxpbmdDb250ZXh0YC5cbiAqXG4gKiAtLS0tLS0tLVxuICovXG5cbi8vIFRoZSBmaXJzdCBiaXQgdmFsdWUgcmVmbGVjdHMgYSBtYXAtYmFzZWQgYmluZGluZyB2YWx1ZSdzIGJpdC5cbi8vIFRoZSByZWFzb24gd2h5IGl0J3MgYWx3YXlzIGFjdGl2YXRlZCBmb3IgZXZlcnkgZW50cnkgaW4gdGhlIG1hcFxuLy8gaXMgc28gdGhhdCBpZiBhbnkgbWFwLWJpbmRpbmcgdmFsdWVzIHVwZGF0ZSB0aGVuIGFsbCBvdGhlciBwcm9wXG4vLyBiYXNlZCBiaW5kaW5ncyB3aWxsIHBhc3MgdGhlIGd1YXJkIGNoZWNrIGF1dG9tYXRpY2FsbHkgd2l0aG91dFxuLy8gYW55IGV4dHJhIGNvZGUgb3IgZmxhZ3MuXG5leHBvcnQgY29uc3QgREVGQVVMVF9HVUFSRF9NQVNLX1ZBTFVFID0gMGIxO1xuXG4vKipcbiAqIFRoZSBndWFyZC91cGRhdGUgbWFzayBiaXQgaW5kZXggbG9jYXRpb24gZm9yIG1hcC1iYXNlZCBiaW5kaW5ncy5cbiAqXG4gKiBBbGwgbWFwLWJhc2VkIGJpbmRpbmdzIChpLmUuIGBbc3R5bGVdYCBhbmQgYFtjbGFzc11gIClcbiAqL1xuY29uc3QgU1RZTElOR19JTkRFWF9GT1JfTUFQX0JJTkRJTkcgPSAwO1xuXG4vKipcbiAqIERlZmF1bHQgZmFsbGJhY2sgdmFsdWUgZm9yIGEgc3R5bGluZyBiaW5kaW5nLlxuICpcbiAqIEEgdmFsdWUgb2YgYG51bGxgIGlzIHVzZWQgaGVyZSB3aGljaCBzaWduYWxzIHRvIHRoZSBzdHlsaW5nIGFsZ29yaXRobSB0aGF0XG4gKiB0aGUgc3R5bGluZyB2YWx1ZSBpcyBub3QgcHJlc2VudC4gVGhpcyB3YXkgaWYgdGhlcmUgYXJlIG5vIG90aGVyIHZhbHVlc1xuICogZGV0ZWN0ZWQgdGhlbiBpdCB3aWxsIGJlIHJlbW92ZWQgb25jZSB0aGUgc3R5bGUvY2xhc3MgcHJvcGVydHkgaXMgZGlydHkgYW5kXG4gKiBkaWZmZWQgd2l0aGluIHRoZSBzdHlsaW5nIGFsZ29yaXRobSBwcmVzZW50IGluIGBmbHVzaFN0eWxpbmdgLlxuICovXG5jb25zdCBERUZBVUxUX0JJTkRJTkdfVkFMVUUgPSBudWxsO1xuXG4vKipcbiAqIERlZmF1bHQgc2l6ZSBjb3VudCB2YWx1ZSBmb3IgYSBuZXcgZW50cnkgaW4gYSBjb250ZXh0LlxuICpcbiAqIEEgdmFsdWUgb2YgYDFgIGlzIHVzZWQgaGVyZSBiZWNhdXNlIGVhY2ggZW50cnkgaW4gdGhlIGNvbnRleHQgaGFzIGEgZGVmYXVsdFxuICogcHJvcGVydHkuXG4gKi9cbmNvbnN0IERFRkFVTFRfU0laRV9WQUxVRSA9IDE7XG5cbmxldCBkZWZlcnJlZEJpbmRpbmdRdWV1ZTogKFRTdHlsaW5nQ29udGV4dCB8IG51bWJlciB8IHN0cmluZyB8IG51bGwgfCBib29sZWFuKVtdID0gW107XG5cbi8qKlxuICogVmlzaXRzIGEgY2xhc3MtYmFzZWQgYmluZGluZyBhbmQgdXBkYXRlcyB0aGUgbmV3IHZhbHVlIChpZiBjaGFuZ2VkKS5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIGNhbGxlZCBlYWNoIHRpbWUgYSBjbGFzcy1iYXNlZCBzdHlsaW5nIGluc3RydWN0aW9uXG4gKiBpcyBleGVjdXRlZC4gSXQncyBpbXBvcnRhbnQgdGhhdCBpdCdzIGFsd2F5cyBjYWxsZWQgKGV2ZW4gaWYgdGhlIHZhbHVlXG4gKiBoYXMgbm90IGNoYW5nZWQpIHNvIHRoYXQgdGhlIGlubmVyIGNvdW50ZXIgaW5kZXggdmFsdWUgaXMgaW5jcmVtZW50ZWQuXG4gKiBUaGlzIHdheSwgZWFjaCBpbnN0cnVjdGlvbiBpcyBhbHdheXMgZ3VhcmFudGVlZCB0byBnZXQgdGhlIHNhbWUgY291bnRlclxuICogc3RhdGUgZWFjaCB0aW1lIGl0J3MgY2FsbGVkICh3aGljaCB0aGVuIGFsbG93cyB0aGUgYFRTdHlsaW5nQ29udGV4dGBcbiAqIGFuZCB0aGUgYml0IG1hc2sgdmFsdWVzIHRvIGJlIGluIHN5bmMpLlxuICovXG5leHBvcnQgZnVuY3Rpb24gdXBkYXRlQ2xhc3NCaW5kaW5nKFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgZGF0YTogTFN0eWxpbmdEYXRhLCBlbGVtZW50OiBSRWxlbWVudCwgcHJvcDogc3RyaW5nIHwgbnVsbCxcbiAgICBiaW5kaW5nSW5kZXg6IG51bWJlciwgdmFsdWU6IGJvb2xlYW4gfCBzdHJpbmcgfCBudWxsIHwgdW5kZWZpbmVkIHwgU3R5bGluZ01hcEFycmF5LFxuICAgIGRlZmVyUmVnaXN0cmF0aW9uOiBib29sZWFuLCBmb3JjZVVwZGF0ZTogYm9vbGVhbik6IGJvb2xlYW4ge1xuICBjb25zdCBpc01hcEJhc2VkID0gIXByb3A7XG4gIGNvbnN0IHN0YXRlID0gZ2V0U3R5bGluZ1N0YXRlKGVsZW1lbnQsIHN0YXRlSXNQZXJzaXN0ZWQoY29udGV4dCkpO1xuICBjb25zdCBpbmRleCA9IGlzTWFwQmFzZWQgPyBTVFlMSU5HX0lOREVYX0ZPUl9NQVBfQklORElORyA6IHN0YXRlLmNsYXNzZXNJbmRleCsrO1xuICBjb25zdCB1cGRhdGVkID0gdXBkYXRlQmluZGluZ0RhdGEoXG4gICAgICBjb250ZXh0LCBkYXRhLCBpbmRleCwgcHJvcCwgYmluZGluZ0luZGV4LCB2YWx1ZSwgZGVmZXJSZWdpc3RyYXRpb24sIGZvcmNlVXBkYXRlLCBmYWxzZSk7XG4gIGlmICh1cGRhdGVkIHx8IGZvcmNlVXBkYXRlKSB7XG4gICAgLy8gV2UgZmxpcCB0aGUgYml0IGluIHRoZSBiaXRNYXNrIHRvIHJlZmxlY3QgdGhhdCB0aGUgYmluZGluZ1xuICAgIC8vIGF0IHRoZSBgaW5kZXhgIHNsb3QgaGFzIGNoYW5nZWQuIFRoaXMgaWRlbnRpZmllcyB0byB0aGUgZmx1c2hpbmdcbiAgICAvLyBwaGFzZSB0aGF0IHRoZSBiaW5kaW5ncyBmb3IgdGhpcyBwYXJ0aWN1bGFyIENTUyBjbGFzcyBuZWVkIHRvIGJlXG4gICAgLy8gYXBwbGllZCBhZ2FpbiBiZWNhdXNlIG9uIG9yIG1vcmUgb2YgdGhlIGJpbmRpbmdzIGZvciB0aGUgQ1NTXG4gICAgLy8gY2xhc3MgaGF2ZSBjaGFuZ2VkLlxuICAgIHN0YXRlLmNsYXNzZXNCaXRNYXNrIHw9IDEgPDwgaW5kZXg7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cbiAgcmV0dXJuIGZhbHNlO1xufVxuXG4vKipcbiAqIFZpc2l0cyBhIHN0eWxlLWJhc2VkIGJpbmRpbmcgYW5kIHVwZGF0ZXMgdGhlIG5ldyB2YWx1ZSAoaWYgY2hhbmdlZCkuXG4gKlxuICogVGhpcyBmdW5jdGlvbiBpcyBjYWxsZWQgZWFjaCB0aW1lIGEgc3R5bGUtYmFzZWQgc3R5bGluZyBpbnN0cnVjdGlvblxuICogaXMgZXhlY3V0ZWQuIEl0J3MgaW1wb3J0YW50IHRoYXQgaXQncyBhbHdheXMgY2FsbGVkIChldmVuIGlmIHRoZSB2YWx1ZVxuICogaGFzIG5vdCBjaGFuZ2VkKSBzbyB0aGF0IHRoZSBpbm5lciBjb3VudGVyIGluZGV4IHZhbHVlIGlzIGluY3JlbWVudGVkLlxuICogVGhpcyB3YXksIGVhY2ggaW5zdHJ1Y3Rpb24gaXMgYWx3YXlzIGd1YXJhbnRlZWQgdG8gZ2V0IHRoZSBzYW1lIGNvdW50ZXJcbiAqIHN0YXRlIGVhY2ggdGltZSBpdCdzIGNhbGxlZCAod2hpY2ggdGhlbiBhbGxvd3MgdGhlIGBUU3R5bGluZ0NvbnRleHRgXG4gKiBhbmQgdGhlIGJpdCBtYXNrIHZhbHVlcyB0byBiZSBpbiBzeW5jKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHVwZGF0ZVN0eWxlQmluZGluZyhcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGRhdGE6IExTdHlsaW5nRGF0YSwgZWxlbWVudDogUkVsZW1lbnQsIHByb3A6IHN0cmluZyB8IG51bGwsXG4gICAgYmluZGluZ0luZGV4OiBudW1iZXIsIHZhbHVlOiBTdHJpbmcgfCBzdHJpbmcgfCBudW1iZXIgfCBudWxsIHwgdW5kZWZpbmVkIHwgU3R5bGluZ01hcEFycmF5LFxuICAgIHNhbml0aXplcjogU3R5bGVTYW5pdGl6ZUZuIHwgbnVsbCwgZGVmZXJSZWdpc3RyYXRpb246IGJvb2xlYW4sIGZvcmNlVXBkYXRlOiBib29sZWFuKTogYm9vbGVhbiB7XG4gIGNvbnN0IGlzTWFwQmFzZWQgPSAhcHJvcDtcbiAgY29uc3Qgc3RhdGUgPSBnZXRTdHlsaW5nU3RhdGUoZWxlbWVudCwgc3RhdGVJc1BlcnNpc3RlZChjb250ZXh0KSk7XG4gIGNvbnN0IGluZGV4ID0gaXNNYXBCYXNlZCA/IFNUWUxJTkdfSU5ERVhfRk9SX01BUF9CSU5ESU5HIDogc3RhdGUuc3R5bGVzSW5kZXgrKztcbiAgY29uc3Qgc2FuaXRpemF0aW9uUmVxdWlyZWQgPSBpc01hcEJhc2VkID9cbiAgICAgIHRydWUgOlxuICAgICAgKHNhbml0aXplciA/IHNhbml0aXplcihwcm9wICEsIG51bGwsIFN0eWxlU2FuaXRpemVNb2RlLlZhbGlkYXRlUHJvcGVydHkpIDogZmFsc2UpO1xuICBjb25zdCB1cGRhdGVkID0gdXBkYXRlQmluZGluZ0RhdGEoXG4gICAgICBjb250ZXh0LCBkYXRhLCBpbmRleCwgcHJvcCwgYmluZGluZ0luZGV4LCB2YWx1ZSwgZGVmZXJSZWdpc3RyYXRpb24sIGZvcmNlVXBkYXRlLFxuICAgICAgc2FuaXRpemF0aW9uUmVxdWlyZWQpO1xuICBpZiAodXBkYXRlZCB8fCBmb3JjZVVwZGF0ZSkge1xuICAgIC8vIFdlIGZsaXAgdGhlIGJpdCBpbiB0aGUgYml0TWFzayB0byByZWZsZWN0IHRoYXQgdGhlIGJpbmRpbmdcbiAgICAvLyBhdCB0aGUgYGluZGV4YCBzbG90IGhhcyBjaGFuZ2VkLiBUaGlzIGlkZW50aWZpZXMgdG8gdGhlIGZsdXNoaW5nXG4gICAgLy8gcGhhc2UgdGhhdCB0aGUgYmluZGluZ3MgZm9yIHRoaXMgcGFydGljdWxhciBwcm9wZXJ0eSBuZWVkIHRvIGJlXG4gICAgLy8gYXBwbGllZCBhZ2FpbiBiZWNhdXNlIG9uIG9yIG1vcmUgb2YgdGhlIGJpbmRpbmdzIGZvciB0aGUgQ1NTXG4gICAgLy8gcHJvcGVydHkgaGF2ZSBjaGFuZ2VkLlxuICAgIHN0YXRlLnN0eWxlc0JpdE1hc2sgfD0gMSA8PCBpbmRleDtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbi8qKlxuICogQ2FsbGVkIGVhY2ggdGltZSBhIGJpbmRpbmcgdmFsdWUgaGFzIGNoYW5nZWQgd2l0aGluIHRoZSBwcm92aWRlZCBgVFN0eWxpbmdDb250ZXh0YC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIGRlc2lnbmVkIHRvIGJlIGNhbGxlZCBmcm9tIGB1cGRhdGVTdHlsZUJpbmRpbmdgIGFuZCBgdXBkYXRlQ2xhc3NCaW5kaW5nYC5cbiAqIElmIGNhbGxlZCBkdXJpbmcgdGhlIGZpcnN0IHVwZGF0ZSBwYXNzLCB0aGUgYmluZGluZyB3aWxsIGJlIHJlZ2lzdGVyZWQgaW4gdGhlIGNvbnRleHQuXG4gKiBJZiB0aGUgYmluZGluZyBkb2VzIGdldCByZWdpc3RlcmVkIGFuZCB0aGUgYGRlZmVyUmVnaXN0cmF0aW9uYCBmbGFnIGlzIHRydWUgdGhlbiB0aGVcbiAqIGJpbmRpbmcgZGF0YSB3aWxsIGJlIHF1ZXVlZCB1cCB1bnRpbCB0aGUgY29udGV4dCBpcyBsYXRlciBmbHVzaGVkIGluIGBhcHBseVN0eWxpbmdgLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gd2lsbCBhbHNvIHVwZGF0ZSBiaW5kaW5nIHNsb3QgaW4gdGhlIHByb3ZpZGVkIGBMU3R5bGluZ0RhdGFgIHdpdGggdGhlXG4gKiBuZXcgYmluZGluZyBlbnRyeSAoaWYgaXQgaGFzIGNoYW5nZWQpLlxuICpcbiAqIEByZXR1cm5zIHdoZXRoZXIgb3Igbm90IHRoZSBiaW5kaW5nIHZhbHVlIHdhcyB1cGRhdGVkIGluIHRoZSBgTFN0eWxpbmdEYXRhYC5cbiAqL1xuZnVuY3Rpb24gdXBkYXRlQmluZGluZ0RhdGEoXG4gICAgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBkYXRhOiBMU3R5bGluZ0RhdGEsIGNvdW50ZXJJbmRleDogbnVtYmVyLCBwcm9wOiBzdHJpbmcgfCBudWxsLFxuICAgIGJpbmRpbmdJbmRleDogbnVtYmVyLFxuICAgIHZhbHVlOiBzdHJpbmcgfCBTdHJpbmcgfCBudW1iZXIgfCBib29sZWFuIHwgbnVsbCB8IHVuZGVmaW5lZCB8IFN0eWxpbmdNYXBBcnJheSxcbiAgICBkZWZlclJlZ2lzdHJhdGlvbjogYm9vbGVhbiwgZm9yY2VVcGRhdGU6IGJvb2xlYW4sIHNhbml0aXphdGlvblJlcXVpcmVkOiBib29sZWFuKTogYm9vbGVhbiB7XG4gIGlmICghaXNDb250ZXh0TG9ja2VkKGNvbnRleHQpKSB7XG4gICAgaWYgKGRlZmVyUmVnaXN0cmF0aW9uKSB7XG4gICAgICBkZWZlckJpbmRpbmdSZWdpc3RyYXRpb24oY29udGV4dCwgY291bnRlckluZGV4LCBwcm9wLCBiaW5kaW5nSW5kZXgsIHNhbml0aXphdGlvblJlcXVpcmVkKTtcbiAgICB9IGVsc2Uge1xuICAgICAgZGVmZXJyZWRCaW5kaW5nUXVldWUubGVuZ3RoICYmIGZsdXNoRGVmZXJyZWRCaW5kaW5ncygpO1xuXG4gICAgICAvLyB0aGlzIHdpbGwgb25seSBoYXBwZW4gZHVyaW5nIHRoZSBmaXJzdCB1cGRhdGUgcGFzcyBvZiB0aGVcbiAgICAgIC8vIGNvbnRleHQuIFRoZSByZWFzb24gd2h5IHdlIGNhbid0IHVzZSBgdE5vZGUuZmlyc3RUZW1wbGF0ZVBhc3NgXG4gICAgICAvLyBoZXJlIGlzIGJlY2F1c2UgaXRzIG5vdCBndWFyYW50ZWVkIHRvIGJlIHRydWUgd2hlbiB0aGUgZmlyc3RcbiAgICAgIC8vIHVwZGF0ZSBwYXNzIGlzIGV4ZWN1dGVkIChyZW1lbWJlciB0aGF0IGFsbCBzdHlsaW5nIGluc3RydWN0aW9uc1xuICAgICAgLy8gYXJlIHJ1biBpbiB0aGUgdXBkYXRlIHBoYXNlLCBhbmQsIGFzIGEgcmVzdWx0LCBhcmUgbm8gbW9yZVxuICAgICAgLy8gc3R5bGluZyBpbnN0cnVjdGlvbnMgdGhhdCBhcmUgcnVuIGluIHRoZSBjcmVhdGlvbiBwaGFzZSkuXG4gICAgICByZWdpc3RlckJpbmRpbmcoY29udGV4dCwgY291bnRlckluZGV4LCBwcm9wLCBiaW5kaW5nSW5kZXgsIHNhbml0aXphdGlvblJlcXVpcmVkKTtcbiAgICB9XG4gIH1cblxuICBjb25zdCBjaGFuZ2VkID0gZm9yY2VVcGRhdGUgfHwgaGFzVmFsdWVDaGFuZ2VkKGRhdGFbYmluZGluZ0luZGV4XSwgdmFsdWUpO1xuICBpZiAoY2hhbmdlZCkge1xuICAgIGRhdGFbYmluZGluZ0luZGV4XSA9IHZhbHVlO1xuICB9XG4gIHJldHVybiBjaGFuZ2VkO1xufVxuXG4vKipcbiAqIFNjaGVkdWxlcyBhIGJpbmRpbmcgcmVnaXN0cmF0aW9uIHRvIGJlIHJ1biBhdCBhIGxhdGVyIHBvaW50LlxuICpcbiAqIFRoZSByZWFzb25pbmcgZm9yIHRoaXMgZmVhdHVyZSBpcyB0byBlbnN1cmUgdGhhdCBzdHlsaW5nXG4gKiBiaW5kaW5ncyBhcmUgcmVnaXN0ZXJlZCBpbiB0aGUgY29ycmVjdCBvcmRlciBmb3Igd2hlblxuICogZGlyZWN0aXZlcy9jb21wb25lbnRzIGhhdmUgYSBzdXBlci9zdWIgY2xhc3MgaW5oZXJpdGFuY2VcbiAqIGNoYWlucy4gRWFjaCBkaXJlY3RpdmUncyBzdHlsaW5nIGJpbmRpbmdzIG11c3QgYmVcbiAqIHJlZ2lzdGVyZWQgaW50byB0aGUgY29udGV4dCBpbiByZXZlcnNlIG9yZGVyLiBUaGVyZWZvcmUgYWxsXG4gKiBiaW5kaW5ncyB3aWxsIGJlIGJ1ZmZlcmVkIGluIHJldmVyc2Ugb3JkZXIgYW5kIHRoZW4gYXBwbGllZFxuICogYWZ0ZXIgdGhlIGluaGVyaXRhbmNlIGNoYWluIGV4aXRzLlxuICovXG5mdW5jdGlvbiBkZWZlckJpbmRpbmdSZWdpc3RyYXRpb24oXG4gICAgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBjb3VudGVySW5kZXg6IG51bWJlciwgcHJvcDogc3RyaW5nIHwgbnVsbCwgYmluZGluZ0luZGV4OiBudW1iZXIsXG4gICAgc2FuaXRpemF0aW9uUmVxdWlyZWQ6IGJvb2xlYW4pIHtcbiAgZGVmZXJyZWRCaW5kaW5nUXVldWUudW5zaGlmdChjb250ZXh0LCBjb3VudGVySW5kZXgsIHByb3AsIGJpbmRpbmdJbmRleCwgc2FuaXRpemF0aW9uUmVxdWlyZWQpO1xufVxuXG4vKipcbiAqIEZsdXNoZXMgdGhlIGNvbGxlY3Rpb24gb2YgZGVmZXJyZWQgYmluZGluZ3MgYW5kIGNhdXNlcyBlYWNoIGVudHJ5XG4gKiB0byBiZSByZWdpc3RlcmVkIGludG8gdGhlIGNvbnRleHQuXG4gKi9cbmZ1bmN0aW9uIGZsdXNoRGVmZXJyZWRCaW5kaW5ncygpIHtcbiAgbGV0IGkgPSAwO1xuICB3aGlsZSAoaSA8IGRlZmVycmVkQmluZGluZ1F1ZXVlLmxlbmd0aCkge1xuICAgIGNvbnN0IGNvbnRleHQgPSBkZWZlcnJlZEJpbmRpbmdRdWV1ZVtpKytdIGFzIFRTdHlsaW5nQ29udGV4dDtcbiAgICBjb25zdCBjb3VudCA9IGRlZmVycmVkQmluZGluZ1F1ZXVlW2krK10gYXMgbnVtYmVyO1xuICAgIGNvbnN0IHByb3AgPSBkZWZlcnJlZEJpbmRpbmdRdWV1ZVtpKytdIGFzIHN0cmluZztcbiAgICBjb25zdCBiaW5kaW5nSW5kZXggPSBkZWZlcnJlZEJpbmRpbmdRdWV1ZVtpKytdIGFzIG51bWJlciB8IG51bGw7XG4gICAgY29uc3Qgc2FuaXRpemF0aW9uUmVxdWlyZWQgPSBkZWZlcnJlZEJpbmRpbmdRdWV1ZVtpKytdIGFzIGJvb2xlYW47XG4gICAgcmVnaXN0ZXJCaW5kaW5nKGNvbnRleHQsIGNvdW50LCBwcm9wLCBiaW5kaW5nSW5kZXgsIHNhbml0aXphdGlvblJlcXVpcmVkKTtcbiAgfVxuICBkZWZlcnJlZEJpbmRpbmdRdWV1ZS5sZW5ndGggPSAwO1xufVxuXG4vKipcbiAqIFJlZ2lzdGVycyB0aGUgcHJvdmlkZWQgYmluZGluZyAocHJvcCArIGJpbmRpbmdJbmRleCkgaW50byB0aGUgY29udGV4dC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIHNoYXJlZCBiZXR3ZWVuIGJpbmRpbmdzIHRoYXQgYXJlIGFzc2lnbmVkIGltbWVkaWF0ZWx5XG4gKiAodmlhIGB1cGRhdGVCaW5kaW5nRGF0YWApIGFuZCBhdCBhIGRlZmVycmVkIHN0YWdlLiBXaGVuIGNhbGxlZCwgaXQgd2lsbFxuICogZmlndXJlIG91dCBleGFjdGx5IHdoZXJlIHRvIHBsYWNlIHRoZSBiaW5kaW5nIGRhdGEgaW4gdGhlIGNvbnRleHQuXG4gKlxuICogSXQgaXMgbmVlZGVkIGJlY2F1c2UgaXQgd2lsbCBlaXRoZXIgdXBkYXRlIG9yIGluc2VydCBhIHN0eWxpbmcgcHJvcGVydHlcbiAqIGludG8gdGhlIGNvbnRleHQgYXQgdGhlIGNvcnJlY3Qgc3BvdC5cbiAqXG4gKiBXaGVuIGNhbGxlZCwgb25lIG9mIHR3byB0aGluZ3Mgd2lsbCBoYXBwZW46XG4gKlxuICogMSkgSWYgdGhlIHByb3BlcnR5IGFscmVhZHkgZXhpc3RzIGluIHRoZSBjb250ZXh0IHRoZW4gaXQgd2lsbCBqdXN0IGFkZFxuICogICAgdGhlIHByb3ZpZGVkIGBiaW5kaW5nVmFsdWVgIHRvIHRoZSBlbmQgb2YgdGhlIGJpbmRpbmcgc291cmNlcyByZWdpb25cbiAqICAgIGZvciB0aGF0IHBhcnRpY3VsYXIgcHJvcGVydHkuXG4gKlxuICogICAgLSBJZiB0aGUgYmluZGluZyB2YWx1ZSBpcyBhIG51bWJlciB0aGVuIGl0IHdpbGwgYmUgYWRkZWQgYXMgYSBuZXdcbiAqICAgICAgYmluZGluZyBpbmRleCBzb3VyY2UgbmV4dCB0byB0aGUgb3RoZXIgYmluZGluZyBzb3VyY2VzIGZvciB0aGUgcHJvcGVydHkuXG4gKlxuICogICAgLSBPdGhlcndpc2UsIGlmIHRoZSBiaW5kaW5nIHZhbHVlIGlzIGEgc3RyaW5nL2Jvb2xlYW4vbnVsbCB0eXBlIHRoZW4gaXQgd2lsbFxuICogICAgICByZXBsYWNlIHRoZSBkZWZhdWx0IHZhbHVlIGZvciB0aGUgcHJvcGVydHkgaWYgdGhlIGRlZmF1bHQgdmFsdWUgaXMgYG51bGxgLlxuICpcbiAqIDIpIElmIHRoZSBwcm9wZXJ0eSBkb2VzIG5vdCBleGlzdCB0aGVuIGl0IHdpbGwgYmUgaW5zZXJ0ZWQgaW50byB0aGUgY29udGV4dC5cbiAqICAgIFRoZSBzdHlsaW5nIGNvbnRleHQgcmVsaWVzIG9uIGFsbCBwcm9wZXJ0aWVzIGJlaW5nIHN0b3JlZCBpbiBhbHBoYWJldGljYWxcbiAqICAgIG9yZGVyLCBzbyBpdCBrbm93cyBleGFjdGx5IHdoZXJlIHRvIHN0b3JlIGl0LlxuICpcbiAqICAgIFdoZW4gaW5zZXJ0ZWQsIGEgZGVmYXVsdCBgbnVsbGAgdmFsdWUgaXMgY3JlYXRlZCBmb3IgdGhlIHByb3BlcnR5IHdoaWNoIGV4aXN0c1xuICogICAgYXMgdGhlIGRlZmF1bHQgdmFsdWUgZm9yIHRoZSBiaW5kaW5nLiBJZiB0aGUgYmluZGluZ1ZhbHVlIHByb3BlcnR5IGlzIGluc2VydGVkXG4gKiAgICBhbmQgaXQgaXMgZWl0aGVyIGEgc3RyaW5nLCBudW1iZXIgb3IgbnVsbCB2YWx1ZSB0aGVuIHRoYXQgd2lsbCByZXBsYWNlIHRoZSBkZWZhdWx0XG4gKiAgICB2YWx1ZS5cbiAqXG4gKiBOb3RlIHRoYXQgdGhpcyBmdW5jdGlvbiBpcyBhbHNvIHVzZWQgZm9yIG1hcC1iYXNlZCBzdHlsaW5nIGJpbmRpbmdzLiBUaGV5IGFyZSB0cmVhdGVkXG4gKiBtdWNoIHRoZSBzYW1lIGFzIHByb3AtYmFzZWQgYmluZGluZ3MsIGJ1dCwgYmVjYXVzZSB0aGV5IGRvIG5vdCBoYXZlIGEgcHJvcGVydHkgdmFsdWVcbiAqIChzaW5jZSBpdCdzIGEgbWFwKSwgYWxsIG1hcC1iYXNlZCBlbnRyaWVzIGFyZSBzdG9yZWQgaW4gYW4gYWxyZWFkeSBwb3B1bGF0ZWQgYXJlYSBvZlxuICogdGhlIGNvbnRleHQgYXQgdGhlIHRvcCAod2hpY2ggaXMgcmVzZXJ2ZWQgZm9yIG1hcC1iYXNlZCBlbnRyaWVzKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHJlZ2lzdGVyQmluZGluZyhcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGNvdW50SWQ6IG51bWJlciwgcHJvcDogc3RyaW5nIHwgbnVsbCxcbiAgICBiaW5kaW5nVmFsdWU6IG51bWJlciB8IG51bGwgfCBzdHJpbmcgfCBib29sZWFuLCBzYW5pdGl6YXRpb25SZXF1aXJlZD86IGJvb2xlYW4pOiBib29sZWFuIHtcbiAgbGV0IHJlZ2lzdGVyZWQgPSBmYWxzZTtcbiAgaWYgKHByb3ApIHtcbiAgICAvLyBwcm9wLWJhc2VkIGJpbmRpbmdzIChlLmcgYDxkaXYgW3N0eWxlLndpZHRoXT1cIndcIiBbY2xhc3MuZm9vXT1cImZcIj5gKVxuICAgIGxldCBmb3VuZCA9IGZhbHNlO1xuICAgIGxldCBpID0gZ2V0UHJvcFZhbHVlc1N0YXJ0UG9zaXRpb24oY29udGV4dCk7XG4gICAgd2hpbGUgKGkgPCBjb250ZXh0Lmxlbmd0aCkge1xuICAgICAgY29uc3QgdmFsdWVzQ291bnQgPSBnZXRWYWx1ZXNDb3VudChjb250ZXh0LCBpKTtcbiAgICAgIGNvbnN0IHAgPSBnZXRQcm9wKGNvbnRleHQsIGkpO1xuICAgICAgZm91bmQgPSBwcm9wIDw9IHA7XG4gICAgICBpZiAoZm91bmQpIHtcbiAgICAgICAgLy8gYWxsIHN0eWxlL2NsYXNzIGJpbmRpbmdzIGFyZSBzb3J0ZWQgYnkgcHJvcGVydHkgbmFtZVxuICAgICAgICBpZiAocHJvcCA8IHApIHtcbiAgICAgICAgICBhbGxvY2F0ZU5ld0NvbnRleHRFbnRyeShjb250ZXh0LCBpLCBwcm9wLCBzYW5pdGl6YXRpb25SZXF1aXJlZCk7XG4gICAgICAgIH1cbiAgICAgICAgYWRkQmluZGluZ0ludG9Db250ZXh0KGNvbnRleHQsIGZhbHNlLCBpLCBiaW5kaW5nVmFsdWUsIGNvdW50SWQpO1xuICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICAgIGkgKz0gVFN0eWxpbmdDb250ZXh0SW5kZXguQmluZGluZ3NTdGFydE9mZnNldCArIHZhbHVlc0NvdW50O1xuICAgIH1cblxuICAgIGlmICghZm91bmQpIHtcbiAgICAgIGFsbG9jYXRlTmV3Q29udGV4dEVudHJ5KGNvbnRleHQsIGNvbnRleHQubGVuZ3RoLCBwcm9wLCBzYW5pdGl6YXRpb25SZXF1aXJlZCk7XG4gICAgICBhZGRCaW5kaW5nSW50b0NvbnRleHQoY29udGV4dCwgZmFsc2UsIGksIGJpbmRpbmdWYWx1ZSwgY291bnRJZCk7XG4gICAgICByZWdpc3RlcmVkID0gdHJ1ZTtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgLy8gbWFwLWJhc2VkIGJpbmRpbmdzIChlLmcgYDxkaXYgW3N0eWxlXT1cInNcIiBbY2xhc3NdPVwie2NsYXNzTmFtZTp0cnVlfVwiPmApXG4gICAgLy8gdGhlcmUgaXMgbm8gbmVlZCB0byBhbGxvY2F0ZSB0aGUgbWFwLWJhc2VkIGJpbmRpbmcgcmVnaW9uIGludG8gdGhlIGNvbnRleHRcbiAgICAvLyBzaW5jZSBpdCBpcyBhbHJlYWR5IHRoZXJlIHdoZW4gdGhlIGNvbnRleHQgaXMgZmlyc3QgY3JlYXRlZC5cbiAgICBhZGRCaW5kaW5nSW50b0NvbnRleHQoXG4gICAgICAgIGNvbnRleHQsIHRydWUsIFRTdHlsaW5nQ29udGV4dEluZGV4Lk1hcEJpbmRpbmdzUG9zaXRpb24sIGJpbmRpbmdWYWx1ZSwgY291bnRJZCk7XG4gICAgcmVnaXN0ZXJlZCA9IHRydWU7XG4gIH1cbiAgcmV0dXJuIHJlZ2lzdGVyZWQ7XG59XG5cbmZ1bmN0aW9uIGFsbG9jYXRlTmV3Q29udGV4dEVudHJ5KFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgaW5kZXg6IG51bWJlciwgcHJvcDogc3RyaW5nLCBzYW5pdGl6YXRpb25SZXF1aXJlZD86IGJvb2xlYW4pIHtcbiAgLy8gMSwyOiBzcGxpY2UgaW5kZXggbG9jYXRpb25zXG4gIC8vIDM6IGVhY2ggZW50cnkgZ2V0cyBhIGNvbmZpZyB2YWx1ZSAoZ3VhcmQgbWFzayArIGZsYWdzKVxuICAvLyA0LiBlYWNoIGVudHJ5IGdldHMgYSBzaXplIHZhbHVlICh3aGljaCBpcyBhbHdheXMgb25lIGJlY2F1c2UgdGhlcmUgaXMgYWx3YXlzIGEgZGVmYXVsdCBiaW5kaW5nXG4gIC8vIHZhbHVlKVxuICAvLyA1LiB0aGUgcHJvcGVydHkgdGhhdCBpcyBnZXR0aW5nIGFsbG9jYXRlZCBpbnRvIHRoZSBjb250ZXh0XG4gIC8vIDYuIHRoZSBkZWZhdWx0IGJpbmRpbmcgdmFsdWUgKHVzdWFsbHkgYG51bGxgKVxuICBjb25zdCBjb25maWcgPSBzYW5pdGl6YXRpb25SZXF1aXJlZCA/IFRTdHlsaW5nQ29udGV4dFByb3BDb25maWdGbGFncy5TYW5pdGl6YXRpb25SZXF1aXJlZCA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgVFN0eWxpbmdDb250ZXh0UHJvcENvbmZpZ0ZsYWdzLkRlZmF1bHQ7XG4gIGNvbnRleHQuc3BsaWNlKGluZGV4LCAwLCBjb25maWcsIERFRkFVTFRfU0laRV9WQUxVRSwgcHJvcCwgREVGQVVMVF9CSU5ESU5HX1ZBTFVFKTtcbiAgc2V0R3VhcmRNYXNrKGNvbnRleHQsIGluZGV4LCBERUZBVUxUX0dVQVJEX01BU0tfVkFMVUUpO1xufVxuXG4vKipcbiAqIEluc2VydHMgYSBuZXcgYmluZGluZyB2YWx1ZSBpbnRvIGEgc3R5bGluZyBwcm9wZXJ0eSB0dXBsZSBpbiB0aGUgYFRTdHlsaW5nQ29udGV4dGAuXG4gKlxuICogQSBiaW5kaW5nVmFsdWUgaXMgaW5zZXJ0ZWQgaW50byBhIGNvbnRleHQgZHVyaW5nIHRoZSBmaXJzdCB1cGRhdGUgcGFzc1xuICogb2YgYSB0ZW1wbGF0ZSBvciBob3N0IGJpbmRpbmdzIGZ1bmN0aW9uLiBXaGVuIHRoaXMgb2NjdXJzLCB0d28gdGhpbmdzXG4gKiBoYXBwZW46XG4gKlxuICogLSBJZiB0aGUgYmluZGluZ1ZhbHVlIHZhbHVlIGlzIGEgbnVtYmVyIHRoZW4gaXQgaXMgdHJlYXRlZCBhcyBhIGJpbmRpbmdJbmRleFxuICogICB2YWx1ZSAoYSBpbmRleCBpbiB0aGUgYExWaWV3YCkgYW5kIGl0IHdpbGwgYmUgaW5zZXJ0ZWQgbmV4dCB0byB0aGUgb3RoZXJcbiAqICAgYmluZGluZyBpbmRleCBlbnRyaWVzLlxuICpcbiAqIC0gT3RoZXJ3aXNlIHRoZSBiaW5kaW5nIHZhbHVlIHdpbGwgdXBkYXRlIHRoZSBkZWZhdWx0IHZhbHVlIGZvciB0aGUgcHJvcGVydHlcbiAqICAgYW5kIHRoaXMgd2lsbCBvbmx5IGhhcHBlbiBpZiB0aGUgZGVmYXVsdCB2YWx1ZSBpcyBgbnVsbGAuXG4gKlxuICogTm90ZSB0aGF0IHRoaXMgZnVuY3Rpb24gYWxzbyBoYW5kbGVzIG1hcC1iYXNlZCBiaW5kaW5ncyBhbmQgd2lsbCBpbnNlcnQgdGhlbVxuICogYXQgdGhlIHRvcCBvZiB0aGUgY29udGV4dC5cbiAqL1xuZnVuY3Rpb24gYWRkQmluZGluZ0ludG9Db250ZXh0KFxuICAgIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCwgaXNNYXBCYXNlZDogYm9vbGVhbiwgaW5kZXg6IG51bWJlcixcbiAgICBiaW5kaW5nVmFsdWU6IG51bWJlciB8IHN0cmluZyB8IGJvb2xlYW4gfCBudWxsLCBjb3VudElkOiBudW1iZXIpIHtcbiAgY29uc3QgdmFsdWVzQ291bnQgPSBnZXRWYWx1ZXNDb3VudChjb250ZXh0LCBpbmRleCk7XG5cbiAgY29uc3QgZmlyc3RWYWx1ZUluZGV4ID0gaW5kZXggKyBUU3R5bGluZ0NvbnRleHRJbmRleC5CaW5kaW5nc1N0YXJ0T2Zmc2V0O1xuICBsZXQgbGFzdFZhbHVlSW5kZXggPSBmaXJzdFZhbHVlSW5kZXggKyB2YWx1ZXNDb3VudDtcbiAgaWYgKCFpc01hcEJhc2VkKSB7XG4gICAgLy8gcHJvcC1iYXNlZCB2YWx1ZXMgYWxsIGhhdmUgZGVmYXVsdCB2YWx1ZXMsIGJ1dCBtYXAtYmFzZWQgZW50cmllcyBkbyBub3QuXG4gICAgLy8gd2Ugd2FudCB0byBhY2Nlc3MgdGhlIGluZGV4IGZvciB0aGUgZGVmYXVsdCB2YWx1ZSBpbiB0aGlzIGNhc2UgYW5kIG5vdCBqdXN0XG4gICAgLy8gdGhlIGJpbmRpbmdzLi4uXG4gICAgbGFzdFZhbHVlSW5kZXgtLTtcbiAgfVxuXG4gIGlmICh0eXBlb2YgYmluZGluZ1ZhbHVlID09PSAnbnVtYmVyJykge1xuICAgIC8vIHRoZSBsb29wIGhlcmUgd2lsbCBjaGVjayB0byBzZWUgaWYgdGhlIGJpbmRpbmcgYWxyZWFkeSBleGlzdHNcbiAgICAvLyBmb3IgdGhlIHByb3BlcnR5IGluIHRoZSBjb250ZXh0LiBXaHk/IFRoZSByZWFzb24gZm9yIHRoaXMgaXNcbiAgICAvLyBiZWNhdXNlIHRoZSBzdHlsaW5nIGNvbnRleHQgaXMgbm90IFwibG9ja2VkXCIgdW50aWwgdGhlIGZpcnN0XG4gICAgLy8gZmx1c2ggaGFzIG9jY3VycmVkLiBUaGlzIG1lYW5zIHRoYXQgaWYgYSByZXBlYXRlZCBlbGVtZW50XG4gICAgLy8gcmVnaXN0ZXJzIGl0cyBzdHlsaW5nIGJpbmRpbmdzIHRoZW4gaXQgd2lsbCByZWdpc3RlciBlYWNoXG4gICAgLy8gYmluZGluZyBtb3JlIHRoYW4gb25jZSAoc2luY2UgaXRzIGR1cGxpY2F0ZWQpLiBUaGlzIGNoZWNrXG4gICAgLy8gd2lsbCBwcmV2ZW50IHRoYXQgZnJvbSBoYXBwZW5pbmcuIE5vdGUgdGhhdCB0aGlzIG9ubHkgaGFwcGVuc1xuICAgIC8vIHdoZW4gYSBiaW5kaW5nIGlzIGZpcnN0IGVuY291bnRlcmVkIGFuZCBub3QgZWFjaCB0aW1lIGl0IGlzXG4gICAgLy8gdXBkYXRlZC5cbiAgICBmb3IgKGxldCBpID0gZmlyc3RWYWx1ZUluZGV4OyBpIDw9IGxhc3RWYWx1ZUluZGV4OyBpKyspIHtcbiAgICAgIGNvbnN0IGluZGV4QXRQb3NpdGlvbiA9IGNvbnRleHRbaV07XG4gICAgICBpZiAoaW5kZXhBdFBvc2l0aW9uID09PSBiaW5kaW5nVmFsdWUpIHJldHVybjtcbiAgICB9XG5cbiAgICBjb250ZXh0LnNwbGljZShsYXN0VmFsdWVJbmRleCwgMCwgYmluZGluZ1ZhbHVlKTtcbiAgICAoY29udGV4dFtpbmRleCArIFRTdHlsaW5nQ29udGV4dEluZGV4LlZhbHVlc0NvdW50T2Zmc2V0XSBhcyBudW1iZXIpKys7XG5cbiAgICAvLyBub3cgdGhhdCBhIG5ldyBiaW5kaW5nIGluZGV4IGhhcyBiZWVuIGFkZGVkIHRvIHRoZSBwcm9wZXJ0eVxuICAgIC8vIHRoZSBndWFyZCBtYXNrIGJpdCB2YWx1ZSAoYXQgdGhlIGBjb3VudElkYCBwb3NpdGlvbikgbmVlZHNcbiAgICAvLyB0byBiZSBpbmNsdWRlZCBpbnRvIHRoZSBleGlzdGluZyBtYXNrIHZhbHVlLlxuICAgIGNvbnN0IGd1YXJkTWFzayA9IGdldEd1YXJkTWFzayhjb250ZXh0LCBpbmRleCkgfCAoMSA8PCBjb3VudElkKTtcbiAgICBzZXRHdWFyZE1hc2soY29udGV4dCwgaW5kZXgsIGd1YXJkTWFzayk7XG4gIH0gZWxzZSBpZiAoYmluZGluZ1ZhbHVlICE9PSBudWxsICYmIGNvbnRleHRbbGFzdFZhbHVlSW5kZXhdID09IG51bGwpIHtcbiAgICBjb250ZXh0W2xhc3RWYWx1ZUluZGV4XSA9IGJpbmRpbmdWYWx1ZTtcbiAgfVxufVxuXG4vKipcbiAqIEFwcGxpZXMgYWxsIHBlbmRpbmcgc3R5bGUgYW5kIGNsYXNzIGJpbmRpbmdzIHRvIHRoZSBwcm92aWRlZCBlbGVtZW50LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gd2lsbCBhdHRlbXB0IHRvIGZsdXNoIHN0eWxpbmcgdmlhIHRoZSBwcm92aWRlZCBgY2xhc3Nlc0NvbnRleHRgXG4gKiBhbmQgYHN0eWxlc0NvbnRleHRgIGNvbnRleHQgdmFsdWVzLiBUaGlzIGZ1bmN0aW9uIGlzIGRlc2lnbmVkIHRvIGJlIHJ1biBmcm9tXG4gKiB0aGUgYHN0eWxpbmdBcHBseSgpYCBpbnN0cnVjdGlvbiAod2hpY2ggaXMgcnVuIGF0IHRoZSB2ZXJ5IGVuZCBvZiBzdHlsaW5nXG4gKiBjaGFuZ2UgZGV0ZWN0aW9uKSBhbmQgd2lsbCByZWx5IG9uIGFueSBzdGF0ZSB2YWx1ZXMgdGhhdCBhcmUgc2V0IGZyb20gd2hlblxuICogYW55IHN0eWxpbmcgYmluZGluZ3MgdXBkYXRlLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gbWF5IGJlIGNhbGxlZCBtdWx0aXBsZSB0aW1lcyBvbiB0aGUgc2FtZSBlbGVtZW50IGJlY2F1c2UgaXQgY2FuXG4gKiBiZSBjYWxsZWQgZnJvbSB0aGUgdGVtcGxhdGUgY29kZSBhcyB3ZWxsIGFzIGZyb20gaG9zdCBiaW5kaW5ncy4gSW4gb3JkZXIgZm9yXG4gKiBzdHlsaW5nIHRvIGJlIHN1Y2Nlc3NmdWxseSBmbHVzaGVkIHRvIHRoZSBlbGVtZW50ICh3aGljaCB3aWxsIG9ubHkgaGFwcGVuIG9uY2VcbiAqIGRlc3BpdGUgdGhpcyBiZWluZyBjYWxsZWQgbXVsdGlwbGUgdGltZXMpLCB0aGUgZm9sbG93aW5nIGNyaXRlcmlhIG11c3QgYmUgbWV0OlxuICpcbiAqIC0gYGZsdXNoU3R5bGluZ2AgaXMgY2FsbGVkIGZyb20gdGhlIHZlcnkgbGFzdCBkaXJlY3RpdmUgdGhhdCBoYXMgc3R5bGluZyBmb3JcbiAqICAgIHRoZSBlbGVtZW50IChzZWUgYGFsbG93U3R5bGluZ0ZsdXNoKClgKS5cbiAqIC0gb25lIG9yIG1vcmUgYmluZGluZ3MgZm9yIGNsYXNzZXMgb3Igc3R5bGVzIGhhcyB1cGRhdGVkICh0aGlzIGlzIGNoZWNrZWQgYnlcbiAqICAgZXhhbWluaW5nIHRoZSBjbGFzc2VzIG9yIHN0eWxlcyBiaXQgbWFzaykuXG4gKlxuICogSWYgdGhlIHN0eWxlIGFuZCBjbGFzcyB2YWx1ZXMgYXJlIHN1Y2Nlc3NmdWxseSBhcHBsaWVkIHRvIHRoZSBlbGVtZW50IHRoZW5cbiAqIHRoZSB0ZW1wb3Jhcnkgc3RhdGUgdmFsdWVzIGZvciB0aGUgZWxlbWVudCB3aWxsIGJlIGNsZWFyZWQuIE90aGVyd2lzZSwgaWZcbiAqIHRoaXMgZGlkIG5vdCBvY2N1ciB0aGVuIHRoZSBzdHlsaW5nIHN0YXRlIGlzIHBlcnNpc3RlZCAoc2VlIGBzdGF0ZS50c2AgZm9yXG4gKiBtb3JlIGluZm9ybWF0aW9uIG9uIGhvdyB0aGlzIHdvcmtzKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGZsdXNoU3R5bGluZyhcbiAgICByZW5kZXJlcjogUmVuZGVyZXIzIHwgUHJvY2VkdXJhbFJlbmRlcmVyMyB8IG51bGwsIGRhdGE6IExTdHlsaW5nRGF0YSxcbiAgICBjbGFzc2VzQ29udGV4dDogVFN0eWxpbmdDb250ZXh0IHwgbnVsbCwgc3R5bGVzQ29udGV4dDogVFN0eWxpbmdDb250ZXh0IHwgbnVsbCxcbiAgICBlbGVtZW50OiBSRWxlbWVudCwgZGlyZWN0aXZlSW5kZXg6IG51bWJlciwgc3R5bGVTYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbiB8IG51bGwpOiB2b2lkIHtcbiAgbmdEZXZNb2RlICYmIG5nRGV2TW9kZS5mbHVzaFN0eWxpbmcrKztcblxuICBjb25zdCBwZXJzaXN0U3RhdGUgPSBjbGFzc2VzQ29udGV4dCA/IHN0YXRlSXNQZXJzaXN0ZWQoY2xhc3Nlc0NvbnRleHQpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAoc3R5bGVzQ29udGV4dCA/IHN0YXRlSXNQZXJzaXN0ZWQoc3R5bGVzQ29udGV4dCkgOiBmYWxzZSk7XG4gIGNvbnN0IGFsbG93Rmx1c2hDbGFzc2VzID0gYWxsb3dTdHlsaW5nRmx1c2goY2xhc3Nlc0NvbnRleHQsIGRpcmVjdGl2ZUluZGV4KTtcbiAgY29uc3QgYWxsb3dGbHVzaFN0eWxlcyA9IGFsbG93U3R5bGluZ0ZsdXNoKHN0eWxlc0NvbnRleHQsIGRpcmVjdGl2ZUluZGV4KTtcblxuICAvLyBkZWZlcnJlZCBiaW5kaW5ncyBhcmUgYmluZGluZ3Mgd2hpY2ggYXJlIHNjaGVkdWxlZCB0byByZWdpc3RlciB3aXRoXG4gIC8vIHRoZSBjb250ZXh0IGF0IGEgbGF0ZXIgcG9pbnQuIFRoZXNlIGJpbmRpbmdzIGNhbiBvbmx5IHJlZ2lzdGVyZWQgd2hlblxuICAvLyB0aGUgY29udGV4dCB3aWxsIGJlIDEwMCUgZmx1c2hlZCB0byB0aGUgZWxlbWVudC5cbiAgaWYgKGRlZmVycmVkQmluZGluZ1F1ZXVlLmxlbmd0aCAmJiAoYWxsb3dGbHVzaENsYXNzZXMgfHwgYWxsb3dGbHVzaFN0eWxlcykpIHtcbiAgICBmbHVzaERlZmVycmVkQmluZGluZ3MoKTtcbiAgfVxuXG4gIGNvbnN0IHN0YXRlID0gZ2V0U3R5bGluZ1N0YXRlKGVsZW1lbnQsIHBlcnNpc3RTdGF0ZSk7XG4gIGNvbnN0IGNsYXNzZXNGbHVzaGVkID0gbWF5YmVBcHBseVN0eWxpbmcoXG4gICAgICByZW5kZXJlciwgZWxlbWVudCwgZGF0YSwgY2xhc3Nlc0NvbnRleHQsIGFsbG93Rmx1c2hDbGFzc2VzLCBzdGF0ZS5jbGFzc2VzQml0TWFzaywgc2V0Q2xhc3MsXG4gICAgICBudWxsKTtcbiAgY29uc3Qgc3R5bGVzRmx1c2hlZCA9IG1heWJlQXBwbHlTdHlsaW5nKFxuICAgICAgcmVuZGVyZXIsIGVsZW1lbnQsIGRhdGEsIHN0eWxlc0NvbnRleHQsIGFsbG93Rmx1c2hTdHlsZXMsIHN0YXRlLnN0eWxlc0JpdE1hc2ssIHNldFN0eWxlLFxuICAgICAgc3R5bGVTYW5pdGl6ZXIpO1xuXG4gIGlmIChjbGFzc2VzRmx1c2hlZCAmJiBzdHlsZXNGbHVzaGVkKSB7XG4gICAgcmVzZXRTdHlsaW5nU3RhdGUoKTtcbiAgICBpZiAocGVyc2lzdFN0YXRlKSB7XG4gICAgICBkZWxldGVTdHlsaW5nU3RhdGVGcm9tU3RvcmFnZShlbGVtZW50KTtcbiAgICB9XG4gIH0gZWxzZSBpZiAocGVyc2lzdFN0YXRlKSB7XG4gICAgc3RvcmVTdHlsaW5nU3RhdGUoZWxlbWVudCwgc3RhdGUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIG1heWJlQXBwbHlTdHlsaW5nKFxuICAgIHJlbmRlcmVyOiBSZW5kZXJlcjMgfCBQcm9jZWR1cmFsUmVuZGVyZXIzIHwgbnVsbCwgZWxlbWVudDogUkVsZW1lbnQsIGRhdGE6IExTdHlsaW5nRGF0YSxcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQgfCBudWxsLCBhbGxvd0ZsdXNoOiBib29sZWFuLCBiaXRNYXNrOiBudW1iZXIsXG4gICAgc3R5bGVTZXR0ZXI6IEFwcGx5U3R5bGluZ0ZuLCBzdHlsZVNhbml0aXplcjogYW55IHwgbnVsbCk6IGJvb2xlYW4ge1xuICBpZiAoYWxsb3dGbHVzaCAmJiBjb250ZXh0KSB7XG4gICAgbG9ja0FuZEZpbmFsaXplQ29udGV4dChjb250ZXh0KTtcbiAgICBpZiAoY29udGV4dEhhc1VwZGF0ZXMoY29udGV4dCwgYml0TWFzaykpIHtcbiAgICAgIG5nRGV2TW9kZSAmJiAoc3R5bGVTYW5pdGl6ZXIgPyBuZ0Rldk1vZGUuc3R5bGVzQXBwbGllZCsrIDogbmdEZXZNb2RlLmNsYXNzZXNBcHBsaWVkKyspO1xuICAgICAgYXBwbHlTdHlsaW5nKGNvbnRleHQgISwgcmVuZGVyZXIsIGVsZW1lbnQsIGRhdGEsIGJpdE1hc2ssIHN0eWxlU2V0dGVyLCBzdHlsZVNhbml0aXplcik7XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGFsbG93Rmx1c2g7XG59XG5cbmZ1bmN0aW9uIGNvbnRleHRIYXNVcGRhdGVzKGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCB8IG51bGwsIGJpdE1hc2s6IG51bWJlcikge1xuICByZXR1cm4gY29udGV4dCAmJiBiaXRNYXNrID4gQklUX01BU0tfU1RBUlRfVkFMVUU7XG59XG5cbi8qKlxuICogTG9ja3MgdGhlIGNvbnRleHQgKHNvIG5vIG1vcmUgYmluZGluZ3MgY2FuIGJlIGFkZGVkKSBhbmQgYWxzbyBjb3BpZXMgb3ZlciBpbml0aWFsIGNsYXNzL3N0eWxlXG4gKiB2YWx1ZXMgaW50byB0aGVpciBiaW5kaW5nIGFyZWFzLlxuICpcbiAqIFRoZXJlIGFyZSB0d28gbWFpbiBhY3Rpb25zIHRoYXQgdGFrZSBwbGFjZSBpbiB0aGlzIGZ1bmN0aW9uOlxuICpcbiAqIC0gTG9ja2luZyB0aGUgY29udGV4dDpcbiAqICAgTG9ja2luZyB0aGUgY29udGV4dCBpcyByZXF1aXJlZCBzbyB0aGF0IHRoZSBzdHlsZS9jbGFzcyBpbnN0cnVjdGlvbnMga25vdyBOT1QgdG9cbiAqICAgcmVnaXN0ZXIgYSBiaW5kaW5nIGFnYWluIGFmdGVyIHRoZSBmaXJzdCB1cGRhdGUgcGFzcyBoYXMgcnVuLiBJZiBhIGxvY2tpbmcgYml0IHdhc1xuICogICBub3QgdXNlZCB0aGVuIGl0IHdvdWxkIG5lZWQgdG8gc2NhbiBvdmVyIHRoZSBjb250ZXh0IGVhY2ggdGltZSBhbiBpbnN0cnVjdGlvbiBpcyBydW5cbiAqICAgKHdoaWNoIGlzIGV4cGVuc2l2ZSkuXG4gKlxuICogLSBQYXRjaGluZyBpbml0aWFsIHZhbHVlczpcbiAqICAgRGlyZWN0aXZlcyBhbmQgY29tcG9uZW50IGhvc3QgYmluZGluZ3MgbWF5IGluY2x1ZGUgc3RhdGljIGNsYXNzL3N0eWxlIHZhbHVlcyB3aGljaCBhcmVcbiAqICAgYm91bmQgdG8gdGhlIGhvc3QgZWxlbWVudC4gV2hlbiB0aGlzIGhhcHBlbnMsIHRoZSBzdHlsaW5nIGNvbnRleHQgd2lsbCBuZWVkIHRvIGJlIGluZm9ybWVkXG4gKiAgIHNvIGl0IGNhbiB1c2UgdGhlc2Ugc3RhdGljIHN0eWxpbmcgdmFsdWVzIGFzIGRlZmF1bHRzIHdoZW4gYSBtYXRjaGluZyBiaW5kaW5nIGlzIGZhbHN5LlxuICogICBUaGVzZSBpbml0aWFsIHN0eWxpbmcgdmFsdWVzIGFyZSByZWFkIGZyb20gdGhlIGluaXRpYWwgc3R5bGluZyB2YWx1ZXMgc2xvdCB3aXRoaW4gdGhlXG4gKiAgIHByb3ZpZGVkIGBUU3R5bGluZ0NvbnRleHRgICh3aGljaCBpcyBhbiBpbnN0YW5jZSBvZiBhIGBTdHlsaW5nTWFwQXJyYXlgKS4gVGhpcyBpbm5lciBtYXAgd2lsbFxuICogICBiZSB1cGRhdGVkIGVhY2ggdGltZSBhIGhvc3QgYmluZGluZyBhcHBsaWVzIGl0cyBzdGF0aWMgc3R5bGluZyB2YWx1ZXMgKHZpYSBgZWxlbWVudEhvc3RBdHRyc2ApXG4gKiAgIHNvIHRoZXNlIHZhbHVlcyBhcmUgb25seSByZWFkIGF0IHRoaXMgcG9pbnQgYmVjYXVzZSB0aGlzIGlzIHRoZSB2ZXJ5IGxhc3QgcG9pbnQgYmVmb3JlIHRoZVxuICogICBmaXJzdCBzdHlsZS9jbGFzcyB2YWx1ZXMgYXJlIGZsdXNoZWQgdG8gdGhlIGVsZW1lbnQuXG4gKi9cbmZ1bmN0aW9uIGxvY2tBbmRGaW5hbGl6ZUNvbnRleHQoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KTogdm9pZCB7XG4gIGlmICghaXNDb250ZXh0TG9ja2VkKGNvbnRleHQpKSB7XG4gICAgY29uc3QgaW5pdGlhbFZhbHVlcyA9IGdldFN0eWxpbmdNYXBBcnJheShjb250ZXh0KTtcbiAgICBpZiAoaW5pdGlhbFZhbHVlcykge1xuICAgICAgdXBkYXRlSW5pdGlhbFN0eWxpbmdPbkNvbnRleHQoY29udGV4dCwgaW5pdGlhbFZhbHVlcyk7XG4gICAgfVxuICAgIGxvY2tDb250ZXh0KGNvbnRleHQpO1xuICB9XG59XG5cbi8qKlxuICogUnVucyB0aHJvdWdoIHRoZSBwcm92aWRlZCBzdHlsaW5nIGNvbnRleHQgYW5kIGFwcGxpZXMgZWFjaCB2YWx1ZSB0b1xuICogdGhlIHByb3ZpZGVkIGVsZW1lbnQgKHZpYSB0aGUgcmVuZGVyZXIpIGlmIG9uZSBvciBtb3JlIHZhbHVlcyBhcmUgcHJlc2VudC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIHdpbGwgaXRlcmF0ZSBvdmVyIGFsbCBlbnRyaWVzIHByZXNlbnQgaW4gdGhlIHByb3ZpZGVkXG4gKiBgVFN0eWxpbmdDb250ZXh0YCBhcnJheSAoYm90aCBwcm9wLWJhc2VkIGFuZCBtYXAtYmFzZWQgYmluZGluZ3MpLi1cbiAqXG4gKiBFYWNoIGVudHJ5LCB3aXRoaW4gdGhlIGBUU3R5bGluZ0NvbnRleHRgIGFycmF5LCBpcyBzdG9yZWQgYWxwaGFiZXRpY2FsbHlcbiAqIGFuZCB0aGlzIG1lYW5zIHRoYXQgZWFjaCBwcm9wL3ZhbHVlIGVudHJ5IHdpbGwgYmUgYXBwbGllZCBpbiBvcmRlclxuICogKHNvIGxvbmcgYXMgaXQgaXMgbWFya2VkIGRpcnR5IGluIHRoZSBwcm92aWRlZCBgYml0TWFza2AgdmFsdWUpLlxuICpcbiAqIElmIHRoZXJlIGFyZSBhbnkgbWFwLWJhc2VkIGVudHJpZXMgcHJlc2VudCAod2hpY2ggYXJlIGFwcGxpZWQgdG8gdGhlXG4gKiBlbGVtZW50IHZpYSB0aGUgYFtzdHlsZV1gIGFuZCBgW2NsYXNzXWAgYmluZGluZ3MpIHRoZW4gdGhvc2UgZW50cmllc1xuICogd2lsbCBiZSBhcHBsaWVkIGFzIHdlbGwuIEhvd2V2ZXIsIHRoZSBjb2RlIGZvciB0aGF0IGlzIG5vdCBhIHBhcnQgb2ZcbiAqIHRoaXMgZnVuY3Rpb24uIEluc3RlYWQsIGVhY2ggdGltZSBhIHByb3BlcnR5IGlzIHZpc2l0ZWQsIHRoZW4gdGhlXG4gKiBjb2RlIGJlbG93IHdpbGwgY2FsbCBhbiBleHRlcm5hbCBmdW5jdGlvbiBjYWxsZWQgYHN0eWxpbmdNYXBzU3luY0ZuYFxuICogYW5kLCBpZiBwcmVzZW50LCBpdCB3aWxsIGtlZXAgdGhlIGFwcGxpY2F0aW9uIG9mIHN0eWxpbmcgdmFsdWVzIGluXG4gKiBtYXAtYmFzZWQgYmluZGluZ3MgdXAgdG8gc3luYyB3aXRoIHRoZSBhcHBsaWNhdGlvbiBvZiBwcm9wLWJhc2VkXG4gKiBiaW5kaW5ncy5cbiAqXG4gKiBWaXNpdCBgc3R5bGluZ19uZXh0L21hcF9iYXNlZF9iaW5kaW5ncy50c2AgdG8gbGVhcm4gbW9yZSBhYm91dCBob3cgdGhlXG4gKiBhbGdvcml0aG0gd29ya3MgZm9yIG1hcC1iYXNlZCBzdHlsaW5nIGJpbmRpbmdzLlxuICpcbiAqIE5vdGUgdGhhdCB0aGlzIGZ1bmN0aW9uIGlzIG5vdCBkZXNpZ25lZCB0byBiZSBjYWxsZWQgaW4gaXNvbGF0aW9uICh1c2VcbiAqIGBhcHBseUNsYXNzZXNgIGFuZCBgYXBwbHlTdHlsZXNgIHRvIGFjdHVhbGx5IGFwcGx5IHN0eWxpbmcgdmFsdWVzKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGFwcGx5U3R5bGluZyhcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIHJlbmRlcmVyOiBSZW5kZXJlcjMgfCBQcm9jZWR1cmFsUmVuZGVyZXIzIHwgbnVsbCwgZWxlbWVudDogUkVsZW1lbnQsXG4gICAgYmluZGluZ0RhdGE6IExTdHlsaW5nRGF0YSwgYml0TWFza1ZhbHVlOiBudW1iZXIgfCBib29sZWFuLCBhcHBseVN0eWxpbmdGbjogQXBwbHlTdHlsaW5nRm4sXG4gICAgc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm4gfCBudWxsKSB7XG4gIGNvbnN0IGJpdE1hc2sgPSBub3JtYWxpemVCaXRNYXNrVmFsdWUoYml0TWFza1ZhbHVlKTtcbiAgY29uc3Qgc3R5bGluZ01hcHNTeW5jRm4gPSBnZXRTdHlsaW5nTWFwc1N5bmNGbigpO1xuICBjb25zdCBtYXBzR3VhcmRNYXNrID0gZ2V0R3VhcmRNYXNrKGNvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4Lk1hcEJpbmRpbmdzUG9zaXRpb24pO1xuICBjb25zdCBhcHBseUFsbFZhbHVlcyA9IChiaXRNYXNrICYgbWFwc0d1YXJkTWFzaykgPiAwO1xuICBjb25zdCBtYXBzTW9kZSA9XG4gICAgICBhcHBseUFsbFZhbHVlcyA/IFN0eWxpbmdNYXBzU3luY01vZGUuQXBwbHlBbGxWYWx1ZXMgOiBTdHlsaW5nTWFwc1N5bmNNb2RlLlRyYXZlcnNlVmFsdWVzO1xuXG4gIGxldCBpID0gZ2V0UHJvcFZhbHVlc1N0YXJ0UG9zaXRpb24oY29udGV4dCk7XG4gIHdoaWxlIChpIDwgY29udGV4dC5sZW5ndGgpIHtcbiAgICBjb25zdCB2YWx1ZXNDb3VudCA9IGdldFZhbHVlc0NvdW50KGNvbnRleHQsIGkpO1xuICAgIGNvbnN0IGd1YXJkTWFzayA9IGdldEd1YXJkTWFzayhjb250ZXh0LCBpKTtcbiAgICBpZiAoYml0TWFzayAmIGd1YXJkTWFzaykge1xuICAgICAgbGV0IHZhbHVlQXBwbGllZCA9IGZhbHNlO1xuICAgICAgY29uc3QgcHJvcCA9IGdldFByb3AoY29udGV4dCwgaSk7XG4gICAgICBjb25zdCB2YWx1ZXNDb3VudFVwVG9EZWZhdWx0ID0gdmFsdWVzQ291bnQgLSAxO1xuICAgICAgY29uc3QgZGVmYXVsdFZhbHVlID0gZ2V0QmluZGluZ1ZhbHVlKGNvbnRleHQsIGksIHZhbHVlc0NvdW50VXBUb0RlZmF1bHQpIGFzIHN0cmluZyB8IG51bGw7XG5cbiAgICAgIC8vIGNhc2UgMTogYXBwbHkgcHJvcC1iYXNlZCB2YWx1ZXNcbiAgICAgIC8vIHRyeSB0byBhcHBseSB0aGUgYmluZGluZyB2YWx1ZXMgYW5kIHNlZSBpZiBhIG5vbi1udWxsXG4gICAgICAvLyB2YWx1ZSBnZXRzIHNldCBmb3IgdGhlIHN0eWxpbmcgYmluZGluZ1xuICAgICAgZm9yIChsZXQgaiA9IDA7IGogPCB2YWx1ZXNDb3VudFVwVG9EZWZhdWx0OyBqKyspIHtcbiAgICAgICAgY29uc3QgYmluZGluZ0luZGV4ID0gZ2V0QmluZGluZ1ZhbHVlKGNvbnRleHQsIGksIGopIGFzIG51bWJlcjtcbiAgICAgICAgY29uc3QgdmFsdWUgPSBiaW5kaW5nRGF0YVtiaW5kaW5nSW5kZXhdO1xuICAgICAgICBpZiAoaXNTdHlsaW5nVmFsdWVEZWZpbmVkKHZhbHVlKSkge1xuICAgICAgICAgIGNvbnN0IGZpbmFsVmFsdWUgPSBzYW5pdGl6ZXIgJiYgaXNTYW5pdGl6YXRpb25SZXF1aXJlZChjb250ZXh0LCBpKSA/XG4gICAgICAgICAgICAgIHNhbml0aXplcihwcm9wLCB2YWx1ZSwgU3R5bGVTYW5pdGl6ZU1vZGUuU2FuaXRpemVPbmx5KSA6XG4gICAgICAgICAgICAgIHZhbHVlO1xuICAgICAgICAgIGFwcGx5U3R5bGluZ0ZuKHJlbmRlcmVyLCBlbGVtZW50LCBwcm9wLCBmaW5hbFZhbHVlLCBiaW5kaW5nSW5kZXgpO1xuICAgICAgICAgIHZhbHVlQXBwbGllZCA9IHRydWU7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgLy8gY2FzZSAyOiBhcHBseSBtYXAtYmFzZWQgdmFsdWVzXG4gICAgICAvLyB0cmF2ZXJzZSB0aHJvdWdoIGVhY2ggbWFwLWJhc2VkIHN0eWxpbmcgYmluZGluZyBhbmQgdXBkYXRlIGFsbCB2YWx1ZXMgdXAgdG9cbiAgICAgIC8vIHRoZSBwcm92aWRlZCBgcHJvcGAgdmFsdWUuIElmIHRoZSBwcm9wZXJ0eSB3YXMgbm90IGFwcGxpZWQgaW4gdGhlIGxvb3AgYWJvdmVcbiAgICAgIC8vIHRoZW4gaXQgd2lsbCBiZSBhdHRlbXB0ZWQgdG8gYmUgYXBwbGllZCBpbiB0aGUgbWFwcyBzeW5jIGNvZGUgYmVsb3cuXG4gICAgICBpZiAoc3R5bGluZ01hcHNTeW5jRm4pIHtcbiAgICAgICAgLy8gZGV0ZXJtaW5lIHdoZXRoZXIgb3Igbm90IHRvIGFwcGx5IHRoZSB0YXJnZXQgcHJvcGVydHkgb3IgdG8gc2tpcCBpdFxuICAgICAgICBjb25zdCBtb2RlID0gbWFwc01vZGUgfCAodmFsdWVBcHBsaWVkID8gU3R5bGluZ01hcHNTeW5jTW9kZS5Ta2lwVGFyZ2V0UHJvcCA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBTdHlsaW5nTWFwc1N5bmNNb2RlLkFwcGx5VGFyZ2V0UHJvcCk7XG4gICAgICAgIGNvbnN0IHZhbHVlQXBwbGllZFdpdGhpbk1hcCA9IHN0eWxpbmdNYXBzU3luY0ZuKFxuICAgICAgICAgICAgY29udGV4dCwgcmVuZGVyZXIsIGVsZW1lbnQsIGJpbmRpbmdEYXRhLCBhcHBseVN0eWxpbmdGbiwgc2FuaXRpemVyLCBtb2RlLCBwcm9wLFxuICAgICAgICAgICAgZGVmYXVsdFZhbHVlKTtcbiAgICAgICAgdmFsdWVBcHBsaWVkID0gdmFsdWVBcHBsaWVkIHx8IHZhbHVlQXBwbGllZFdpdGhpbk1hcDtcbiAgICAgIH1cblxuICAgICAgLy8gY2FzZSAzOiBhcHBseSB0aGUgZGVmYXVsdCB2YWx1ZVxuICAgICAgLy8gaWYgdGhlIHZhbHVlIGhhcyBub3QgeWV0IGJlZW4gYXBwbGllZCB0aGVuIGEgdHJ1dGh5IHZhbHVlIGRvZXMgbm90IGV4aXN0IGluIHRoZVxuICAgICAgLy8gcHJvcC1iYXNlZCBvciBtYXAtYmFzZWQgYmluZGluZ3MgY29kZS4gSWYgYW5kIHdoZW4gdGhpcyBoYXBwZW5zLCBqdXN0IGFwcGx5IHRoZVxuICAgICAgLy8gZGVmYXVsdCB2YWx1ZSAoZXZlbiBpZiB0aGUgZGVmYXVsdCB2YWx1ZSBpcyBgbnVsbGApLlxuICAgICAgaWYgKCF2YWx1ZUFwcGxpZWQpIHtcbiAgICAgICAgYXBwbHlTdHlsaW5nRm4ocmVuZGVyZXIsIGVsZW1lbnQsIHByb3AsIGRlZmF1bHRWYWx1ZSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaSArPSBUU3R5bGluZ0NvbnRleHRJbmRleC5CaW5kaW5nc1N0YXJ0T2Zmc2V0ICsgdmFsdWVzQ291bnQ7XG4gIH1cblxuICAvLyB0aGUgbWFwLWJhc2VkIHN0eWxpbmcgZW50cmllcyBtYXkgaGF2ZSBub3QgYXBwbGllZCBhbGwgdGhlaXJcbiAgLy8gdmFsdWVzLiBGb3IgdGhpcyByZWFzb24sIG9uZSBtb3JlIGNhbGwgdG8gdGhlIHN5bmMgZnVuY3Rpb25cbiAgLy8gbmVlZHMgdG8gYmUgaXNzdWVkIGF0IHRoZSBlbmQuXG4gIGlmIChzdHlsaW5nTWFwc1N5bmNGbikge1xuICAgIHN0eWxpbmdNYXBzU3luY0ZuKGNvbnRleHQsIHJlbmRlcmVyLCBlbGVtZW50LCBiaW5kaW5nRGF0YSwgYXBwbHlTdHlsaW5nRm4sIHNhbml0aXplciwgbWFwc01vZGUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIG5vcm1hbGl6ZUJpdE1hc2tWYWx1ZSh2YWx1ZTogbnVtYmVyIHwgYm9vbGVhbik6IG51bWJlciB7XG4gIC8vIGlmIHBhc3MgPT4gYXBwbHkgYWxsIHZhbHVlcyAoLTEgaW1wbGllcyB0aGF0IGFsbCBiaXRzIGFyZSBmbGlwcGVkIHRvIHRydWUpXG4gIGlmICh2YWx1ZSA9PT0gdHJ1ZSkgcmV0dXJuIC0xO1xuXG4gIC8vIGlmIHBhc3MgPT4gc2tpcCBhbGwgdmFsdWVzXG4gIGlmICh2YWx1ZSA9PT0gZmFsc2UpIHJldHVybiAwO1xuXG4gIC8vIHJldHVybiB0aGUgYml0IG1hc2sgdmFsdWUgYXMgaXNcbiAgcmV0dXJuIHZhbHVlO1xufVxuXG5sZXQgX2FjdGl2ZVN0eWxpbmdNYXBBcHBseUZuOiBTeW5jU3R5bGluZ01hcHNGbnxudWxsID0gbnVsbDtcbmV4cG9ydCBmdW5jdGlvbiBnZXRTdHlsaW5nTWFwc1N5bmNGbigpIHtcbiAgcmV0dXJuIF9hY3RpdmVTdHlsaW5nTWFwQXBwbHlGbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNldFN0eWxpbmdNYXBzU3luY0ZuKGZuOiBTeW5jU3R5bGluZ01hcHNGbikge1xuICBfYWN0aXZlU3R5bGluZ01hcEFwcGx5Rm4gPSBmbjtcbn1cblxuLyoqXG4gKiBBc3NpZ25zIGEgc3R5bGUgdmFsdWUgdG8gYSBzdHlsZSBwcm9wZXJ0eSBmb3IgdGhlIGdpdmVuIGVsZW1lbnQuXG4gKi9cbmNvbnN0IHNldFN0eWxlOiBBcHBseVN0eWxpbmdGbiA9XG4gICAgKHJlbmRlcmVyOiBSZW5kZXJlcjMgfCBudWxsLCBuYXRpdmU6IFJFbGVtZW50LCBwcm9wOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcgfCBudWxsKSA9PiB7XG4gICAgICAvLyB0aGUgcmVhc29uIHdoeSB0aGlzIG1heSBiZSBgbnVsbGAgaXMgZWl0aGVyIGJlY2F1c2VcbiAgICAgIC8vIGl0J3MgYSBjb250YWluZXIgZWxlbWVudCBvciBpdCdzIGEgcGFydCBvZiBhIHRlc3RcbiAgICAgIC8vIGVudmlyb25tZW50IHRoYXQgZG9lc24ndCBoYXZlIHN0eWxpbmcuIEluIGVpdGhlclxuICAgICAgLy8gY2FzZSBpdCdzIHNhZmUgbm90IHRvIGFwcGx5IHN0eWxpbmcgdG8gdGhlIGVsZW1lbnQuXG4gICAgICBjb25zdCBuYXRpdmVTdHlsZSA9IG5hdGl2ZS5zdHlsZTtcbiAgICAgIGlmICh2YWx1ZSkge1xuICAgICAgICAvLyBvcGFjaXR5LCB6LWluZGV4IGFuZCBmbGV4Ym94IGFsbCBoYXZlIG51bWJlciB2YWx1ZXNcbiAgICAgICAgLy8gYW5kIHRoZXNlIG5lZWQgdG8gYmUgY29udmVydGVkIGludG8gc3RyaW5ncyBzbyB0aGF0XG4gICAgICAgIC8vIHRoZXkgY2FuIGJlIGFzc2lnbmVkIHByb3Blcmx5LlxuICAgICAgICB2YWx1ZSA9IHZhbHVlLnRvU3RyaW5nKCk7XG4gICAgICAgIG5nRGV2TW9kZSAmJiBuZ0Rldk1vZGUucmVuZGVyZXJTZXRTdHlsZSsrO1xuICAgICAgICByZW5kZXJlciAmJiBpc1Byb2NlZHVyYWxSZW5kZXJlcihyZW5kZXJlcikgP1xuICAgICAgICAgICAgcmVuZGVyZXIuc2V0U3R5bGUobmF0aXZlLCBwcm9wLCB2YWx1ZSwgUmVuZGVyZXJTdHlsZUZsYWdzMy5EYXNoQ2FzZSkgOlxuICAgICAgICAgICAgKG5hdGl2ZVN0eWxlICYmIG5hdGl2ZVN0eWxlLnNldFByb3BlcnR5KHByb3AsIHZhbHVlKSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLnJlbmRlcmVyUmVtb3ZlU3R5bGUrKztcbiAgICAgICAgcmVuZGVyZXIgJiYgaXNQcm9jZWR1cmFsUmVuZGVyZXIocmVuZGVyZXIpID9cbiAgICAgICAgICAgIHJlbmRlcmVyLnJlbW92ZVN0eWxlKG5hdGl2ZSwgcHJvcCwgUmVuZGVyZXJTdHlsZUZsYWdzMy5EYXNoQ2FzZSkgOlxuICAgICAgICAgICAgKG5hdGl2ZVN0eWxlICYmIG5hdGl2ZVN0eWxlLnJlbW92ZVByb3BlcnR5KHByb3ApKTtcbiAgICAgIH1cbiAgICB9O1xuXG4vKipcbiAqIEFkZHMvcmVtb3ZlcyB0aGUgcHJvdmlkZWQgY2xhc3NOYW1lIHZhbHVlIHRvIHRoZSBwcm92aWRlZCBlbGVtZW50LlxuICovXG5jb25zdCBzZXRDbGFzczogQXBwbHlTdHlsaW5nRm4gPVxuICAgIChyZW5kZXJlcjogUmVuZGVyZXIzIHwgbnVsbCwgbmF0aXZlOiBSRWxlbWVudCwgY2xhc3NOYW1lOiBzdHJpbmcsIHZhbHVlOiBhbnkpID0+IHtcbiAgICAgIGlmIChjbGFzc05hbWUgIT09ICcnKSB7XG4gICAgICAgIC8vIHRoZSByZWFzb24gd2h5IHRoaXMgbWF5IGJlIGBudWxsYCBpcyBlaXRoZXIgYmVjYXVzZVxuICAgICAgICAvLyBpdCdzIGEgY29udGFpbmVyIGVsZW1lbnQgb3IgaXQncyBhIHBhcnQgb2YgYSB0ZXN0XG4gICAgICAgIC8vIGVudmlyb25tZW50IHRoYXQgZG9lc24ndCBoYXZlIHN0eWxpbmcuIEluIGVpdGhlclxuICAgICAgICAvLyBjYXNlIGl0J3Mgc2FmZSBub3QgdG8gYXBwbHkgc3R5bGluZyB0byB0aGUgZWxlbWVudC5cbiAgICAgICAgY29uc3QgY2xhc3NMaXN0ID0gbmF0aXZlLmNsYXNzTGlzdDtcbiAgICAgICAgaWYgKHZhbHVlKSB7XG4gICAgICAgICAgbmdEZXZNb2RlICYmIG5nRGV2TW9kZS5yZW5kZXJlckFkZENsYXNzKys7XG4gICAgICAgICAgcmVuZGVyZXIgJiYgaXNQcm9jZWR1cmFsUmVuZGVyZXIocmVuZGVyZXIpID8gcmVuZGVyZXIuYWRkQ2xhc3MobmF0aXZlLCBjbGFzc05hbWUpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAoY2xhc3NMaXN0ICYmIGNsYXNzTGlzdC5hZGQoY2xhc3NOYW1lKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbmdEZXZNb2RlICYmIG5nRGV2TW9kZS5yZW5kZXJlclJlbW92ZUNsYXNzKys7XG4gICAgICAgICAgcmVuZGVyZXIgJiYgaXNQcm9jZWR1cmFsUmVuZGVyZXIocmVuZGVyZXIpID8gcmVuZGVyZXIucmVtb3ZlQ2xhc3MobmF0aXZlLCBjbGFzc05hbWUpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAoY2xhc3NMaXN0ICYmIGNsYXNzTGlzdC5yZW1vdmUoY2xhc3NOYW1lKSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9O1xuXG4vKipcbiAqIEl0ZXJhdGVzIG92ZXIgYWxsIHByb3ZpZGVkIHN0eWxpbmcgZW50cmllcyBhbmQgcmVuZGVycyB0aGVtIG9uIHRoZSBlbGVtZW50LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgdXNlZCBhbG9uZ3NpZGUgYSBgU3R5bGluZ01hcEFycmF5YCBlbnRyeS4gVGhpcyBlbnRyeSBpcyBub3RcbiAqIHRoZSBzYW1lIGFzIHRoZSBgVFN0eWxpbmdDb250ZXh0YCBhbmQgaXMgb25seSByZWFsbHkgdXNlZCB3aGVuIGFuIGVsZW1lbnQgY29udGFpbnNcbiAqIGluaXRpYWwgc3R5bGluZyB2YWx1ZXMgKGUuZy4gYDxkaXYgc3R5bGU9XCJ3aWR0aDoyMDBweFwiPmApLCBidXQgbm8gc3R5bGUvY2xhc3MgYmluZGluZ3NcbiAqIGFyZSBwcmVzZW50LiBJZiBhbmQgd2hlbiB0aGF0IGhhcHBlbnMgdGhlbiB0aGlzIGZ1bmN0aW9uIHdpbGwgYmUgY2FsbGVkIHRvIHJlbmRlciBhbGxcbiAqIGluaXRpYWwgc3R5bGluZyB2YWx1ZXMgb24gYW4gZWxlbWVudC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHJlbmRlclN0eWxpbmdNYXAoXG4gICAgcmVuZGVyZXI6IFJlbmRlcmVyMywgZWxlbWVudDogUkVsZW1lbnQsIHN0eWxpbmdWYWx1ZXM6IFRTdHlsaW5nQ29udGV4dCB8IFN0eWxpbmdNYXBBcnJheSB8IG51bGwsXG4gICAgaXNDbGFzc0Jhc2VkOiBib29sZWFuKTogdm9pZCB7XG4gIGNvbnN0IHN0eWxpbmdNYXBBcnIgPSBnZXRTdHlsaW5nTWFwQXJyYXkoc3R5bGluZ1ZhbHVlcyk7XG4gIGlmIChzdHlsaW5nTWFwQXJyKSB7XG4gICAgZm9yIChsZXQgaSA9IFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb247IGkgPCBzdHlsaW5nTWFwQXJyLmxlbmd0aDtcbiAgICAgICAgIGkgKz0gU3R5bGluZ01hcEFycmF5SW5kZXguVHVwbGVTaXplKSB7XG4gICAgICBjb25zdCBwcm9wID0gZ2V0TWFwUHJvcChzdHlsaW5nTWFwQXJyLCBpKTtcbiAgICAgIGNvbnN0IHZhbHVlID0gZ2V0TWFwVmFsdWUoc3R5bGluZ01hcEFyciwgaSk7XG4gICAgICBpZiAoaXNDbGFzc0Jhc2VkKSB7XG4gICAgICAgIHNldENsYXNzKHJlbmRlcmVyLCBlbGVtZW50LCBwcm9wLCB2YWx1ZSwgbnVsbCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBzZXRTdHlsZShyZW5kZXJlciwgZWxlbWVudCwgcHJvcCwgdmFsdWUsIG51bGwpO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIFJlZ2lzdGVycyBhbGwgaW5pdGlhbCBzdHlsaW5nIGVudHJpZXMgaW50byB0aGUgcHJvdmlkZWQgY29udGV4dC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIHdpbGwgaXRlcmF0ZSBvdmVyIGFsbCBlbnRyaWVzIGluIHRoZSBwcm92aWRlZCBgaW5pdGlhbFN0eWxpbmdgIGFyfXJheSBhbmQgcmVnaXN0ZXJcbiAqIHRoZW0gYXMgZGVmYXVsdCAoaW5pdGlhbCkgdmFsdWVzIGluIHRoZSBwcm92aWRlZCBjb250ZXh0LiBJbml0aWFsIHN0eWxpbmcgdmFsdWVzIGluIGEgY29udGV4dCBhcmVcbiAqIHRoZSBkZWZhdWx0IHZhbHVlcyB0aGF0IGFyZSB0byBiZSBhcHBsaWVkIHVubGVzcyBvdmVyd3JpdHRlbiBieSBhIGJpbmRpbmcuXG4gKlxuICogVGhlIHJlYXNvbiB3aHkgdGhpcyBmdW5jdGlvbiBleGlzdHMgYW5kIGlzbid0IGEgcGFydCBvZiB0aGUgY29udGV4dCBjb25zdHJ1Y3Rpb24gaXMgYmVjYXVzZVxuICogaG9zdCBiaW5kaW5nIGlzIGV2YWx1YXRlZCBhdCBhIGxhdGVyIHN0YWdlIGFmdGVyIHRoZSBlbGVtZW50IGlzIGNyZWF0ZWQuIFRoaXMgbWVhbnMgdGhhdFxuICogaWYgYSBkaXJlY3RpdmUgb3IgY29tcG9uZW50IGNvbnRhaW5zIGFueSBpbml0aWFsIHN0eWxpbmcgY29kZSAoaS5lLiBgPGRpdiBjbGFzcz1cImZvb1wiPmApXG4gKiB0aGVuIHRoYXQgaW5pdGlhbCBzdHlsaW5nIGRhdGEgY2FuIG9ubHkgYmUgYXBwbGllZCBvbmNlIHRoZSBzdHlsaW5nIGZvciB0aGF0IGVsZW1lbnRcbiAqIGlzIGZpcnN0IGFwcGxpZWQgKGF0IHRoZSBlbmQgb2YgdGhlIHVwZGF0ZSBwaGFzZSkuIE9uY2UgdGhhdCBoYXBwZW5zIHRoZW4gdGhlIGNvbnRleHQgd2lsbFxuICogdXBkYXRlIGl0c2VsZiB3aXRoIHRoZSBjb21wbGV0ZSBpbml0aWFsIHN0eWxpbmcgZm9yIHRoZSBlbGVtZW50LlxuICovXG5mdW5jdGlvbiB1cGRhdGVJbml0aWFsU3R5bGluZ09uQ29udGV4dChcbiAgICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIGluaXRpYWxTdHlsaW5nOiBTdHlsaW5nTWFwQXJyYXkpOiB2b2lkIHtcbiAgLy8gYC0xYCBpcyB1c2VkIGhlcmUgYmVjYXVzZSBhbGwgaW5pdGlhbCBzdHlsaW5nIGRhdGEgaXMgbm90IGEgc3BhcnRcbiAgLy8gb2YgYSBiaW5kaW5nIChzaW5jZSBpdCdzIHN0YXRpYylcbiAgY29uc3QgSU5JVElBTF9TVFlMSU5HX0NPVU5UX0lEID0gLTE7XG5cbiAgZm9yIChsZXQgaSA9IFN0eWxpbmdNYXBBcnJheUluZGV4LlZhbHVlc1N0YXJ0UG9zaXRpb247IGkgPCBpbml0aWFsU3R5bGluZy5sZW5ndGg7XG4gICAgICAgaSArPSBTdHlsaW5nTWFwQXJyYXlJbmRleC5UdXBsZVNpemUpIHtcbiAgICBjb25zdCB2YWx1ZSA9IGdldE1hcFZhbHVlKGluaXRpYWxTdHlsaW5nLCBpKTtcbiAgICBpZiAodmFsdWUpIHtcbiAgICAgIGNvbnN0IHByb3AgPSBnZXRNYXBQcm9wKGluaXRpYWxTdHlsaW5nLCBpKTtcbiAgICAgIHJlZ2lzdGVyQmluZGluZyhjb250ZXh0LCBJTklUSUFMX1NUWUxJTkdfQ09VTlRfSUQsIHByb3AsIHZhbHVlLCBmYWxzZSk7XG4gICAgfVxuICB9XG59XG4iXX0=