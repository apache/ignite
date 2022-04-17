/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ɵstringify as stringify } from '@angular/core';
/** @type {?} */
let _nextReferenceId = 0;
export class MetadataOverrider {
    constructor() {
        this._references = new Map();
    }
    /**
     * Creates a new instance for the given metadata class
     * based on an old instance and overrides.
     * @template C, T
     * @param {?} metadataClass
     * @param {?} oldMetadata
     * @param {?} override
     * @return {?}
     */
    overrideMetadata(metadataClass, oldMetadata, override) {
        /** @type {?} */
        const props = {};
        if (oldMetadata) {
            _valueProps(oldMetadata).forEach((/**
             * @param {?} prop
             * @return {?}
             */
            (prop) => props[prop] = ((/** @type {?} */ (oldMetadata)))[prop]));
        }
        if (override.set) {
            if (override.remove || override.add) {
                throw new Error(`Cannot set and add/remove ${stringify(metadataClass)} at the same time!`);
            }
            setMetadata(props, override.set);
        }
        if (override.remove) {
            removeMetadata(props, override.remove, this._references);
        }
        if (override.add) {
            addMetadata(props, override.add);
        }
        return new metadataClass((/** @type {?} */ (props)));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MetadataOverrider.prototype._references;
}
/**
 * @param {?} metadata
 * @param {?} remove
 * @param {?} references
 * @return {?}
 */
function removeMetadata(metadata, remove, references) {
    /** @type {?} */
    const removeObjects = new Set();
    for (const prop in remove) {
        /** @type {?} */
        const removeValue = remove[prop];
        if (removeValue instanceof Array) {
            removeValue.forEach((/**
             * @param {?} value
             * @return {?}
             */
            (value) => { removeObjects.add(_propHashKey(prop, value, references)); }));
        }
        else {
            removeObjects.add(_propHashKey(prop, removeValue, references));
        }
    }
    for (const prop in metadata) {
        /** @type {?} */
        const propValue = metadata[prop];
        if (propValue instanceof Array) {
            metadata[prop] = propValue.filter((/**
             * @param {?} value
             * @return {?}
             */
            (value) => !removeObjects.has(_propHashKey(prop, value, references))));
        }
        else {
            if (removeObjects.has(_propHashKey(prop, propValue, references))) {
                metadata[prop] = undefined;
            }
        }
    }
}
/**
 * @param {?} metadata
 * @param {?} add
 * @return {?}
 */
function addMetadata(metadata, add) {
    for (const prop in add) {
        /** @type {?} */
        const addValue = add[prop];
        /** @type {?} */
        const propValue = metadata[prop];
        if (propValue != null && propValue instanceof Array) {
            metadata[prop] = propValue.concat(addValue);
        }
        else {
            metadata[prop] = addValue;
        }
    }
}
/**
 * @param {?} metadata
 * @param {?} set
 * @return {?}
 */
function setMetadata(metadata, set) {
    for (const prop in set) {
        metadata[prop] = set[prop];
    }
}
/**
 * @param {?} propName
 * @param {?} propValue
 * @param {?} references
 * @return {?}
 */
function _propHashKey(propName, propValue, references) {
    /** @type {?} */
    const replacer = (/**
     * @param {?} key
     * @param {?} value
     * @return {?}
     */
    (key, value) => {
        if (typeof value === 'function') {
            value = _serializeReference(value, references);
        }
        return value;
    });
    return `${propName}:${JSON.stringify(propValue, replacer)}`;
}
/**
 * @param {?} ref
 * @param {?} references
 * @return {?}
 */
function _serializeReference(ref, references) {
    /** @type {?} */
    let id = references.get(ref);
    if (!id) {
        id = `${stringify(ref)}${_nextReferenceId++}`;
        references.set(ref, id);
    }
    return id;
}
/**
 * @param {?} obj
 * @return {?}
 */
function _valueProps(obj) {
    /** @type {?} */
    const props = [];
    // regular public props
    Object.keys(obj).forEach((/**
     * @param {?} prop
     * @return {?}
     */
    (prop) => {
        if (!prop.startsWith('_')) {
            props.push(prop);
        }
    }));
    // getters
    /** @type {?} */
    let proto = obj;
    while (proto = Object.getPrototypeOf(proto)) {
        Object.keys(proto).forEach((/**
         * @param {?} protoProp
         * @return {?}
         */
        (protoProp) => {
            /** @type {?} */
            const desc = Object.getOwnPropertyDescriptor(proto, protoProp);
            if (!protoProp.startsWith('_') && desc && 'get' in desc) {
                props.push(protoProp);
            }
        }));
    }
    return props;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWV0YWRhdGFfb3ZlcnJpZGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS90ZXN0aW5nL3NyYy9tZXRhZGF0YV9vdmVycmlkZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsVUFBVSxJQUFJLFNBQVMsRUFBQyxNQUFNLGVBQWUsQ0FBQzs7SUFPbEQsZ0JBQWdCLEdBQUcsQ0FBQztBQUV4QixNQUFNLE9BQU8saUJBQWlCO0lBQTlCO1FBQ1UsZ0JBQVcsR0FBRyxJQUFJLEdBQUcsRUFBZSxDQUFDO0lBMEIvQyxDQUFDOzs7Ozs7Ozs7O0lBckJDLGdCQUFnQixDQUNaLGFBQXFDLEVBQUUsV0FBYyxFQUFFLFFBQTZCOztjQUNoRixLQUFLLEdBQWMsRUFBRTtRQUMzQixJQUFJLFdBQVcsRUFBRTtZQUNmLFdBQVcsQ0FBQyxXQUFXLENBQUMsQ0FBQyxPQUFPOzs7O1lBQUMsQ0FBQyxJQUFJLEVBQUUsRUFBRSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLG1CQUFLLFdBQVcsRUFBQSxDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUMsQ0FBQztTQUNwRjtRQUVELElBQUksUUFBUSxDQUFDLEdBQUcsRUFBRTtZQUNoQixJQUFJLFFBQVEsQ0FBQyxNQUFNLElBQUksUUFBUSxDQUFDLEdBQUcsRUFBRTtnQkFDbkMsTUFBTSxJQUFJLEtBQUssQ0FBQyw2QkFBNkIsU0FBUyxDQUFDLGFBQWEsQ0FBQyxvQkFBb0IsQ0FBQyxDQUFDO2FBQzVGO1lBQ0QsV0FBVyxDQUFDLEtBQUssRUFBRSxRQUFRLENBQUMsR0FBRyxDQUFDLENBQUM7U0FDbEM7UUFDRCxJQUFJLFFBQVEsQ0FBQyxNQUFNLEVBQUU7WUFDbkIsY0FBYyxDQUFDLEtBQUssRUFBRSxRQUFRLENBQUMsTUFBTSxFQUFFLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQztTQUMxRDtRQUNE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SCxDQUFDLEVBQUMsQ0FBQztLQUNKO0lBQ0QsT0FBTyxLQUFLLENBQUM7QUFDZixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge8m1c3RyaW5naWZ5IGFzIHN0cmluZ2lmeX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge01ldGFkYXRhT3ZlcnJpZGV9IGZyb20gJy4vbWV0YWRhdGFfb3ZlcnJpZGUnO1xuXG50eXBlIFN0cmluZ01hcCA9IHtcbiAgW2tleTogc3RyaW5nXTogYW55XG59O1xuXG5sZXQgX25leHRSZWZlcmVuY2VJZCA9IDA7XG5cbmV4cG9ydCBjbGFzcyBNZXRhZGF0YU92ZXJyaWRlciB7XG4gIHByaXZhdGUgX3JlZmVyZW5jZXMgPSBuZXcgTWFwPGFueSwgc3RyaW5nPigpO1xuICAvKipcbiAgICogQ3JlYXRlcyBhIG5ldyBpbnN0YW5jZSBmb3IgdGhlIGdpdmVuIG1ldGFkYXRhIGNsYXNzXG4gICAqIGJhc2VkIG9uIGFuIG9sZCBpbnN0YW5jZSBhbmQgb3ZlcnJpZGVzLlxuICAgKi9cbiAgb3ZlcnJpZGVNZXRhZGF0YTxDIGV4dGVuZHMgVCwgVD4oXG4gICAgICBtZXRhZGF0YUNsYXNzOiB7bmV3IChvcHRpb25zOiBUKTogQzt9LCBvbGRNZXRhZGF0YTogQywgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8VD4pOiBDIHtcbiAgICBjb25zdCBwcm9wczogU3RyaW5nTWFwID0ge307XG4gICAgaWYgKG9sZE1ldGFkYXRhKSB7XG4gICAgICBfdmFsdWVQcm9wcyhvbGRNZXRhZGF0YSkuZm9yRWFjaCgocHJvcCkgPT4gcHJvcHNbcHJvcF0gPSAoPGFueT5vbGRNZXRhZGF0YSlbcHJvcF0pO1xuICAgIH1cblxuICAgIGlmIChvdmVycmlkZS5zZXQpIHtcbiAgICAgIGlmIChvdmVycmlkZS5yZW1vdmUgfHwgb3ZlcnJpZGUuYWRkKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgQ2Fubm90IHNldCBhbmQgYWRkL3JlbW92ZSAke3N0cmluZ2lmeShtZXRhZGF0YUNsYXNzKX0gYXQgdGhlIHNhbWUgdGltZSFgKTtcbiAgICAgIH1cbiAgICAgIHNldE1ldGFkYXRhKHByb3BzLCBvdmVycmlkZS5zZXQpO1xuICAgIH1cbiAgICBpZiAob3ZlcnJpZGUucmVtb3ZlKSB7XG4gICAgICByZW1vdmVNZXRhZGF0YShwcm9wcywgb3ZlcnJpZGUucmVtb3ZlLCB0aGlzLl9yZWZlcmVuY2VzKTtcbiAgICB9XG4gICAgaWYgKG92ZXJyaWRlLmFkZCkge1xuICAgICAgYWRkTWV0YWRhdGEocHJvcHMsIG92ZXJyaWRlLmFkZCk7XG4gICAgfVxuICAgIHJldHVybiBuZXcgbWV0YWRhdGFDbGFzcyg8YW55PnByb3BzKTtcbiAgfVxufVxuXG5mdW5jdGlvbiByZW1vdmVNZXRhZGF0YShtZXRhZGF0YTogU3RyaW5nTWFwLCByZW1vdmU6IGFueSwgcmVmZXJlbmNlczogTWFwPGFueSwgc3RyaW5nPikge1xuICBjb25zdCByZW1vdmVPYmplY3RzID0gbmV3IFNldDxzdHJpbmc+KCk7XG4gIGZvciAoY29uc3QgcHJvcCBpbiByZW1vdmUpIHtcbiAgICBjb25zdCByZW1vdmVWYWx1ZSA9IHJlbW92ZVtwcm9wXTtcbiAgICBpZiAocmVtb3ZlVmFsdWUgaW5zdGFuY2VvZiBBcnJheSkge1xuICAgICAgcmVtb3ZlVmFsdWUuZm9yRWFjaChcbiAgICAgICAgICAodmFsdWU6IGFueSkgPT4geyByZW1vdmVPYmplY3RzLmFkZChfcHJvcEhhc2hLZXkocHJvcCwgdmFsdWUsIHJlZmVyZW5jZXMpKTsgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJlbW92ZU9iamVjdHMuYWRkKF9wcm9wSGFzaEtleShwcm9wLCByZW1vdmVWYWx1ZSwgcmVmZXJlbmNlcykpO1xuICAgIH1cbiAgfVxuXG4gIGZvciAoY29uc3QgcHJvcCBpbiBtZXRhZGF0YSkge1xuICAgIGNvbnN0IHByb3BWYWx1ZSA9IG1ldGFkYXRhW3Byb3BdO1xuICAgIGlmIChwcm9wVmFsdWUgaW5zdGFuY2VvZiBBcnJheSkge1xuICAgICAgbWV0YWRhdGFbcHJvcF0gPSBwcm9wVmFsdWUuZmlsdGVyKFxuICAgICAgICAgICh2YWx1ZTogYW55KSA9PiAhcmVtb3ZlT2JqZWN0cy5oYXMoX3Byb3BIYXNoS2V5KHByb3AsIHZhbHVlLCByZWZlcmVuY2VzKSkpO1xuICAgIH0gZWxzZSB7XG4gICAgICBpZiAocmVtb3ZlT2JqZWN0cy5oYXMoX3Byb3BIYXNoS2V5KHByb3AsIHByb3BWYWx1ZSwgcmVmZXJlbmNlcykpKSB7XG4gICAgICAgIG1ldGFkYXRhW3Byb3BdID0gdW5kZWZpbmVkO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBhZGRNZXRhZGF0YShtZXRhZGF0YTogU3RyaW5nTWFwLCBhZGQ6IGFueSkge1xuICBmb3IgKGNvbnN0IHByb3AgaW4gYWRkKSB7XG4gICAgY29uc3QgYWRkVmFsdWUgPSBhZGRbcHJvcF07XG4gICAgY29uc3QgcHJvcFZhbHVlID0gbWV0YWRhdGFbcHJvcF07XG4gICAgaWYgKHByb3BWYWx1ZSAhPSBudWxsICYmIHByb3BWYWx1ZSBpbnN0YW5jZW9mIEFycmF5KSB7XG4gICAgICBtZXRhZGF0YVtwcm9wXSA9IHByb3BWYWx1ZS5jb25jYXQoYWRkVmFsdWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBtZXRhZGF0YVtwcm9wXSA9IGFkZFZhbHVlO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBzZXRNZXRhZGF0YShtZXRhZGF0YTogU3RyaW5nTWFwLCBzZXQ6IGFueSkge1xuICBmb3IgKGNvbnN0IHByb3AgaW4gc2V0KSB7XG4gICAgbWV0YWRhdGFbcHJvcF0gPSBzZXRbcHJvcF07XG4gIH1cbn1cblxuZnVuY3Rpb24gX3Byb3BIYXNoS2V5KHByb3BOYW1lOiBhbnksIHByb3BWYWx1ZTogYW55LCByZWZlcmVuY2VzOiBNYXA8YW55LCBzdHJpbmc+KTogc3RyaW5nIHtcbiAgY29uc3QgcmVwbGFjZXIgPSAoa2V5OiBhbnksIHZhbHVlOiBhbnkpID0+IHtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSAnZnVuY3Rpb24nKSB7XG4gICAgICB2YWx1ZSA9IF9zZXJpYWxpemVSZWZlcmVuY2UodmFsdWUsIHJlZmVyZW5jZXMpO1xuICAgIH1cbiAgICByZXR1cm4gdmFsdWU7XG4gIH07XG5cbiAgcmV0dXJuIGAke3Byb3BOYW1lfToke0pTT04uc3RyaW5naWZ5KHByb3BWYWx1ZSwgcmVwbGFjZXIpfWA7XG59XG5cbmZ1bmN0aW9uIF9zZXJpYWxpemVSZWZlcmVuY2UocmVmOiBhbnksIHJlZmVyZW5jZXM6IE1hcDxhbnksIHN0cmluZz4pOiBzdHJpbmcge1xuICBsZXQgaWQgPSByZWZlcmVuY2VzLmdldChyZWYpO1xuICBpZiAoIWlkKSB7XG4gICAgaWQgPSBgJHtzdHJpbmdpZnkocmVmKX0ke19uZXh0UmVmZXJlbmNlSWQrK31gO1xuICAgIHJlZmVyZW5jZXMuc2V0KHJlZiwgaWQpO1xuICB9XG4gIHJldHVybiBpZDtcbn1cblxuXG5mdW5jdGlvbiBfdmFsdWVQcm9wcyhvYmo6IGFueSk6IHN0cmluZ1tdIHtcbiAgY29uc3QgcHJvcHM6IHN0cmluZ1tdID0gW107XG4gIC8vIHJlZ3VsYXIgcHVibGljIHByb3BzXG4gIE9iamVjdC5rZXlzKG9iaikuZm9yRWFjaCgocHJvcCkgPT4ge1xuICAgIGlmICghcHJvcC5zdGFydHNXaXRoKCdfJykpIHtcbiAgICAgIHByb3BzLnB1c2gocHJvcCk7XG4gICAgfVxuICB9KTtcblxuICAvLyBnZXR0ZXJzXG4gIGxldCBwcm90byA9IG9iajtcbiAgd2hpbGUgKHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKHByb3RvKSkge1xuICAgIE9iamVjdC5rZXlzKHByb3RvKS5mb3JFYWNoKChwcm90b1Byb3ApID0+IHtcbiAgICAgIGNvbnN0IGRlc2MgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yKHByb3RvLCBwcm90b1Byb3ApO1xuICAgICAgaWYgKCFwcm90b1Byb3Auc3RhcnRzV2l0aCgnXycpICYmIGRlc2MgJiYgJ2dldCcgaW4gZGVzYykge1xuICAgICAgICBwcm9wcy5wdXNoKHByb3RvUHJvcCk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIHByb3BzO1xufVxuIl19