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
import { ElementRef, Injectable, IterableDiffers, KeyValueDiffers, Renderer2, ɵisListLikeIterable as isListLikeIterable, ɵstringify as stringify } from '@angular/core';
import { StylingDiffer } from './styling_differ';
/**
 * Used as a token for an injected service within the NgClass directive.
 *
 * NgClass behaves differenly whether or not VE is being used or not. If
 * present then the legacy ngClass diffing algorithm will be used as an
 * injected service. Otherwise the new diffing algorithm (which delegates
 * to the `[class]` binding) will be used. This toggle behavior is done so
 * via the ivy_switch mechanism.
 * @abstract
 */
export class NgClassImpl {
}
if (false) {
    /**
     * @abstract
     * @param {?} value
     * @return {?}
     */
    NgClassImpl.prototype.setClass = function (value) { };
    /**
     * @abstract
     * @param {?} value
     * @return {?}
     */
    NgClassImpl.prototype.setNgClass = function (value) { };
    /**
     * @abstract
     * @return {?}
     */
    NgClassImpl.prototype.applyChanges = function () { };
    /**
     * @abstract
     * @return {?}
     */
    NgClassImpl.prototype.getValue = function () { };
}
export class NgClassR2Impl {
    /**
     * @param {?} _iterableDiffers
     * @param {?} _keyValueDiffers
     * @param {?} _ngEl
     * @param {?} _renderer
     */
    constructor(_iterableDiffers, _keyValueDiffers, _ngEl, _renderer) {
        this._iterableDiffers = _iterableDiffers;
        this._keyValueDiffers = _keyValueDiffers;
        this._ngEl = _ngEl;
        this._renderer = _renderer;
        this._initialClasses = [];
    }
    /**
     * @return {?}
     */
    getValue() { return null; }
    /**
     * @param {?} value
     * @return {?}
     */
    setClass(value) {
        this._removeClasses(this._initialClasses);
        this._initialClasses = typeof value === 'string' ? value.split(/\s+/) : [];
        this._applyClasses(this._initialClasses);
        this._applyClasses(this._rawClass);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    setNgClass(value) {
        this._removeClasses(this._rawClass);
        this._applyClasses(this._initialClasses);
        this._iterableDiffer = null;
        this._keyValueDiffer = null;
        this._rawClass = typeof value === 'string' ? value.split(/\s+/) : value;
        if (this._rawClass) {
            if (isListLikeIterable(this._rawClass)) {
                this._iterableDiffer = this._iterableDiffers.find(this._rawClass).create();
            }
            else {
                this._keyValueDiffer = this._keyValueDiffers.find(this._rawClass).create();
            }
        }
    }
    /**
     * @return {?}
     */
    applyChanges() {
        if (this._iterableDiffer) {
            /** @type {?} */
            const iterableChanges = this._iterableDiffer.diff((/** @type {?} */ (this._rawClass)));
            if (iterableChanges) {
                this._applyIterableChanges(iterableChanges);
            }
        }
        else if (this._keyValueDiffer) {
            /** @type {?} */
            const keyValueChanges = this._keyValueDiffer.diff((/** @type {?} */ (this._rawClass)));
            if (keyValueChanges) {
                this._applyKeyValueChanges(keyValueChanges);
            }
        }
    }
    /**
     * @private
     * @param {?} changes
     * @return {?}
     */
    _applyKeyValueChanges(changes) {
        changes.forEachAddedItem((/**
         * @param {?} record
         * @return {?}
         */
        (record) => this._toggleClass(record.key, record.currentValue)));
        changes.forEachChangedItem((/**
         * @param {?} record
         * @return {?}
         */
        (record) => this._toggleClass(record.key, record.currentValue)));
        changes.forEachRemovedItem((/**
         * @param {?} record
         * @return {?}
         */
        (record) => {
            if (record.previousValue) {
                this._toggleClass(record.key, false);
            }
        }));
    }
    /**
     * @private
     * @param {?} changes
     * @return {?}
     */
    _applyIterableChanges(changes) {
        changes.forEachAddedItem((/**
         * @param {?} record
         * @return {?}
         */
        (record) => {
            if (typeof record.item === 'string') {
                this._toggleClass(record.item, true);
            }
            else {
                throw new Error(`NgClass can only toggle CSS classes expressed as strings, got ${stringify(record.item)}`);
            }
        }));
        changes.forEachRemovedItem((/**
         * @param {?} record
         * @return {?}
         */
        (record) => this._toggleClass(record.item, false)));
    }
    /**
     * Applies a collection of CSS classes to the DOM element.
     *
     * For argument of type Set and Array CSS class names contained in those collections are always
     * added.
     * For argument of type Map CSS class name in the map's key is toggled based on the value (added
     * for truthy and removed for falsy).
     * @private
     * @param {?} rawClassVal
     * @return {?}
     */
    _applyClasses(rawClassVal) {
        if (rawClassVal) {
            if (Array.isArray(rawClassVal) || rawClassVal instanceof Set) {
                ((/** @type {?} */ (rawClassVal))).forEach((/**
                 * @param {?} klass
                 * @return {?}
                 */
                (klass) => this._toggleClass(klass, true)));
            }
            else {
                Object.keys(rawClassVal).forEach((/**
                 * @param {?} klass
                 * @return {?}
                 */
                klass => this._toggleClass(klass, !!rawClassVal[klass])));
            }
        }
    }
    /**
     * Removes a collection of CSS classes from the DOM element. This is mostly useful for cleanup
     * purposes.
     * @private
     * @param {?} rawClassVal
     * @return {?}
     */
    _removeClasses(rawClassVal) {
        if (rawClassVal) {
            if (Array.isArray(rawClassVal) || rawClassVal instanceof Set) {
                ((/** @type {?} */ (rawClassVal))).forEach((/**
                 * @param {?} klass
                 * @return {?}
                 */
                (klass) => this._toggleClass(klass, false)));
            }
            else {
                Object.keys(rawClassVal).forEach((/**
                 * @param {?} klass
                 * @return {?}
                 */
                klass => this._toggleClass(klass, false)));
            }
        }
    }
    /**
     * @private
     * @param {?} klass
     * @param {?} enabled
     * @return {?}
     */
    _toggleClass(klass, enabled) {
        klass = klass.trim();
        if (klass) {
            klass.split(/\s+/g).forEach((/**
             * @param {?} klass
             * @return {?}
             */
            klass => {
                if (enabled) {
                    this._renderer.addClass(this._ngEl.nativeElement, klass);
                }
                else {
                    this._renderer.removeClass(this._ngEl.nativeElement, klass);
                }
            }));
        }
    }
}
NgClassR2Impl.decorators = [
    { type: Injectable }
];
/** @nocollapse */
NgClassR2Impl.ctorParameters = () => [
    { type: IterableDiffers },
    { type: KeyValueDiffers },
    { type: ElementRef },
    { type: Renderer2 }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._iterableDiffer;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._keyValueDiffer;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._initialClasses;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._rawClass;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._iterableDiffers;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._keyValueDiffers;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._ngEl;
    /**
     * @type {?}
     * @private
     */
    NgClassR2Impl.prototype._renderer;
}
export class NgClassR3Impl {
    constructor() {
        this._value = null;
        this._ngClassDiffer = new StylingDiffer('NgClass', 1 /* TrimProperties */ |
            2 /* AllowSubKeys */ |
            4 /* AllowStringValue */ | 16 /* ForceAsMap */);
        this._classStringDiffer = null;
    }
    /**
     * @return {?}
     */
    getValue() { return this._value; }
    /**
     * @param {?} value
     * @return {?}
     */
    setClass(value) {
        // early exit incase the binding gets emitted as an empty value which
        // means there is no reason to instantiate and diff the values...
        if (!value && !this._classStringDiffer)
            return;
        this._classStringDiffer = this._classStringDiffer ||
            new StylingDiffer('class', 4 /* AllowStringValue */ | 16 /* ForceAsMap */);
        this._classStringDiffer.setValue(value);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    setNgClass(value) {
        this._ngClassDiffer.setValue(value);
    }
    /**
     * @return {?}
     */
    applyChanges() {
        /** @type {?} */
        const classChanged = this._classStringDiffer ? this._classStringDiffer.hasValueChanged() : false;
        /** @type {?} */
        const ngClassChanged = this._ngClassDiffer.hasValueChanged();
        if (classChanged || ngClassChanged) {
            /** @type {?} */
            let value = this._ngClassDiffer.value;
            if (this._classStringDiffer) {
                /** @type {?} */
                let classValue = this._classStringDiffer.value;
                if (classValue) {
                    value = value ? Object.assign({}, classValue, value) : classValue;
                }
            }
            this._value = value;
        }
    }
}
NgClassR3Impl.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    NgClassR3Impl.prototype._value;
    /**
     * @type {?}
     * @private
     */
    NgClassR3Impl.prototype._ngClassDiffer;
    /**
     * @type {?}
     * @private
     */
    NgClassR3Impl.prototype._classStringDiffer;
}
// the implementation for both NgStyleR2Impl and NgStyleR3Impl are
// not ivy_switch'd away, instead they are only hooked up into the
// DI via NgStyle's directive's provider property.
/** @type {?} */
export const NgClassImplProvider__PRE_R3__ = {
    provide: NgClassImpl,
    useClass: NgClassR2Impl
};
/** @type {?} */
export const NgClassImplProvider__POST_R3__ = {
    provide: NgClassImpl,
    useClass: NgClassR3Impl
};
/** @type {?} */
export const NgClassImplProvider = NgClassImplProvider__PRE_R3__;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY2xhc3NfaW1wbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvZGlyZWN0aXZlcy9uZ19jbGFzc19pbXBsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBT0EsT0FBTyxFQUFDLFVBQVUsRUFBRSxVQUFVLEVBQW1DLGVBQWUsRUFBbUMsZUFBZSxFQUFFLFNBQVMsRUFBRSxtQkFBbUIsSUFBSSxrQkFBa0IsRUFBRSxVQUFVLElBQUksU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRXhPLE9BQU8sRUFBQyxhQUFhLEVBQXVCLE1BQU0sa0JBQWtCLENBQUM7Ozs7Ozs7Ozs7O0FBV3JFLE1BQU0sT0FBZ0IsV0FBVztDQUtoQzs7Ozs7OztJQUpDLHNEQUF1Qzs7Ozs7O0lBQ3ZDLHdEQUFxRjs7Ozs7SUFDckYscURBQThCOzs7OztJQUM5QixpREFBK0M7O0FBSWpELE1BQU0sT0FBTyxhQUFhOzs7Ozs7O0lBU3hCLFlBQ1ksZ0JBQWlDLEVBQVUsZ0JBQWlDLEVBQzVFLEtBQWlCLEVBQVUsU0FBb0I7UUFEL0MscUJBQWdCLEdBQWhCLGdCQUFnQixDQUFpQjtRQUFVLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBaUI7UUFDNUUsVUFBSyxHQUFMLEtBQUssQ0FBWTtRQUFVLGNBQVMsR0FBVCxTQUFTLENBQVc7UUFObkQsb0JBQWUsR0FBYSxFQUFFLENBQUM7SUFNdUIsQ0FBQzs7OztJQUUvRCxRQUFRLEtBQUssT0FBTyxJQUFJLENBQUMsQ0FBQyxDQUFDOzs7OztJQUUzQixRQUFRLENBQUMsS0FBYTtRQUNwQixJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsQ0FBQztRQUMxQyxJQUFJLENBQUMsZUFBZSxHQUFHLE9BQU8sS0FBSyxLQUFLLFFBQVEsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO1FBQzNFLElBQUksQ0FBQyxhQUFhLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxDQUFDO1FBQ3pDLElBQUksQ0FBQyxhQUFhLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDO0lBQ3JDLENBQUM7Ozs7O0lBRUQsVUFBVSxDQUFDLEtBQWE7UUFDdEIsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDcEMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsZUFBZSxDQUFDLENBQUM7UUFFekMsSUFBSSxDQUFDLGVBQWUsR0FBRyxJQUFJLENBQUM7UUFDNUIsSUFBSSxDQUFDLGVBQWUsR0FBRyxJQUFJLENBQUM7UUFFNUIsSUFBSSxDQUFDLFNBQVMsR0FBRyxPQUFPLEtBQUssS0FBSyxRQUFRLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQztRQUV4RSxJQUFJLElBQUksQ0FBQyxTQUFTLEVBQUU7WUFDbEIsSUFBSSxrQkFBa0IsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLEVBQUU7Z0JBQ3RDLElBQUksQ0FBQyxlQUFlLEdBQUcsSUFBSSxDQUFDLGdCQUFnQixDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUMsTUFBTSxFQUFFLENBQUM7YUFDNUU7aUJBQU07Z0JBQ0wsSUFBSSxDQUFDLGVBQWUsR0FBRyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQyxNQUFNLEVBQUUsQ0FBQzthQUM1RTtTQUNGO0lBQ0gsQ0FBQzs7OztJQUVELFlBQVk7UUFDVixJQUFJLElBQUksQ0FBQyxlQUFlLEVBQUU7O2tCQUNsQixlQUFlLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsbUJBQUEsSUFBSSxDQUFDLFNBQVMsRUFBWSxDQUFDO1lBQzdFLElBQUksZUFBZSxFQUFFO2dCQUNuQixJQUFJLENBQUMscUJBQXFCLENBQUMsZUFBZSxDQUFDLENBQUM7YUFDN0M7U0FDRjthQUFNLElBQUksSUFBSSxDQUFDLGVBQWUsRUFBRTs7a0JBQ3pCLGVBQWUsR0FBRyxJQUFJLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxtQkFBQSxJQUFJLENBQUMsU0FBUyxFQUFxQixDQUFDO1lBQ3RGLElBQUksZUFBZSxFQUFFO2dCQUNuQixJQUFJLENBQUMscUJBQXFCLENBQUMsZUFBZSxDQUFDLENBQUM7YUFDN0M7U0FDRjtJQUNILENBQUM7Ozs7OztJQUVPLHFCQUFxQixDQUFDLE9BQXFDO1FBQ2pFLE9BQU8sQ0FBQyxnQkFBZ0I7Ozs7UUFBQyxDQUFDLE1BQU0sRUFBRSxFQUFFLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLENBQUMsR0FBRyxFQUFFLE1BQU0sQ0FBQyxZQUFZLENBQUMsRUFBQyxDQUFDO1FBQ3pGLE9BQU8sQ0FBQyxrQkFBa0I7Ozs7UUFBQyxDQUFDLE1BQU0sRUFBRSxFQUFFLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLENBQUMsR0FBRyxFQUFFLE1BQU0sQ0FBQyxZQUFZLENBQUMsRUFBQyxDQUFDO1FBQzNGLE9BQU8sQ0FBQyxrQkFBa0I7Ozs7UUFBQyxDQUFDLE1BQU0sRUFBRSxFQUFFO1lBQ3BDLElBQUksTUFBTSxDQUFDLGFBQWEsRUFBRTtnQkFDeEIsSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBQyxDQUFDO2FBQ3RDO1FBQ0gsQ0FBQyxFQUFDLENBQUM7SUFDTCxDQUFDOzs7Ozs7SUFFTyxxQkFBcUIsQ0FBQyxPQUFnQztRQUM1RCxPQUFPLENBQUMsZ0JBQWdCOzs7O1FBQUMsQ0FBQyxNQUFNLEVBQUUsRUFBRTtZQUNsQyxJQUFJLE9BQU8sTUFBTSxDQUFDLElBQUksS0FBSyxRQUFRLEVBQUU7Z0JBQ25DLElBQUksQ0FBQyxZQUFZLENBQUMsTUFBTSxDQUFDLElBQUksRUFBRSxJQUFJLENBQUMsQ0FBQzthQUN0QztpQkFBTTtnQkFDTCxNQUFNLElBQUksS0FBSyxDQUNYLGlFQUFpRSxTQUFTLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQzthQUNoRztRQUNILENBQUMsRUFBQyxDQUFDO1FBRUgsT0FBTyxDQUFDLGtCQUFrQjs7OztRQUFDLENBQUMsTUFBTSxFQUFFLEVBQUUsQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLE1BQU0sQ0FBQyxJQUFJLEVBQUUsS0FBSyxDQUFDLEVBQUMsQ0FBQztJQUNoRixDQUFDOzs7Ozs7Ozs7Ozs7SUFVTyxhQUFhLENBQUMsV0FBd0Q7UUFDNUUsSUFBSSxXQUFXLEVBQUU7WUFDZixJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsV0FBVyxDQUFDLElBQUksV0FBVyxZQUFZLEdBQUcsRUFBRTtnQkFDNUQsQ0FBQyxtQkFBSyxXQUFXLEVBQUEsQ0FBQyxDQUFDLE9BQU87Ozs7Z0JBQUMsQ0FBQyxLQUFhLEVBQUUsRUFBRSxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsS0FBSyxFQUFFLElBQUksQ0FBQyxFQUFDLENBQUM7YUFDL0U7aUJBQU07Z0JBQ0wsTUFBTSxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQyxPQUFPOzs7O2dCQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLFdBQVcsQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFDLENBQUM7YUFDM0Y7U0FDRjtJQUNILENBQUM7Ozs7Ozs7O0lBTU8sY0FBYyxDQUFDLFdBQXdEO1FBQzdFLElBQUksV0FBVyxFQUFFO1lBQ2YsSUFBSSxLQUFLLENBQUMsT0FBTyxDQUFDLFdBQVcsQ0FBQyxJQUFJLFdBQVcsWUFBWSxHQUFHLEVBQUU7Z0JBQzVELENBQUMsbUJBQUssV0FBVyxFQUFBLENBQUMsQ0FBQyxPQUFPOzs7O2dCQUFDLENBQUMsS0FBYSxFQUFFLEVBQUUsQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsRUFBQyxDQUFDO2FBQ2hGO2lCQUFNO2dCQUNMLE1BQU0sQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLENBQUMsT0FBTzs7OztnQkFBQyxLQUFLLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQyxFQUFDLENBQUM7YUFDNUU7U0FDRjtJQUNILENBQUM7Ozs7Ozs7SUFFTyxZQUFZLENBQUMsS0FBYSxFQUFFLE9BQWdCO1FBQ2xELEtBQUssR0FBRyxLQUFLLENBQUMsSUFBSSxFQUFFLENBQUM7UUFDckIsSUFBSSxLQUFLLEVBQUU7WUFDVCxLQUFLLENBQUMsS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDLE9BQU87Ozs7WUFBQyxLQUFLLENBQUMsRUFBRTtnQkFDbEMsSUFBSSxPQUFPLEVBQUU7b0JBQ1gsSUFBSSxDQUFDLFNBQVMsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxhQUFhLEVBQUUsS0FBSyxDQUFDLENBQUM7aUJBQzFEO3FCQUFNO29CQUNMLElBQUksQ0FBQyxTQUFTLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsYUFBYSxFQUFFLEtBQUssQ0FBQyxDQUFDO2lCQUM3RDtZQUNILENBQUMsRUFBQyxDQUFDO1NBQ0o7SUFDSCxDQUFDOzs7WUF6SEYsVUFBVTs7OztZQXBCc0QsZUFBZTtZQUFtQyxlQUFlO1lBQTFILFVBQVU7WUFBa0gsU0FBUzs7Ozs7OztJQXVCM0ksd0NBQXdEOzs7OztJQUV4RCx3Q0FBNkQ7Ozs7O0lBQzdELHdDQUF1Qzs7Ozs7SUFFdkMsa0NBQW9FOzs7OztJQUdoRSx5Q0FBeUM7Ozs7O0lBQUUseUNBQXlDOzs7OztJQUNwRiw4QkFBeUI7Ozs7O0lBQUUsa0NBQTRCOztBQWlIN0QsTUFBTSxPQUFPLGFBQWE7SUFEMUI7UUFFVSxXQUFNLEdBQWtDLElBQUksQ0FBQztRQUM3QyxtQkFBYyxHQUFHLElBQUksYUFBYSxDQUN0QyxTQUFTLEVBQUU7Z0NBQ2lDO29DQUNJLHNCQUFnQyxDQUFDLENBQUM7UUFDOUUsdUJBQWtCLEdBQWlELElBQUksQ0FBQztJQWtDbEYsQ0FBQzs7OztJQWhDQyxRQUFRLEtBQUssT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFFbEMsUUFBUSxDQUFDLEtBQWE7UUFDcEIscUVBQXFFO1FBQ3JFLGlFQUFpRTtRQUNqRSxJQUFJLENBQUMsS0FBSyxJQUFJLENBQUMsSUFBSSxDQUFDLGtCQUFrQjtZQUFFLE9BQU87UUFFL0MsSUFBSSxDQUFDLGtCQUFrQixHQUFHLElBQUksQ0FBQyxrQkFBa0I7WUFDN0MsSUFBSSxhQUFhLENBQUMsT0FBTyxFQUNQLDhDQUF1RSxDQUFDLENBQUM7UUFDL0YsSUFBSSxDQUFDLGtCQUFrQixDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUMxQyxDQUFDOzs7OztJQUVELFVBQVUsQ0FBQyxLQUF5RDtRQUNsRSxJQUFJLENBQUMsY0FBYyxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUN0QyxDQUFDOzs7O0lBRUQsWUFBWTs7Y0FDSixZQUFZLEdBQ2QsSUFBSSxDQUFDLGtCQUFrQixDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsa0JBQWtCLENBQUMsZUFBZSxFQUFFLENBQUMsQ0FBQyxDQUFDLEtBQUs7O2NBQ3pFLGNBQWMsR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLGVBQWUsRUFBRTtRQUM1RCxJQUFJLFlBQVksSUFBSSxjQUFjLEVBQUU7O2dCQUM5QixLQUFLLEdBQUcsSUFBSSxDQUFDLGNBQWMsQ0FBQyxLQUFLO1lBQ3JDLElBQUksSUFBSSxDQUFDLGtCQUFrQixFQUFFOztvQkFDdkIsVUFBVSxHQUFHLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxLQUFLO2dCQUM5QyxJQUFJLFVBQVUsRUFBRTtvQkFDZCxLQUFLLEdBQUcsS0FBSyxDQUFDLENBQUMsbUJBQUssVUFBVSxFQUFLLEtBQUssRUFBRSxDQUFDLENBQUMsVUFBVSxDQUFDO2lCQUN4RDthQUNGO1lBQ0QsSUFBSSxDQUFDLE1BQU0sR0FBRyxLQUFLLENBQUM7U0FDckI7SUFDSCxDQUFDOzs7WUF4Q0YsVUFBVTs7Ozs7OztJQUVULCtCQUFxRDs7Ozs7SUFDckQsdUNBR3NGOzs7OztJQUN0RiwyQ0FBZ0Y7Ozs7OztBQXVDbEYsTUFBTSxPQUFPLDZCQUE2QixHQUFHO0lBQzNDLE9BQU8sRUFBRSxXQUFXO0lBQ3BCLFFBQVEsRUFBRSxhQUFhO0NBQ3hCOztBQUVELE1BQU0sT0FBTyw4QkFBOEIsR0FBRztJQUM1QyxPQUFPLEVBQUUsV0FBVztJQUNwQixRQUFRLEVBQUUsYUFBYTtDQUN4Qjs7QUFFRCxNQUFNLE9BQU8sbUJBQW1CLEdBQUcsNkJBQTZCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuaW1wb3J0IHtFbGVtZW50UmVmLCBJbmplY3RhYmxlLCBJdGVyYWJsZUNoYW5nZXMsIEl0ZXJhYmxlRGlmZmVyLCBJdGVyYWJsZURpZmZlcnMsIEtleVZhbHVlQ2hhbmdlcywgS2V5VmFsdWVEaWZmZXIsIEtleVZhbHVlRGlmZmVycywgUmVuZGVyZXIyLCDJtWlzTGlzdExpa2VJdGVyYWJsZSBhcyBpc0xpc3RMaWtlSXRlcmFibGUsIMm1c3RyaW5naWZ5IGFzIHN0cmluZ2lmeX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7U3R5bGluZ0RpZmZlciwgU3R5bGluZ0RpZmZlck9wdGlvbnN9IGZyb20gJy4vc3R5bGluZ19kaWZmZXInO1xuXG4vKipcbiAqIFVzZWQgYXMgYSB0b2tlbiBmb3IgYW4gaW5qZWN0ZWQgc2VydmljZSB3aXRoaW4gdGhlIE5nQ2xhc3MgZGlyZWN0aXZlLlxuICpcbiAqIE5nQ2xhc3MgYmVoYXZlcyBkaWZmZXJlbmx5IHdoZXRoZXIgb3Igbm90IFZFIGlzIGJlaW5nIHVzZWQgb3Igbm90LiBJZlxuICogcHJlc2VudCB0aGVuIHRoZSBsZWdhY3kgbmdDbGFzcyBkaWZmaW5nIGFsZ29yaXRobSB3aWxsIGJlIHVzZWQgYXMgYW5cbiAqIGluamVjdGVkIHNlcnZpY2UuIE90aGVyd2lzZSB0aGUgbmV3IGRpZmZpbmcgYWxnb3JpdGhtICh3aGljaCBkZWxlZ2F0ZXNcbiAqIHRvIHRoZSBgW2NsYXNzXWAgYmluZGluZykgd2lsbCBiZSB1c2VkLiBUaGlzIHRvZ2dsZSBiZWhhdmlvciBpcyBkb25lIHNvXG4gKiB2aWEgdGhlIGl2eV9zd2l0Y2ggbWVjaGFuaXNtLlxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgTmdDbGFzc0ltcGwge1xuICBhYnN0cmFjdCBzZXRDbGFzcyh2YWx1ZTogc3RyaW5nKTogdm9pZDtcbiAgYWJzdHJhY3Qgc2V0TmdDbGFzcyh2YWx1ZTogc3RyaW5nfHN0cmluZ1tdfFNldDxzdHJpbmc+fHtba2xhc3M6IHN0cmluZ106IGFueX0pOiB2b2lkO1xuICBhYnN0cmFjdCBhcHBseUNoYW5nZXMoKTogdm9pZDtcbiAgYWJzdHJhY3QgZ2V0VmFsdWUoKToge1trZXk6IHN0cmluZ106IGFueX18bnVsbDtcbn1cblxuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE5nQ2xhc3NSMkltcGwgaW1wbGVtZW50cyBOZ0NsYXNzSW1wbCB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9pdGVyYWJsZURpZmZlciAhOiBJdGVyYWJsZURpZmZlcjxzdHJpbmc+fCBudWxsO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfa2V5VmFsdWVEaWZmZXIgITogS2V5VmFsdWVEaWZmZXI8c3RyaW5nLCBhbnk+fCBudWxsO1xuICBwcml2YXRlIF9pbml0aWFsQ2xhc3Nlczogc3RyaW5nW10gPSBbXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3Jhd0NsYXNzICE6IHN0cmluZ1tdIHwgU2V0PHN0cmluZz58IHtba2xhc3M6IHN0cmluZ106IGFueX07XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9pdGVyYWJsZURpZmZlcnM6IEl0ZXJhYmxlRGlmZmVycywgcHJpdmF0ZSBfa2V5VmFsdWVEaWZmZXJzOiBLZXlWYWx1ZURpZmZlcnMsXG4gICAgICBwcml2YXRlIF9uZ0VsOiBFbGVtZW50UmVmLCBwcml2YXRlIF9yZW5kZXJlcjogUmVuZGVyZXIyKSB7fVxuXG4gIGdldFZhbHVlKCkgeyByZXR1cm4gbnVsbDsgfVxuXG4gIHNldENsYXNzKHZhbHVlOiBzdHJpbmcpIHtcbiAgICB0aGlzLl9yZW1vdmVDbGFzc2VzKHRoaXMuX2luaXRpYWxDbGFzc2VzKTtcbiAgICB0aGlzLl9pbml0aWFsQ2xhc3NlcyA9IHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycgPyB2YWx1ZS5zcGxpdCgvXFxzKy8pIDogW107XG4gICAgdGhpcy5fYXBwbHlDbGFzc2VzKHRoaXMuX2luaXRpYWxDbGFzc2VzKTtcbiAgICB0aGlzLl9hcHBseUNsYXNzZXModGhpcy5fcmF3Q2xhc3MpO1xuICB9XG5cbiAgc2V0TmdDbGFzcyh2YWx1ZTogc3RyaW5nKSB7XG4gICAgdGhpcy5fcmVtb3ZlQ2xhc3Nlcyh0aGlzLl9yYXdDbGFzcyk7XG4gICAgdGhpcy5fYXBwbHlDbGFzc2VzKHRoaXMuX2luaXRpYWxDbGFzc2VzKTtcblxuICAgIHRoaXMuX2l0ZXJhYmxlRGlmZmVyID0gbnVsbDtcbiAgICB0aGlzLl9rZXlWYWx1ZURpZmZlciA9IG51bGw7XG5cbiAgICB0aGlzLl9yYXdDbGFzcyA9IHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycgPyB2YWx1ZS5zcGxpdCgvXFxzKy8pIDogdmFsdWU7XG5cbiAgICBpZiAodGhpcy5fcmF3Q2xhc3MpIHtcbiAgICAgIGlmIChpc0xpc3RMaWtlSXRlcmFibGUodGhpcy5fcmF3Q2xhc3MpKSB7XG4gICAgICAgIHRoaXMuX2l0ZXJhYmxlRGlmZmVyID0gdGhpcy5faXRlcmFibGVEaWZmZXJzLmZpbmQodGhpcy5fcmF3Q2xhc3MpLmNyZWF0ZSgpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5fa2V5VmFsdWVEaWZmZXIgPSB0aGlzLl9rZXlWYWx1ZURpZmZlcnMuZmluZCh0aGlzLl9yYXdDbGFzcykuY3JlYXRlKCk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgYXBwbHlDaGFuZ2VzKCkge1xuICAgIGlmICh0aGlzLl9pdGVyYWJsZURpZmZlcikge1xuICAgICAgY29uc3QgaXRlcmFibGVDaGFuZ2VzID0gdGhpcy5faXRlcmFibGVEaWZmZXIuZGlmZih0aGlzLl9yYXdDbGFzcyBhcyBzdHJpbmdbXSk7XG4gICAgICBpZiAoaXRlcmFibGVDaGFuZ2VzKSB7XG4gICAgICAgIHRoaXMuX2FwcGx5SXRlcmFibGVDaGFuZ2VzKGl0ZXJhYmxlQ2hhbmdlcyk7XG4gICAgICB9XG4gICAgfSBlbHNlIGlmICh0aGlzLl9rZXlWYWx1ZURpZmZlcikge1xuICAgICAgY29uc3Qga2V5VmFsdWVDaGFuZ2VzID0gdGhpcy5fa2V5VmFsdWVEaWZmZXIuZGlmZih0aGlzLl9yYXdDbGFzcyBhc3tbazogc3RyaW5nXTogYW55fSk7XG4gICAgICBpZiAoa2V5VmFsdWVDaGFuZ2VzKSB7XG4gICAgICAgIHRoaXMuX2FwcGx5S2V5VmFsdWVDaGFuZ2VzKGtleVZhbHVlQ2hhbmdlcyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfYXBwbHlLZXlWYWx1ZUNoYW5nZXMoY2hhbmdlczogS2V5VmFsdWVDaGFuZ2VzPHN0cmluZywgYW55Pik6IHZvaWQge1xuICAgIGNoYW5nZXMuZm9yRWFjaEFkZGVkSXRlbSgocmVjb3JkKSA9PiB0aGlzLl90b2dnbGVDbGFzcyhyZWNvcmQua2V5LCByZWNvcmQuY3VycmVudFZhbHVlKSk7XG4gICAgY2hhbmdlcy5mb3JFYWNoQ2hhbmdlZEl0ZW0oKHJlY29yZCkgPT4gdGhpcy5fdG9nZ2xlQ2xhc3MocmVjb3JkLmtleSwgcmVjb3JkLmN1cnJlbnRWYWx1ZSkpO1xuICAgIGNoYW5nZXMuZm9yRWFjaFJlbW92ZWRJdGVtKChyZWNvcmQpID0+IHtcbiAgICAgIGlmIChyZWNvcmQucHJldmlvdXNWYWx1ZSkge1xuICAgICAgICB0aGlzLl90b2dnbGVDbGFzcyhyZWNvcmQua2V5LCBmYWxzZSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIF9hcHBseUl0ZXJhYmxlQ2hhbmdlcyhjaGFuZ2VzOiBJdGVyYWJsZUNoYW5nZXM8c3RyaW5nPik6IHZvaWQge1xuICAgIGNoYW5nZXMuZm9yRWFjaEFkZGVkSXRlbSgocmVjb3JkKSA9PiB7XG4gICAgICBpZiAodHlwZW9mIHJlY29yZC5pdGVtID09PSAnc3RyaW5nJykge1xuICAgICAgICB0aGlzLl90b2dnbGVDbGFzcyhyZWNvcmQuaXRlbSwgdHJ1ZSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICBgTmdDbGFzcyBjYW4gb25seSB0b2dnbGUgQ1NTIGNsYXNzZXMgZXhwcmVzc2VkIGFzIHN0cmluZ3MsIGdvdCAke3N0cmluZ2lmeShyZWNvcmQuaXRlbSl9YCk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjaGFuZ2VzLmZvckVhY2hSZW1vdmVkSXRlbSgocmVjb3JkKSA9PiB0aGlzLl90b2dnbGVDbGFzcyhyZWNvcmQuaXRlbSwgZmFsc2UpKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBBcHBsaWVzIGEgY29sbGVjdGlvbiBvZiBDU1MgY2xhc3NlcyB0byB0aGUgRE9NIGVsZW1lbnQuXG4gICAqXG4gICAqIEZvciBhcmd1bWVudCBvZiB0eXBlIFNldCBhbmQgQXJyYXkgQ1NTIGNsYXNzIG5hbWVzIGNvbnRhaW5lZCBpbiB0aG9zZSBjb2xsZWN0aW9ucyBhcmUgYWx3YXlzXG4gICAqIGFkZGVkLlxuICAgKiBGb3IgYXJndW1lbnQgb2YgdHlwZSBNYXAgQ1NTIGNsYXNzIG5hbWUgaW4gdGhlIG1hcCdzIGtleSBpcyB0b2dnbGVkIGJhc2VkIG9uIHRoZSB2YWx1ZSAoYWRkZWRcbiAgICogZm9yIHRydXRoeSBhbmQgcmVtb3ZlZCBmb3IgZmFsc3kpLlxuICAgKi9cbiAgcHJpdmF0ZSBfYXBwbHlDbGFzc2VzKHJhd0NsYXNzVmFsOiBzdHJpbmdbXXxTZXQ8c3RyaW5nPnx7W2tsYXNzOiBzdHJpbmddOiBhbnl9KSB7XG4gICAgaWYgKHJhd0NsYXNzVmFsKSB7XG4gICAgICBpZiAoQXJyYXkuaXNBcnJheShyYXdDbGFzc1ZhbCkgfHwgcmF3Q2xhc3NWYWwgaW5zdGFuY2VvZiBTZXQpIHtcbiAgICAgICAgKDxhbnk+cmF3Q2xhc3NWYWwpLmZvckVhY2goKGtsYXNzOiBzdHJpbmcpID0+IHRoaXMuX3RvZ2dsZUNsYXNzKGtsYXNzLCB0cnVlKSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBPYmplY3Qua2V5cyhyYXdDbGFzc1ZhbCkuZm9yRWFjaChrbGFzcyA9PiB0aGlzLl90b2dnbGVDbGFzcyhrbGFzcywgISFyYXdDbGFzc1ZhbFtrbGFzc10pKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogUmVtb3ZlcyBhIGNvbGxlY3Rpb24gb2YgQ1NTIGNsYXNzZXMgZnJvbSB0aGUgRE9NIGVsZW1lbnQuIFRoaXMgaXMgbW9zdGx5IHVzZWZ1bCBmb3IgY2xlYW51cFxuICAgKiBwdXJwb3Nlcy5cbiAgICovXG4gIHByaXZhdGUgX3JlbW92ZUNsYXNzZXMocmF3Q2xhc3NWYWw6IHN0cmluZ1tdfFNldDxzdHJpbmc+fHtba2xhc3M6IHN0cmluZ106IGFueX0pIHtcbiAgICBpZiAocmF3Q2xhc3NWYWwpIHtcbiAgICAgIGlmIChBcnJheS5pc0FycmF5KHJhd0NsYXNzVmFsKSB8fCByYXdDbGFzc1ZhbCBpbnN0YW5jZW9mIFNldCkge1xuICAgICAgICAoPGFueT5yYXdDbGFzc1ZhbCkuZm9yRWFjaCgoa2xhc3M6IHN0cmluZykgPT4gdGhpcy5fdG9nZ2xlQ2xhc3Moa2xhc3MsIGZhbHNlKSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBPYmplY3Qua2V5cyhyYXdDbGFzc1ZhbCkuZm9yRWFjaChrbGFzcyA9PiB0aGlzLl90b2dnbGVDbGFzcyhrbGFzcywgZmFsc2UpKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF90b2dnbGVDbGFzcyhrbGFzczogc3RyaW5nLCBlbmFibGVkOiBib29sZWFuKTogdm9pZCB7XG4gICAga2xhc3MgPSBrbGFzcy50cmltKCk7XG4gICAgaWYgKGtsYXNzKSB7XG4gICAgICBrbGFzcy5zcGxpdCgvXFxzKy9nKS5mb3JFYWNoKGtsYXNzID0+IHtcbiAgICAgICAgaWYgKGVuYWJsZWQpIHtcbiAgICAgICAgICB0aGlzLl9yZW5kZXJlci5hZGRDbGFzcyh0aGlzLl9uZ0VsLm5hdGl2ZUVsZW1lbnQsIGtsYXNzKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aGlzLl9yZW5kZXJlci5yZW1vdmVDbGFzcyh0aGlzLl9uZ0VsLm5hdGl2ZUVsZW1lbnQsIGtsYXNzKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9XG59XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBOZ0NsYXNzUjNJbXBsIGltcGxlbWVudHMgTmdDbGFzc0ltcGwge1xuICBwcml2YXRlIF92YWx1ZToge1trZXk6IHN0cmluZ106IGJvb2xlYW59fG51bGwgPSBudWxsO1xuICBwcml2YXRlIF9uZ0NsYXNzRGlmZmVyID0gbmV3IFN0eWxpbmdEaWZmZXI8e1trZXk6IHN0cmluZ106IGJvb2xlYW59fG51bGw+KFxuICAgICAgJ05nQ2xhc3MnLCBTdHlsaW5nRGlmZmVyT3B0aW9ucy5UcmltUHJvcGVydGllc3xcbiAgICAgICAgICAgICAgICAgU3R5bGluZ0RpZmZlck9wdGlvbnMuQWxsb3dTdWJLZXlzfFxuICAgICAgICAgICAgICAgICBTdHlsaW5nRGlmZmVyT3B0aW9ucy5BbGxvd1N0cmluZ1ZhbHVlfFN0eWxpbmdEaWZmZXJPcHRpb25zLkZvcmNlQXNNYXApO1xuICBwcml2YXRlIF9jbGFzc1N0cmluZ0RpZmZlcjogU3R5bGluZ0RpZmZlcjx7W2tleTogc3RyaW5nXTogYm9vbGVhbn0+fG51bGwgPSBudWxsO1xuXG4gIGdldFZhbHVlKCkgeyByZXR1cm4gdGhpcy5fdmFsdWU7IH1cblxuICBzZXRDbGFzcyh2YWx1ZTogc3RyaW5nKSB7XG4gICAgLy8gZWFybHkgZXhpdCBpbmNhc2UgdGhlIGJpbmRpbmcgZ2V0cyBlbWl0dGVkIGFzIGFuIGVtcHR5IHZhbHVlIHdoaWNoXG4gICAgLy8gbWVhbnMgdGhlcmUgaXMgbm8gcmVhc29uIHRvIGluc3RhbnRpYXRlIGFuZCBkaWZmIHRoZSB2YWx1ZXMuLi5cbiAgICBpZiAoIXZhbHVlICYmICF0aGlzLl9jbGFzc1N0cmluZ0RpZmZlcikgcmV0dXJuO1xuXG4gICAgdGhpcy5fY2xhc3NTdHJpbmdEaWZmZXIgPSB0aGlzLl9jbGFzc1N0cmluZ0RpZmZlciB8fFxuICAgICAgICBuZXcgU3R5bGluZ0RpZmZlcignY2xhc3MnLFxuICAgICAgICAgICAgICAgICAgICAgICAgICBTdHlsaW5nRGlmZmVyT3B0aW9ucy5BbGxvd1N0cmluZ1ZhbHVlIHwgU3R5bGluZ0RpZmZlck9wdGlvbnMuRm9yY2VBc01hcCk7XG4gICAgdGhpcy5fY2xhc3NTdHJpbmdEaWZmZXIuc2V0VmFsdWUodmFsdWUpO1xuICB9XG5cbiAgc2V0TmdDbGFzcyh2YWx1ZTogc3RyaW5nfHN0cmluZ1tdfFNldDxzdHJpbmc+fHtba2xhc3M6IHN0cmluZ106IGFueX0pIHtcbiAgICB0aGlzLl9uZ0NsYXNzRGlmZmVyLnNldFZhbHVlKHZhbHVlKTtcbiAgfVxuXG4gIGFwcGx5Q2hhbmdlcygpIHtcbiAgICBjb25zdCBjbGFzc0NoYW5nZWQgPVxuICAgICAgICB0aGlzLl9jbGFzc1N0cmluZ0RpZmZlciA/IHRoaXMuX2NsYXNzU3RyaW5nRGlmZmVyLmhhc1ZhbHVlQ2hhbmdlZCgpIDogZmFsc2U7XG4gICAgY29uc3QgbmdDbGFzc0NoYW5nZWQgPSB0aGlzLl9uZ0NsYXNzRGlmZmVyLmhhc1ZhbHVlQ2hhbmdlZCgpO1xuICAgIGlmIChjbGFzc0NoYW5nZWQgfHwgbmdDbGFzc0NoYW5nZWQpIHtcbiAgICAgIGxldCB2YWx1ZSA9IHRoaXMuX25nQ2xhc3NEaWZmZXIudmFsdWU7XG4gICAgICBpZiAodGhpcy5fY2xhc3NTdHJpbmdEaWZmZXIpIHtcbiAgICAgICAgbGV0IGNsYXNzVmFsdWUgPSB0aGlzLl9jbGFzc1N0cmluZ0RpZmZlci52YWx1ZTtcbiAgICAgICAgaWYgKGNsYXNzVmFsdWUpIHtcbiAgICAgICAgICB2YWx1ZSA9IHZhbHVlID8gey4uLmNsYXNzVmFsdWUsIC4uLnZhbHVlfSA6IGNsYXNzVmFsdWU7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHRoaXMuX3ZhbHVlID0gdmFsdWU7XG4gICAgfVxuICB9XG59XG5cbi8vIHRoZSBpbXBsZW1lbnRhdGlvbiBmb3IgYm90aCBOZ1N0eWxlUjJJbXBsIGFuZCBOZ1N0eWxlUjNJbXBsIGFyZVxuLy8gbm90IGl2eV9zd2l0Y2gnZCBhd2F5LCBpbnN0ZWFkIHRoZXkgYXJlIG9ubHkgaG9va2VkIHVwIGludG8gdGhlXG4vLyBESSB2aWEgTmdTdHlsZSdzIGRpcmVjdGl2ZSdzIHByb3ZpZGVyIHByb3BlcnR5LlxuZXhwb3J0IGNvbnN0IE5nQ2xhc3NJbXBsUHJvdmlkZXJfX1BSRV9SM19fID0ge1xuICBwcm92aWRlOiBOZ0NsYXNzSW1wbCxcbiAgdXNlQ2xhc3M6IE5nQ2xhc3NSMkltcGxcbn07XG5cbmV4cG9ydCBjb25zdCBOZ0NsYXNzSW1wbFByb3ZpZGVyX19QT1NUX1IzX18gPSB7XG4gIHByb3ZpZGU6IE5nQ2xhc3NJbXBsLFxuICB1c2VDbGFzczogTmdDbGFzc1IzSW1wbFxufTtcblxuZXhwb3J0IGNvbnN0IE5nQ2xhc3NJbXBsUHJvdmlkZXIgPSBOZ0NsYXNzSW1wbFByb3ZpZGVyX19QUkVfUjNfXztcbiJdfQ==