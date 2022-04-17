/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { looseIdentical } from '../../util/comparison';
import { stringify } from '../../util/stringify';
import { isJsObject } from '../change_detection_util';
var DefaultKeyValueDifferFactory = /** @class */ (function () {
    function DefaultKeyValueDifferFactory() {
    }
    DefaultKeyValueDifferFactory.prototype.supports = function (obj) { return obj instanceof Map || isJsObject(obj); };
    DefaultKeyValueDifferFactory.prototype.create = function () { return new DefaultKeyValueDiffer(); };
    return DefaultKeyValueDifferFactory;
}());
export { DefaultKeyValueDifferFactory };
var DefaultKeyValueDiffer = /** @class */ (function () {
    function DefaultKeyValueDiffer() {
        this._records = new Map();
        this._mapHead = null;
        // _appendAfter is used in the check loop
        this._appendAfter = null;
        this._previousMapHead = null;
        this._changesHead = null;
        this._changesTail = null;
        this._additionsHead = null;
        this._additionsTail = null;
        this._removalsHead = null;
        this._removalsTail = null;
    }
    Object.defineProperty(DefaultKeyValueDiffer.prototype, "isDirty", {
        get: function () {
            return this._additionsHead !== null || this._changesHead !== null ||
                this._removalsHead !== null;
        },
        enumerable: true,
        configurable: true
    });
    DefaultKeyValueDiffer.prototype.forEachItem = function (fn) {
        var record;
        for (record = this._mapHead; record !== null; record = record._next) {
            fn(record);
        }
    };
    DefaultKeyValueDiffer.prototype.forEachPreviousItem = function (fn) {
        var record;
        for (record = this._previousMapHead; record !== null; record = record._nextPrevious) {
            fn(record);
        }
    };
    DefaultKeyValueDiffer.prototype.forEachChangedItem = function (fn) {
        var record;
        for (record = this._changesHead; record !== null; record = record._nextChanged) {
            fn(record);
        }
    };
    DefaultKeyValueDiffer.prototype.forEachAddedItem = function (fn) {
        var record;
        for (record = this._additionsHead; record !== null; record = record._nextAdded) {
            fn(record);
        }
    };
    DefaultKeyValueDiffer.prototype.forEachRemovedItem = function (fn) {
        var record;
        for (record = this._removalsHead; record !== null; record = record._nextRemoved) {
            fn(record);
        }
    };
    DefaultKeyValueDiffer.prototype.diff = function (map) {
        if (!map) {
            map = new Map();
        }
        else if (!(map instanceof Map || isJsObject(map))) {
            throw new Error("Error trying to diff '" + stringify(map) + "'. Only maps and objects are allowed");
        }
        return this.check(map) ? this : null;
    };
    DefaultKeyValueDiffer.prototype.onDestroy = function () { };
    /**
     * Check the current state of the map vs the previous.
     * The algorithm is optimised for when the keys do no change.
     */
    DefaultKeyValueDiffer.prototype.check = function (map) {
        var _this = this;
        this._reset();
        var insertBefore = this._mapHead;
        this._appendAfter = null;
        this._forEach(map, function (value, key) {
            if (insertBefore && insertBefore.key === key) {
                _this._maybeAddToChanges(insertBefore, value);
                _this._appendAfter = insertBefore;
                insertBefore = insertBefore._next;
            }
            else {
                var record = _this._getOrCreateRecordForKey(key, value);
                insertBefore = _this._insertBeforeOrAppend(insertBefore, record);
            }
        });
        // Items remaining at the end of the list have been deleted
        if (insertBefore) {
            if (insertBefore._prev) {
                insertBefore._prev._next = null;
            }
            this._removalsHead = insertBefore;
            for (var record = insertBefore; record !== null; record = record._nextRemoved) {
                if (record === this._mapHead) {
                    this._mapHead = null;
                }
                this._records.delete(record.key);
                record._nextRemoved = record._next;
                record.previousValue = record.currentValue;
                record.currentValue = null;
                record._prev = null;
                record._next = null;
            }
        }
        // Make sure tails have no next records from previous runs
        if (this._changesTail)
            this._changesTail._nextChanged = null;
        if (this._additionsTail)
            this._additionsTail._nextAdded = null;
        return this.isDirty;
    };
    /**
     * Inserts a record before `before` or append at the end of the list when `before` is null.
     *
     * Notes:
     * - This method appends at `this._appendAfter`,
     * - This method updates `this._appendAfter`,
     * - The return value is the new value for the insertion pointer.
     */
    DefaultKeyValueDiffer.prototype._insertBeforeOrAppend = function (before, record) {
        if (before) {
            var prev = before._prev;
            record._next = before;
            record._prev = prev;
            before._prev = record;
            if (prev) {
                prev._next = record;
            }
            if (before === this._mapHead) {
                this._mapHead = record;
            }
            this._appendAfter = before;
            return before;
        }
        if (this._appendAfter) {
            this._appendAfter._next = record;
            record._prev = this._appendAfter;
        }
        else {
            this._mapHead = record;
        }
        this._appendAfter = record;
        return null;
    };
    DefaultKeyValueDiffer.prototype._getOrCreateRecordForKey = function (key, value) {
        if (this._records.has(key)) {
            var record_1 = this._records.get(key);
            this._maybeAddToChanges(record_1, value);
            var prev = record_1._prev;
            var next = record_1._next;
            if (prev) {
                prev._next = next;
            }
            if (next) {
                next._prev = prev;
            }
            record_1._next = null;
            record_1._prev = null;
            return record_1;
        }
        var record = new KeyValueChangeRecord_(key);
        this._records.set(key, record);
        record.currentValue = value;
        this._addToAdditions(record);
        return record;
    };
    /** @internal */
    DefaultKeyValueDiffer.prototype._reset = function () {
        if (this.isDirty) {
            var record = void 0;
            // let `_previousMapHead` contain the state of the map before the changes
            this._previousMapHead = this._mapHead;
            for (record = this._previousMapHead; record !== null; record = record._next) {
                record._nextPrevious = record._next;
            }
            // Update `record.previousValue` with the value of the item before the changes
            // We need to update all changed items (that's those which have been added and changed)
            for (record = this._changesHead; record !== null; record = record._nextChanged) {
                record.previousValue = record.currentValue;
            }
            for (record = this._additionsHead; record != null; record = record._nextAdded) {
                record.previousValue = record.currentValue;
            }
            this._changesHead = this._changesTail = null;
            this._additionsHead = this._additionsTail = null;
            this._removalsHead = null;
        }
    };
    // Add the record or a given key to the list of changes only when the value has actually changed
    DefaultKeyValueDiffer.prototype._maybeAddToChanges = function (record, newValue) {
        if (!looseIdentical(newValue, record.currentValue)) {
            record.previousValue = record.currentValue;
            record.currentValue = newValue;
            this._addToChanges(record);
        }
    };
    DefaultKeyValueDiffer.prototype._addToAdditions = function (record) {
        if (this._additionsHead === null) {
            this._additionsHead = this._additionsTail = record;
        }
        else {
            this._additionsTail._nextAdded = record;
            this._additionsTail = record;
        }
    };
    DefaultKeyValueDiffer.prototype._addToChanges = function (record) {
        if (this._changesHead === null) {
            this._changesHead = this._changesTail = record;
        }
        else {
            this._changesTail._nextChanged = record;
            this._changesTail = record;
        }
    };
    /** @internal */
    DefaultKeyValueDiffer.prototype._forEach = function (obj, fn) {
        if (obj instanceof Map) {
            obj.forEach(fn);
        }
        else {
            Object.keys(obj).forEach(function (k) { return fn(obj[k], k); });
        }
    };
    return DefaultKeyValueDiffer;
}());
export { DefaultKeyValueDiffer };
var KeyValueChangeRecord_ = /** @class */ (function () {
    function KeyValueChangeRecord_(key) {
        this.key = key;
        this.previousValue = null;
        this.currentValue = null;
        /** @internal */
        this._nextPrevious = null;
        /** @internal */
        this._next = null;
        /** @internal */
        this._prev = null;
        /** @internal */
        this._nextAdded = null;
        /** @internal */
        this._nextRemoved = null;
        /** @internal */
        this._nextChanged = null;
    }
    return KeyValueChangeRecord_;
}());
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGVmYXVsdF9rZXl2YWx1ZV9kaWZmZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9jaGFuZ2VfZGV0ZWN0aW9uL2RpZmZlcnMvZGVmYXVsdF9rZXl2YWx1ZV9kaWZmZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxFQUFDLGNBQWMsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQ3JELE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxzQkFBc0IsQ0FBQztBQUMvQyxPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0sMEJBQTBCLENBQUM7QUFJcEQ7SUFDRTtJQUFlLENBQUM7SUFDaEIsK0NBQVEsR0FBUixVQUFTLEdBQVEsSUFBYSxPQUFPLEdBQUcsWUFBWSxHQUFHLElBQUksVUFBVSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUU3RSw2Q0FBTSxHQUFOLGNBQXVDLE9BQU8sSUFBSSxxQkFBcUIsRUFBUSxDQUFDLENBQUMsQ0FBQztJQUNwRixtQ0FBQztBQUFELENBQUMsQUFMRCxJQUtDOztBQUVEO0lBQUE7UUFDVSxhQUFRLEdBQUcsSUFBSSxHQUFHLEVBQWtDLENBQUM7UUFDckQsYUFBUSxHQUFxQyxJQUFJLENBQUM7UUFDMUQseUNBQXlDO1FBQ2pDLGlCQUFZLEdBQXFDLElBQUksQ0FBQztRQUN0RCxxQkFBZ0IsR0FBcUMsSUFBSSxDQUFDO1FBQzFELGlCQUFZLEdBQXFDLElBQUksQ0FBQztRQUN0RCxpQkFBWSxHQUFxQyxJQUFJLENBQUM7UUFDdEQsbUJBQWMsR0FBcUMsSUFBSSxDQUFDO1FBQ3hELG1CQUFjLEdBQXFDLElBQUksQ0FBQztRQUN4RCxrQkFBYSxHQUFxQyxJQUFJLENBQUM7UUFDdkQsa0JBQWEsR0FBcUMsSUFBSSxDQUFDO0lBb09qRSxDQUFDO0lBbE9DLHNCQUFJLDBDQUFPO2FBQVg7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LEdBQXFDLElBQUksQ0FBQztRQUN0RCxnQkFBZ0I7UUFDaEIsaUJBQVksR0FBcUMsSUFBSSxDQUFDO0lBRTFCLENBQUM7SUFDL0IsNEJBQUM7QUFBRCxDQUFDLEFBbEJELElBa0JDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2xvb3NlSWRlbnRpY2FsfSBmcm9tICcuLi8uLi91dGlsL2NvbXBhcmlzb24nO1xuaW1wb3J0IHtzdHJpbmdpZnl9IGZyb20gJy4uLy4uL3V0aWwvc3RyaW5naWZ5JztcbmltcG9ydCB7aXNKc09iamVjdH0gZnJvbSAnLi4vY2hhbmdlX2RldGVjdGlvbl91dGlsJztcbmltcG9ydCB7S2V5VmFsdWVDaGFuZ2VSZWNvcmQsIEtleVZhbHVlQ2hhbmdlcywgS2V5VmFsdWVEaWZmZXIsIEtleVZhbHVlRGlmZmVyRmFjdG9yeX0gZnJvbSAnLi9rZXl2YWx1ZV9kaWZmZXJzJztcblxuXG5leHBvcnQgY2xhc3MgRGVmYXVsdEtleVZhbHVlRGlmZmVyRmFjdG9yeTxLLCBWPiBpbXBsZW1lbnRzIEtleVZhbHVlRGlmZmVyRmFjdG9yeSB7XG4gIGNvbnN0cnVjdG9yKCkge31cbiAgc3VwcG9ydHMob2JqOiBhbnkpOiBib29sZWFuIHsgcmV0dXJuIG9iaiBpbnN0YW5jZW9mIE1hcCB8fCBpc0pzT2JqZWN0KG9iaik7IH1cblxuICBjcmVhdGU8SywgVj4oKTogS2V5VmFsdWVEaWZmZXI8SywgVj4geyByZXR1cm4gbmV3IERlZmF1bHRLZXlWYWx1ZURpZmZlcjxLLCBWPigpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBEZWZhdWx0S2V5VmFsdWVEaWZmZXI8SywgVj4gaW1wbGVtZW50cyBLZXlWYWx1ZURpZmZlcjxLLCBWPiwgS2V5VmFsdWVDaGFuZ2VzPEssIFY+IHtcbiAgcHJpdmF0ZSBfcmVjb3JkcyA9IG5ldyBNYXA8SywgS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+PigpO1xuICBwcml2YXRlIF9tYXBIZWFkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbCA9IG51bGw7XG4gIC8vIF9hcHBlbmRBZnRlciBpcyB1c2VkIGluIHRoZSBjaGVjayBsb29wXG4gIHByaXZhdGUgX2FwcGVuZEFmdGVyOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX3ByZXZpb3VzTWFwSGVhZDogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGwgPSBudWxsO1xuICBwcml2YXRlIF9jaGFuZ2VzSGVhZDogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGwgPSBudWxsO1xuICBwcml2YXRlIF9jaGFuZ2VzVGFpbDogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGwgPSBudWxsO1xuICBwcml2YXRlIF9hZGRpdGlvbnNIZWFkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX2FkZGl0aW9uc1RhaWw6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfcmVtb3ZhbHNIZWFkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX3JlbW92YWxzVGFpbDogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGwgPSBudWxsO1xuXG4gIGdldCBpc0RpcnR5KCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9hZGRpdGlvbnNIZWFkICE9PSBudWxsIHx8IHRoaXMuX2NoYW5nZXNIZWFkICE9PSBudWxsIHx8XG4gICAgICAgIHRoaXMuX3JlbW92YWxzSGVhZCAhPT0gbnVsbDtcbiAgfVxuXG4gIGZvckVhY2hJdGVtKGZuOiAocjogS2V5VmFsdWVDaGFuZ2VSZWNvcmQ8SywgVj4pID0+IHZvaWQpIHtcbiAgICBsZXQgcmVjb3JkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbDtcbiAgICBmb3IgKHJlY29yZCA9IHRoaXMuX21hcEhlYWQ7IHJlY29yZCAhPT0gbnVsbDsgcmVjb3JkID0gcmVjb3JkLl9uZXh0KSB7XG4gICAgICBmbihyZWNvcmQpO1xuICAgIH1cbiAgfVxuXG4gIGZvckVhY2hQcmV2aW91c0l0ZW0oZm46IChyOiBLZXlWYWx1ZUNoYW5nZVJlY29yZDxLLCBWPikgPT4gdm9pZCkge1xuICAgIGxldCByZWNvcmQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsO1xuICAgIGZvciAocmVjb3JkID0gdGhpcy5fcHJldmlvdXNNYXBIZWFkOyByZWNvcmQgIT09IG51bGw7IHJlY29yZCA9IHJlY29yZC5fbmV4dFByZXZpb3VzKSB7XG4gICAgICBmbihyZWNvcmQpO1xuICAgIH1cbiAgfVxuXG4gIGZvckVhY2hDaGFuZ2VkSXRlbShmbjogKHI6IEtleVZhbHVlQ2hhbmdlUmVjb3JkPEssIFY+KSA9PiB2b2lkKSB7XG4gICAgbGV0IHJlY29yZDogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGw7XG4gICAgZm9yIChyZWNvcmQgPSB0aGlzLl9jaGFuZ2VzSGVhZDsgcmVjb3JkICE9PSBudWxsOyByZWNvcmQgPSByZWNvcmQuX25leHRDaGFuZ2VkKSB7XG4gICAgICBmbihyZWNvcmQpO1xuICAgIH1cbiAgfVxuXG4gIGZvckVhY2hBZGRlZEl0ZW0oZm46IChyOiBLZXlWYWx1ZUNoYW5nZVJlY29yZDxLLCBWPikgPT4gdm9pZCkge1xuICAgIGxldCByZWNvcmQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsO1xuICAgIGZvciAocmVjb3JkID0gdGhpcy5fYWRkaXRpb25zSGVhZDsgcmVjb3JkICE9PSBudWxsOyByZWNvcmQgPSByZWNvcmQuX25leHRBZGRlZCkge1xuICAgICAgZm4ocmVjb3JkKTtcbiAgICB9XG4gIH1cblxuICBmb3JFYWNoUmVtb3ZlZEl0ZW0oZm46IChyOiBLZXlWYWx1ZUNoYW5nZVJlY29yZDxLLCBWPikgPT4gdm9pZCkge1xuICAgIGxldCByZWNvcmQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsO1xuICAgIGZvciAocmVjb3JkID0gdGhpcy5fcmVtb3ZhbHNIZWFkOyByZWNvcmQgIT09IG51bGw7IHJlY29yZCA9IHJlY29yZC5fbmV4dFJlbW92ZWQpIHtcbiAgICAgIGZuKHJlY29yZCk7XG4gICAgfVxuICB9XG5cbiAgZGlmZihtYXA/OiBNYXA8YW55LCBhbnk+fHtbazogc3RyaW5nXTogYW55fXxudWxsKTogYW55IHtcbiAgICBpZiAoIW1hcCkge1xuICAgICAgbWFwID0gbmV3IE1hcCgpO1xuICAgIH0gZWxzZSBpZiAoIShtYXAgaW5zdGFuY2VvZiBNYXAgfHwgaXNKc09iamVjdChtYXApKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBFcnJvciB0cnlpbmcgdG8gZGlmZiAnJHtzdHJpbmdpZnkobWFwKX0nLiBPbmx5IG1hcHMgYW5kIG9iamVjdHMgYXJlIGFsbG93ZWRgKTtcbiAgICB9XG5cbiAgICByZXR1cm4gdGhpcy5jaGVjayhtYXApID8gdGhpcyA6IG51bGw7XG4gIH1cblxuICBvbkRlc3Ryb3koKSB7fVxuXG4gIC8qKlxuICAgKiBDaGVjayB0aGUgY3VycmVudCBzdGF0ZSBvZiB0aGUgbWFwIHZzIHRoZSBwcmV2aW91cy5cbiAgICogVGhlIGFsZ29yaXRobSBpcyBvcHRpbWlzZWQgZm9yIHdoZW4gdGhlIGtleXMgZG8gbm8gY2hhbmdlLlxuICAgKi9cbiAgY2hlY2sobWFwOiBNYXA8YW55LCBhbnk+fHtbazogc3RyaW5nXTogYW55fSk6IGJvb2xlYW4ge1xuICAgIHRoaXMuX3Jlc2V0KCk7XG5cbiAgICBsZXQgaW5zZXJ0QmVmb3JlID0gdGhpcy5fbWFwSGVhZDtcbiAgICB0aGlzLl9hcHBlbmRBZnRlciA9IG51bGw7XG5cbiAgICB0aGlzLl9mb3JFYWNoKG1hcCwgKHZhbHVlOiBhbnksIGtleTogYW55KSA9PiB7XG4gICAgICBpZiAoaW5zZXJ0QmVmb3JlICYmIGluc2VydEJlZm9yZS5rZXkgPT09IGtleSkge1xuICAgICAgICB0aGlzLl9tYXliZUFkZFRvQ2hhbmdlcyhpbnNlcnRCZWZvcmUsIHZhbHVlKTtcbiAgICAgICAgdGhpcy5fYXBwZW5kQWZ0ZXIgPSBpbnNlcnRCZWZvcmU7XG4gICAgICAgIGluc2VydEJlZm9yZSA9IGluc2VydEJlZm9yZS5fbmV4dDtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnN0IHJlY29yZCA9IHRoaXMuX2dldE9yQ3JlYXRlUmVjb3JkRm9yS2V5KGtleSwgdmFsdWUpO1xuICAgICAgICBpbnNlcnRCZWZvcmUgPSB0aGlzLl9pbnNlcnRCZWZvcmVPckFwcGVuZChpbnNlcnRCZWZvcmUsIHJlY29yZCk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICAvLyBJdGVtcyByZW1haW5pbmcgYXQgdGhlIGVuZCBvZiB0aGUgbGlzdCBoYXZlIGJlZW4gZGVsZXRlZFxuICAgIGlmIChpbnNlcnRCZWZvcmUpIHtcbiAgICAgIGlmIChpbnNlcnRCZWZvcmUuX3ByZXYpIHtcbiAgICAgICAgaW5zZXJ0QmVmb3JlLl9wcmV2Ll9uZXh0ID0gbnVsbDtcbiAgICAgIH1cblxuICAgICAgdGhpcy5fcmVtb3ZhbHNIZWFkID0gaW5zZXJ0QmVmb3JlO1xuXG4gICAgICBmb3IgKGxldCByZWNvcmQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsID0gaW5zZXJ0QmVmb3JlOyByZWNvcmQgIT09IG51bGw7XG4gICAgICAgICAgIHJlY29yZCA9IHJlY29yZC5fbmV4dFJlbW92ZWQpIHtcbiAgICAgICAgaWYgKHJlY29yZCA9PT0gdGhpcy5fbWFwSGVhZCkge1xuICAgICAgICAgIHRoaXMuX21hcEhlYWQgPSBudWxsO1xuICAgICAgICB9XG4gICAgICAgIHRoaXMuX3JlY29yZHMuZGVsZXRlKHJlY29yZC5rZXkpO1xuICAgICAgICByZWNvcmQuX25leHRSZW1vdmVkID0gcmVjb3JkLl9uZXh0O1xuICAgICAgICByZWNvcmQucHJldmlvdXNWYWx1ZSA9IHJlY29yZC5jdXJyZW50VmFsdWU7XG4gICAgICAgIHJlY29yZC5jdXJyZW50VmFsdWUgPSBudWxsO1xuICAgICAgICByZWNvcmQuX3ByZXYgPSBudWxsO1xuICAgICAgICByZWNvcmQuX25leHQgPSBudWxsO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIE1ha2Ugc3VyZSB0YWlscyBoYXZlIG5vIG5leHQgcmVjb3JkcyBmcm9tIHByZXZpb3VzIHJ1bnNcbiAgICBpZiAodGhpcy5fY2hhbmdlc1RhaWwpIHRoaXMuX2NoYW5nZXNUYWlsLl9uZXh0Q2hhbmdlZCA9IG51bGw7XG4gICAgaWYgKHRoaXMuX2FkZGl0aW9uc1RhaWwpIHRoaXMuX2FkZGl0aW9uc1RhaWwuX25leHRBZGRlZCA9IG51bGw7XG5cbiAgICByZXR1cm4gdGhpcy5pc0RpcnR5O1xuICB9XG5cbiAgLyoqXG4gICAqIEluc2VydHMgYSByZWNvcmQgYmVmb3JlIGBiZWZvcmVgIG9yIGFwcGVuZCBhdCB0aGUgZW5kIG9mIHRoZSBsaXN0IHdoZW4gYGJlZm9yZWAgaXMgbnVsbC5cbiAgICpcbiAgICogTm90ZXM6XG4gICAqIC0gVGhpcyBtZXRob2QgYXBwZW5kcyBhdCBgdGhpcy5fYXBwZW5kQWZ0ZXJgLFxuICAgKiAtIFRoaXMgbWV0aG9kIHVwZGF0ZXMgYHRoaXMuX2FwcGVuZEFmdGVyYCxcbiAgICogLSBUaGUgcmV0dXJuIHZhbHVlIGlzIHRoZSBuZXcgdmFsdWUgZm9yIHRoZSBpbnNlcnRpb24gcG9pbnRlci5cbiAgICovXG4gIHByaXZhdGUgX2luc2VydEJlZm9yZU9yQXBwZW5kKFxuICAgICAgYmVmb3JlOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbCxcbiAgICAgIHJlY29yZDogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+KTogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGwge1xuICAgIGlmIChiZWZvcmUpIHtcbiAgICAgIGNvbnN0IHByZXYgPSBiZWZvcmUuX3ByZXY7XG4gICAgICByZWNvcmQuX25leHQgPSBiZWZvcmU7XG4gICAgICByZWNvcmQuX3ByZXYgPSBwcmV2O1xuICAgICAgYmVmb3JlLl9wcmV2ID0gcmVjb3JkO1xuICAgICAgaWYgKHByZXYpIHtcbiAgICAgICAgcHJldi5fbmV4dCA9IHJlY29yZDtcbiAgICAgIH1cbiAgICAgIGlmIChiZWZvcmUgPT09IHRoaXMuX21hcEhlYWQpIHtcbiAgICAgICAgdGhpcy5fbWFwSGVhZCA9IHJlY29yZDtcbiAgICAgIH1cblxuICAgICAgdGhpcy5fYXBwZW5kQWZ0ZXIgPSBiZWZvcmU7XG4gICAgICByZXR1cm4gYmVmb3JlO1xuICAgIH1cblxuICAgIGlmICh0aGlzLl9hcHBlbmRBZnRlcikge1xuICAgICAgdGhpcy5fYXBwZW5kQWZ0ZXIuX25leHQgPSByZWNvcmQ7XG4gICAgICByZWNvcmQuX3ByZXYgPSB0aGlzLl9hcHBlbmRBZnRlcjtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5fbWFwSGVhZCA9IHJlY29yZDtcbiAgICB9XG5cbiAgICB0aGlzLl9hcHBlbmRBZnRlciA9IHJlY29yZDtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHByaXZhdGUgX2dldE9yQ3JlYXRlUmVjb3JkRm9yS2V5KGtleTogSywgdmFsdWU6IFYpOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj4ge1xuICAgIGlmICh0aGlzLl9yZWNvcmRzLmhhcyhrZXkpKSB7XG4gICAgICBjb25zdCByZWNvcmQgPSB0aGlzLl9yZWNvcmRzLmdldChrZXkpICE7XG4gICAgICB0aGlzLl9tYXliZUFkZFRvQ2hhbmdlcyhyZWNvcmQsIHZhbHVlKTtcbiAgICAgIGNvbnN0IHByZXYgPSByZWNvcmQuX3ByZXY7XG4gICAgICBjb25zdCBuZXh0ID0gcmVjb3JkLl9uZXh0O1xuICAgICAgaWYgKHByZXYpIHtcbiAgICAgICAgcHJldi5fbmV4dCA9IG5leHQ7XG4gICAgICB9XG4gICAgICBpZiAobmV4dCkge1xuICAgICAgICBuZXh0Ll9wcmV2ID0gcHJldjtcbiAgICAgIH1cbiAgICAgIHJlY29yZC5fbmV4dCA9IG51bGw7XG4gICAgICByZWNvcmQuX3ByZXYgPSBudWxsO1xuXG4gICAgICByZXR1cm4gcmVjb3JkO1xuICAgIH1cblxuICAgIGNvbnN0IHJlY29yZCA9IG5ldyBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj4oa2V5KTtcbiAgICB0aGlzLl9yZWNvcmRzLnNldChrZXksIHJlY29yZCk7XG4gICAgcmVjb3JkLmN1cnJlbnRWYWx1ZSA9IHZhbHVlO1xuICAgIHRoaXMuX2FkZFRvQWRkaXRpb25zKHJlY29yZCk7XG4gICAgcmV0dXJuIHJlY29yZDtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3Jlc2V0KCkge1xuICAgIGlmICh0aGlzLmlzRGlydHkpIHtcbiAgICAgIGxldCByZWNvcmQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsO1xuICAgICAgLy8gbGV0IGBfcHJldmlvdXNNYXBIZWFkYCBjb250YWluIHRoZSBzdGF0ZSBvZiB0aGUgbWFwIGJlZm9yZSB0aGUgY2hhbmdlc1xuICAgICAgdGhpcy5fcHJldmlvdXNNYXBIZWFkID0gdGhpcy5fbWFwSGVhZDtcbiAgICAgIGZvciAocmVjb3JkID0gdGhpcy5fcHJldmlvdXNNYXBIZWFkOyByZWNvcmQgIT09IG51bGw7IHJlY29yZCA9IHJlY29yZC5fbmV4dCkge1xuICAgICAgICByZWNvcmQuX25leHRQcmV2aW91cyA9IHJlY29yZC5fbmV4dDtcbiAgICAgIH1cblxuICAgICAgLy8gVXBkYXRlIGByZWNvcmQucHJldmlvdXNWYWx1ZWAgd2l0aCB0aGUgdmFsdWUgb2YgdGhlIGl0ZW0gYmVmb3JlIHRoZSBjaGFuZ2VzXG4gICAgICAvLyBXZSBuZWVkIHRvIHVwZGF0ZSBhbGwgY2hhbmdlZCBpdGVtcyAodGhhdCdzIHRob3NlIHdoaWNoIGhhdmUgYmVlbiBhZGRlZCBhbmQgY2hhbmdlZClcbiAgICAgIGZvciAocmVjb3JkID0gdGhpcy5fY2hhbmdlc0hlYWQ7IHJlY29yZCAhPT0gbnVsbDsgcmVjb3JkID0gcmVjb3JkLl9uZXh0Q2hhbmdlZCkge1xuICAgICAgICByZWNvcmQucHJldmlvdXNWYWx1ZSA9IHJlY29yZC5jdXJyZW50VmFsdWU7XG4gICAgICB9XG4gICAgICBmb3IgKHJlY29yZCA9IHRoaXMuX2FkZGl0aW9uc0hlYWQ7IHJlY29yZCAhPSBudWxsOyByZWNvcmQgPSByZWNvcmQuX25leHRBZGRlZCkge1xuICAgICAgICByZWNvcmQucHJldmlvdXNWYWx1ZSA9IHJlY29yZC5jdXJyZW50VmFsdWU7XG4gICAgICB9XG5cbiAgICAgIHRoaXMuX2NoYW5nZXNIZWFkID0gdGhpcy5fY2hhbmdlc1RhaWwgPSBudWxsO1xuICAgICAgdGhpcy5fYWRkaXRpb25zSGVhZCA9IHRoaXMuX2FkZGl0aW9uc1RhaWwgPSBudWxsO1xuICAgICAgdGhpcy5fcmVtb3ZhbHNIZWFkID0gbnVsbDtcbiAgICB9XG4gIH1cblxuICAvLyBBZGQgdGhlIHJlY29yZCBvciBhIGdpdmVuIGtleSB0byB0aGUgbGlzdCBvZiBjaGFuZ2VzIG9ubHkgd2hlbiB0aGUgdmFsdWUgaGFzIGFjdHVhbGx5IGNoYW5nZWRcbiAgcHJpdmF0ZSBfbWF5YmVBZGRUb0NoYW5nZXMocmVjb3JkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj4sIG5ld1ZhbHVlOiBhbnkpOiB2b2lkIHtcbiAgICBpZiAoIWxvb3NlSWRlbnRpY2FsKG5ld1ZhbHVlLCByZWNvcmQuY3VycmVudFZhbHVlKSkge1xuICAgICAgcmVjb3JkLnByZXZpb3VzVmFsdWUgPSByZWNvcmQuY3VycmVudFZhbHVlO1xuICAgICAgcmVjb3JkLmN1cnJlbnRWYWx1ZSA9IG5ld1ZhbHVlO1xuICAgICAgdGhpcy5fYWRkVG9DaGFuZ2VzKHJlY29yZCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfYWRkVG9BZGRpdGlvbnMocmVjb3JkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj4pIHtcbiAgICBpZiAodGhpcy5fYWRkaXRpb25zSGVhZCA9PT0gbnVsbCkge1xuICAgICAgdGhpcy5fYWRkaXRpb25zSGVhZCA9IHRoaXMuX2FkZGl0aW9uc1RhaWwgPSByZWNvcmQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX2FkZGl0aW9uc1RhaWwgIS5fbmV4dEFkZGVkID0gcmVjb3JkO1xuICAgICAgdGhpcy5fYWRkaXRpb25zVGFpbCA9IHJlY29yZDtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9hZGRUb0NoYW5nZXMocmVjb3JkOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj4pIHtcbiAgICBpZiAodGhpcy5fY2hhbmdlc0hlYWQgPT09IG51bGwpIHtcbiAgICAgIHRoaXMuX2NoYW5nZXNIZWFkID0gdGhpcy5fY2hhbmdlc1RhaWwgPSByZWNvcmQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX2NoYW5nZXNUYWlsICEuX25leHRDaGFuZ2VkID0gcmVjb3JkO1xuICAgICAgdGhpcy5fY2hhbmdlc1RhaWwgPSByZWNvcmQ7XG4gICAgfVxuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBwcml2YXRlIF9mb3JFYWNoPEssIFY+KG9iajogTWFwPEssIFY+fHtbazogc3RyaW5nXTogVn0sIGZuOiAodjogViwgazogYW55KSA9PiB2b2lkKSB7XG4gICAgaWYgKG9iaiBpbnN0YW5jZW9mIE1hcCkge1xuICAgICAgb2JqLmZvckVhY2goZm4pO1xuICAgIH0gZWxzZSB7XG4gICAgICBPYmplY3Qua2V5cyhvYmopLmZvckVhY2goayA9PiBmbihvYmpba10sIGspKTtcbiAgICB9XG4gIH1cbn1cblxuY2xhc3MgS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+IGltcGxlbWVudHMgS2V5VmFsdWVDaGFuZ2VSZWNvcmQ8SywgVj4ge1xuICBwcmV2aW91c1ZhbHVlOiBWfG51bGwgPSBudWxsO1xuICBjdXJyZW50VmFsdWU6IFZ8bnVsbCA9IG51bGw7XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfbmV4dFByZXZpb3VzOiBLZXlWYWx1ZUNoYW5nZVJlY29yZF88SywgVj58bnVsbCA9IG51bGw7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX25leHQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsID0gbnVsbDtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcHJldjogS2V5VmFsdWVDaGFuZ2VSZWNvcmRfPEssIFY+fG51bGwgPSBudWxsO1xuICAvKiogQGludGVybmFsICovXG4gIF9uZXh0QWRkZWQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsID0gbnVsbDtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfbmV4dFJlbW92ZWQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsID0gbnVsbDtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfbmV4dENoYW5nZWQ6IEtleVZhbHVlQ2hhbmdlUmVjb3JkXzxLLCBWPnxudWxsID0gbnVsbDtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMga2V5OiBLKSB7fVxufVxuIl19