/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * Represents a basic change from a previous to a new value for a single
 * property on a directive instance. Passed as a value in a
 * {@link SimpleChanges} object to the `ngOnChanges` hook.
 *
 * @see `OnChanges`
 *
 * @publicApi
 */
var SimpleChange = /** @class */ (function () {
    function SimpleChange(previousValue, currentValue, firstChange) {
        this.previousValue = previousValue;
        this.currentValue = currentValue;
        this.firstChange = firstChange;
    }
    /**
     * Check whether the new value is the first value assigned.
     */
    SimpleChange.prototype.isFirstChange = function () { return this.firstChange; };
    return SimpleChange;
}());
export { SimpleChange };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2ltcGxlX2NoYW5nZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2ludGVyZmFjZS9zaW1wbGVfY2hhbmdlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVIOzs7Ozs7OztHQVFHO0FBQ0g7SUFDRSxzQkFBbUIsYUFBa0IsRUFBUyxZQUFpQixFQUFTLFdBQW9CO1FBQXpFLGtCQUFhLEdBQWIsYUFBYSxDQUFLO1FBQVMsaUJBQVksR0FBWixZQUFZLENBQUs7UUFBUyxnQkFBVyxHQUFYLFdBQVcsQ0FBUztJQUFHLENBQUM7SUFDaEc7O09BRUc7SUFDSCxvQ0FBYSxHQUFiLGNBQTJCLE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDLENBQUM7SUFDdkQsbUJBQUM7QUFBRCxDQUFDLEFBTkQsSUFNQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBSZXByZXNlbnRzIGEgYmFzaWMgY2hhbmdlIGZyb20gYSBwcmV2aW91cyB0byBhIG5ldyB2YWx1ZSBmb3IgYSBzaW5nbGVcbiAqIHByb3BlcnR5IG9uIGEgZGlyZWN0aXZlIGluc3RhbmNlLiBQYXNzZWQgYXMgYSB2YWx1ZSBpbiBhXG4gKiB7QGxpbmsgU2ltcGxlQ2hhbmdlc30gb2JqZWN0IHRvIHRoZSBgbmdPbkNoYW5nZXNgIGhvb2suXG4gKlxuICogQHNlZSBgT25DaGFuZ2VzYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIFNpbXBsZUNoYW5nZSB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBwcmV2aW91c1ZhbHVlOiBhbnksIHB1YmxpYyBjdXJyZW50VmFsdWU6IGFueSwgcHVibGljIGZpcnN0Q2hhbmdlOiBib29sZWFuKSB7fVxuICAvKipcbiAgICogQ2hlY2sgd2hldGhlciB0aGUgbmV3IHZhbHVlIGlzIHRoZSBmaXJzdCB2YWx1ZSBhc3NpZ25lZC5cbiAgICovXG4gIGlzRmlyc3RDaGFuZ2UoKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLmZpcnN0Q2hhbmdlOyB9XG59XG5cbi8qKlxuICogQSBoYXNodGFibGUgb2YgY2hhbmdlcyByZXByZXNlbnRlZCBieSB7QGxpbmsgU2ltcGxlQ2hhbmdlfSBvYmplY3RzIHN0b3JlZFxuICogYXQgdGhlIGRlY2xhcmVkIHByb3BlcnR5IG5hbWUgdGhleSBiZWxvbmcgdG8gb24gYSBEaXJlY3RpdmUgb3IgQ29tcG9uZW50LiBUaGlzIGlzXG4gKiB0aGUgdHlwZSBwYXNzZWQgdG8gdGhlIGBuZ09uQ2hhbmdlc2AgaG9vay5cbiAqXG4gKiBAc2VlIGBPbkNoYW5nZXNgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgaW50ZXJmYWNlIFNpbXBsZUNoYW5nZXMgeyBbcHJvcE5hbWU6IHN0cmluZ106IFNpbXBsZUNoYW5nZTsgfVxuIl19