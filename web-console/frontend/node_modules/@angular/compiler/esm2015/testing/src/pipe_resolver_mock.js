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
import { PipeResolver } from '@angular/compiler';
export class MockPipeResolver extends PipeResolver {
    /**
     * @param {?} refector
     */
    constructor(refector) {
        super(refector);
        this._pipes = new Map();
    }
    /**
     * Overrides the {\@link Pipe} for a pipe.
     * @param {?} type
     * @param {?} metadata
     * @return {?}
     */
    setPipe(type, metadata) { this._pipes.set(type, metadata); }
    /**
     * Returns the {\@link Pipe} for a pipe:
     * - Set the {\@link Pipe} to the overridden view when it exists or fallback to the
     * default
     * `PipeResolver`, see `setPipe`.
     * @param {?} type
     * @param {?=} throwIfNotFound
     * @return {?}
     */
    resolve(type, throwIfNotFound = true) {
        /** @type {?} */
        let metadata = this._pipes.get(type);
        if (!metadata) {
            metadata = (/** @type {?} */ (super.resolve(type, throwIfNotFound)));
        }
        return metadata;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockPipeResolver.prototype._pipes;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZV9yZXNvbHZlcl9tb2NrLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvdGVzdGluZy9zcmMvcGlwZV9yZXNvbHZlcl9tb2NrLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFtQixZQUFZLEVBQU8sTUFBTSxtQkFBbUIsQ0FBQztBQUV2RSxNQUFNLE9BQU8sZ0JBQWlCLFNBQVEsWUFBWTs7OztJQUdoRCxZQUFZLFFBQTBCO1FBQUksS0FBSyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1FBRmxELFdBQU0sR0FBRyxJQUFJLEdBQUcsRUFBd0IsQ0FBQztJQUVVLENBQUM7Ozs7Ozs7SUFLNUQsT0FBTyxDQUFDLElBQWUsRUFBRSxRQUFtQixJQUFVLElBQUksQ0FBQyxNQUFNLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7Ozs7Ozs7SUFReEYsT0FBTyxDQUFDLElBQWUsRUFBRSxlQUFlLEdBQUcsSUFBSTs7WUFDekMsUUFBUSxHQUFHLElBQUksQ0FBQyxNQUFNLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQztRQUNwQyxJQUFJLENBQUMsUUFBUSxFQUFFO1lBQ2IsUUFBUSxHQUFHLG1CQUFBLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxFQUFFLGVBQWUsQ0FBQyxFQUFFLENBQUM7U0FDbkQ7UUFDRCxPQUFPLFFBQVEsQ0FBQztJQUNsQixDQUFDO0NBQ0Y7Ozs7OztJQXRCQyxrQ0FBaUQiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcGlsZVJlZmxlY3RvciwgUGlwZVJlc29sdmVyLCBjb3JlfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5cbmV4cG9ydCBjbGFzcyBNb2NrUGlwZVJlc29sdmVyIGV4dGVuZHMgUGlwZVJlc29sdmVyIHtcbiAgcHJpdmF0ZSBfcGlwZXMgPSBuZXcgTWFwPGNvcmUuVHlwZSwgY29yZS5QaXBlPigpO1xuXG4gIGNvbnN0cnVjdG9yKHJlZmVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7IHN1cGVyKHJlZmVjdG9yKTsgfVxuXG4gIC8qKlxuICAgKiBPdmVycmlkZXMgdGhlIHtAbGluayBQaXBlfSBmb3IgYSBwaXBlLlxuICAgKi9cbiAgc2V0UGlwZSh0eXBlOiBjb3JlLlR5cGUsIG1ldGFkYXRhOiBjb3JlLlBpcGUpOiB2b2lkIHsgdGhpcy5fcGlwZXMuc2V0KHR5cGUsIG1ldGFkYXRhKTsgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRoZSB7QGxpbmsgUGlwZX0gZm9yIGEgcGlwZTpcbiAgICogLSBTZXQgdGhlIHtAbGluayBQaXBlfSB0byB0aGUgb3ZlcnJpZGRlbiB2aWV3IHdoZW4gaXQgZXhpc3RzIG9yIGZhbGxiYWNrIHRvIHRoZVxuICAgKiBkZWZhdWx0XG4gICAqIGBQaXBlUmVzb2x2ZXJgLCBzZWUgYHNldFBpcGVgLlxuICAgKi9cbiAgcmVzb2x2ZSh0eXBlOiBjb3JlLlR5cGUsIHRocm93SWZOb3RGb3VuZCA9IHRydWUpOiBjb3JlLlBpcGUge1xuICAgIGxldCBtZXRhZGF0YSA9IHRoaXMuX3BpcGVzLmdldCh0eXBlKTtcbiAgICBpZiAoIW1ldGFkYXRhKSB7XG4gICAgICBtZXRhZGF0YSA9IHN1cGVyLnJlc29sdmUodHlwZSwgdGhyb3dJZk5vdEZvdW5kKSAhO1xuICAgIH1cbiAgICByZXR1cm4gbWV0YWRhdGE7XG4gIH1cbn1cbiJdfQ==