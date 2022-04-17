/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { getLView, getSelectedIndex } from '../state';
import { NO_CHANGE } from '../tokens';
import { interpolation1, interpolation2, interpolation3, interpolation4, interpolation5, interpolation6, interpolation7, interpolation8, interpolationV } from './interpolation';
import { textBindingInternal } from './shared';
/**
 *
 * Update text content with a lone bound value
 *
 * Used when a text node has 1 interpolated value in it, an no additional text
 * surrounds that interpolated value:
 *
 * ```html
 * <div>{{v0}}</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate(v0);
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate(v0) {
    ɵɵtextInterpolate1('', v0, '');
    return ɵɵtextInterpolate;
}
/**
 *
 * Update text content with single bound value surrounded by other text.
 *
 * Used when a text node has 1 interpolated value in it:
 *
 * ```html
 * <div>prefix{{v0}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate1('prefix', v0, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate1(prefix, v0, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation1(lView, prefix, v0, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate1;
}
/**
 *
 * Update text content with 2 bound values surrounded by other text.
 *
 * Used when a text node has 2 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate2('prefix', v0, '-', v1, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate2(prefix, v0, i0, v1, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation2(lView, prefix, v0, i0, v1, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate2;
}
/**
 *
 * Update text content with 3 bound values surrounded by other text.
 *
 * Used when a text node has 3 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate3(
 * 'prefix', v0, '-', v1, '-', v2, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate3(prefix, v0, i0, v1, i1, v2, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation3(lView, prefix, v0, i0, v1, i1, v2, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate3;
}
/**
 *
 * Update text content with 4 bound values surrounded by other text.
 *
 * Used when a text node has 4 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}-{{v3}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate4(
 * 'prefix', v0, '-', v1, '-', v2, '-', v3, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see ɵɵtextInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate4(prefix, v0, i0, v1, i1, v2, i2, v3, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation4(lView, prefix, v0, i0, v1, i1, v2, i2, v3, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate4;
}
/**
 *
 * Update text content with 5 bound values surrounded by other text.
 *
 * Used when a text node has 5 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}-{{v3}}-{{v4}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate5(
 * 'prefix', v0, '-', v1, '-', v2, '-', v3, '-', v4, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate5(prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation5(lView, prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate5;
}
/**
 *
 * Update text content with 6 bound values surrounded by other text.
 *
 * Used when a text node has 6 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}-{{v3}}-{{v4}}-{{v5}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate6(
 *    'prefix', v0, '-', v1, '-', v2, '-', v3, '-', v4, '-', v5, 'suffix');
 * ```
 *
 * @param i4 Static value used for concatenation only.
 * @param v5 Value checked for change. @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate6(prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, i4, v5, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation6(lView, prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, i4, v5, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate6;
}
/**
 *
 * Update text content with 7 bound values surrounded by other text.
 *
 * Used when a text node has 7 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}-{{v3}}-{{v4}}-{{v5}}-{{v6}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate7(
 *    'prefix', v0, '-', v1, '-', v2, '-', v3, '-', v4, '-', v5, '-', v6, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate7(prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, i4, v5, i5, v6, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation7(lView, prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, i4, v5, i5, v6, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate7;
}
/**
 *
 * Update text content with 8 bound values surrounded by other text.
 *
 * Used when a text node has 8 interpolated values in it:
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}-{{v3}}-{{v4}}-{{v5}}-{{v6}}-{{v7}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolate8(
 *  'prefix', v0, '-', v1, '-', v2, '-', v3, '-', v4, '-', v5, '-', v6, '-', v7, 'suffix');
 * ```
 * @returns itself, so that it may be chained.
 * @see textInterpolateV
 * @codeGenApi
 */
export function ɵɵtextInterpolate8(prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, i4, v5, i5, v6, i6, v7, suffix) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolation8(lView, prefix, v0, i0, v1, i1, v2, i2, v3, i3, v4, i4, v5, i5, v6, i6, v7, suffix);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolate8;
}
/**
 * Update text content with 9 or more bound values other surrounded by text.
 *
 * Used when the number of interpolated values exceeds 8.
 *
 * ```html
 * <div>prefix{{v0}}-{{v1}}-{{v2}}-{{v3}}-{{v4}}-{{v5}}-{{v6}}-{{v7}}-{{v8}}-{{v9}}suffix</div>
 * ```
 *
 * Its compiled representation is:
 *
 * ```ts
 * ɵɵtextInterpolateV(
 *  ['prefix', v0, '-', v1, '-', v2, '-', v3, '-', v4, '-', v5, '-', v6, '-', v7, '-', v9,
 *  'suffix']);
 * ```
 *.
 * @param values The a collection of values and the strings in between those values, beginning with
 * a string prefix and ending with a string suffix.
 * (e.g. `['prefix', value0, '-', value1, '-', value2, ..., value99, 'suffix']`)
 *
 * @returns itself, so that it may be chained.
 * @codeGenApi
 */
export function ɵɵtextInterpolateV(values) {
    var index = getSelectedIndex();
    var lView = getLView();
    var interpolated = interpolationV(lView, values);
    if (interpolated !== NO_CHANGE) {
        textBindingInternal(lView, index, interpolated);
    }
    return ɵɵtextInterpolateV;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGV4dF9pbnRlcnBvbGF0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9pbnN0cnVjdGlvbnMvdGV4dF9pbnRlcnBvbGF0aW9uLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUNILE9BQU8sRUFBQyxRQUFRLEVBQUUsZ0JBQWdCLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFDcEQsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLFdBQVcsQ0FBQztBQUVwQyxPQUFPLEVBQUMsY0FBYyxFQUFFLGNBQWMsRUFBRSxjQUFjLEVBQUUsY0FBYyxFQUFFLGNBQWMsRUFBRSxjQUFjLEVBQUUsY0FBYyxFQUFFLGNBQWMsRUFBRSxjQUFjLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUMvSyxPQUFPLEVBQW1CLG1CQUFtQixFQUFDLE1BQU0sVUFBVSxDQUFDO0FBRy9EOzs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBbUJHO0FBQ0gsTUFBTSxVQUFVLGlCQUFpQixDQUFDLEVBQU87SUFDdkMsa0JBQWtCLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUMsQ0FBQztJQUMvQixPQUFPLGlCQUFpQixDQUFDO0FBQzNCLENBQUM7QUFHRDs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBa0JHO0FBQ0gsTUFBTSxVQUFVLGtCQUFrQixDQUFDLE1BQWMsRUFBRSxFQUFPLEVBQUUsTUFBYztJQUN4RSxJQUFNLEtBQUssR0FBRyxnQkFBZ0IsRUFBRSxDQUFDO0lBQ2pDLElBQU0sS0FBSyxHQUFHLFFBQVEsRUFBRSxDQUFDO0lBQ3pCLElBQU0sWUFBWSxHQUFHLGNBQWMsQ0FBQyxLQUFLLEVBQUUsTUFBTSxFQUFFLEVBQUUsRUFBRSxNQUFNLENBQUMsQ0FBQztJQUMvRCxJQUFJLFlBQVksS0FBSyxTQUFTLEVBQUU7UUFDOUIsbUJBQW1CLENBQUMsS0FBSyxF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QzlCLG1CQUFtQixDQUFDLEtBQUssRUFBRSxLQUFLLEVBQUUsWUFBc0IsQ0FBQyxDQUFDO0tBQzNEO0lBQ0QsT0FBTyxrQkFBa0IsQ0FBQztBQUM1QixDQUFDO0FBRUQ7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBdUJHO0FBQ0gsTUFBTSxVQUFVLGtCQUFrQixDQUFDLE1BQWE7SUFDOUMsSUFBTSxLQUFLLEdBQUcsZ0JBQWdCLEVBQUUsQ0FBQztJQUNqQyxJQUFNLEtBQUssR0FBRyxRQUFRLEVBQUUsQ0FBQztJQUN6QixJQUFNLFlBQVksR0FBRyxjQUFjLENBQUMsS0FBSyxFQUFFLE1BQU0sQ0FBQyxDQUFDO0lBQ25ELElBQUksWUFBWSxLQUFLLFNBQVMsRUFBRTtRQUM5QixtQkFBbUIsQ0FBQyxLQUFLLEVBQUUsS0FBSyxFQUFFLFlBQXNCLENBQUMsQ0FBQztLQUMzRDtJQUNELE9BQU8sa0JBQWtCLENBQUM7QUFDNUIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cbmltcG9ydCB7Z2V0TFZpZXcsIGdldFNlbGVjdGVkSW5kZXh9IGZyb20gJy4uL3N0YXRlJztcbmltcG9ydCB7Tk9fQ0hBTkdFfSBmcm9tICcuLi90b2tlbnMnO1xuXG5pbXBvcnQge2ludGVycG9sYXRpb24xLCBpbnRlcnBvbGF0aW9uMiwgaW50ZXJwb2xhdGlvbjMsIGludGVycG9sYXRpb240LCBpbnRlcnBvbGF0aW9uNSwgaW50ZXJwb2xhdGlvbjYsIGludGVycG9sYXRpb243LCBpbnRlcnBvbGF0aW9uOCwgaW50ZXJwb2xhdGlvblZ9IGZyb20gJy4vaW50ZXJwb2xhdGlvbic7XG5pbXBvcnQge1RzaWNrbGVJc3N1ZTEwMDksIHRleHRCaW5kaW5nSW50ZXJuYWx9IGZyb20gJy4vc2hhcmVkJztcblxuXG4vKipcbiAqXG4gKiBVcGRhdGUgdGV4dCBjb250ZW50IHdpdGggYSBsb25lIGJvdW5kIHZhbHVlXG4gKlxuICogVXNlZCB3aGVuIGEgdGV4dCBub2RlIGhhcyAxIGludGVycG9sYXRlZCB2YWx1ZSBpbiBpdCwgYW4gbm8gYWRkaXRpb25hbCB0ZXh0XG4gKiBzdXJyb3VuZHMgdGhhdCBpbnRlcnBvbGF0ZWQgdmFsdWU6XG4gKlxuICogYGBgaHRtbFxuICogPGRpdj57e3YwfX08L2Rpdj5cbiAqIGBgYFxuICpcbiAqIEl0cyBjb21waWxlZCByZXByZXNlbnRhdGlvbiBpczpcbiAqXG4gKiBgYGB0c1xuICogybXJtXRleHRJbnRlcnBvbGF0ZSh2MCk7XG4gKiBgYGBcbiAqIEByZXR1cm5zIGl0c2VsZiwgc28gdGhhdCBpdCBtYXkgYmUgY2hhaW5lZC5cbiAqIEBzZWUgdGV4dEludGVycG9sYXRlVlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybV0ZXh0SW50ZXJwb2xhdGUodjA6IGFueSk6IFRzaWNrbGVJc3N1ZTEwMDkge1xuICDJtcm1dGV4dEludGVycG9sYXRlMSgnJywgdjAsICcnKTtcbiAgcmV0dXJuIMm1ybV0ZXh0SW50ZXJwb2xhdGU7XG59XG5cblxuLyoqXG4gKlxuICogVXBkYXRlIHRleHQgY29udGVudCB3aXRoIHNpbmdsZSBib3VuZCB2YWx1ZSBzdXJyb3VuZGVkIGJ5IG90aGVyIHRleHQuXG4gKlxuICogVXNlZCB3aGVuIGEgdGV4dCBub2RlIGhhcyAxIGludGVycG9sYXRlZCB2YWx1ZSBpbiBpdDpcbiAqXG4gKiBgYGBodG1sXG4gKiA8ZGl2PnByZWZpeHt7djB9fXN1ZmZpeDwvZGl2PlxuICogYGBgXG4gKlxuICogSXRzIGNvbXBpbGVkIHJlcHJlc2VudGF0aW9uIGlzOlxuICpcbiAqIGBgYHRzXG4gKiDJtcm1dGV4dEludGVycG9sYXRlMSgncHJlZml4JywgdjAsICdzdWZmaXgnKTtcbiAqIGBgYFxuICogQHJldHVybnMgaXRzZWxmLCBzbyB0aGF0IGl0IG1heSBiZSBjaGFpbmVkLlxuICogQHNlZSB0ZXh0SW50ZXJwb2xhdGVWXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXRleHRJbnRlcnBvbGF0ZTEocHJlZml4OiBzdHJpbmcsIHYwOiBhbnksIHN1ZmZpeDogc3RyaW5nKTogVHNpY2tsZUlzc3VlMTAwOSB7XG4gIGNvbnN0IGluZGV4ID0gZ2V0U2VsZWN0ZWRJbmRleCgpO1xuICBjb25zdCBsVmlldyA9IGdldExWaWV3KCk7XG4gIGNvbnN0IGludGVycG9sYXRlZCA9IGludGVycG9sYXRpb24xKGxWaWV3LCBwcmVmaXgsIHYwLCBzdWZmaXgpO1xuICBpZiAoaW50ZXJwb2xhdGVkICE9PSBOT19DSEFOR0UpIHtcbiAgICB0ZXh0QmluZGluZ0ludGVybmFsKGxWaWV3LCBpbmRleCwgaW50ZXJwb2xhdGVkIGFzIHN0cmluZyk7XG4gIH1cbiAgcmV0dXJuIMm1ybV0ZXh0SW50ZXJwb2xhdGUxO1xufVxuXG4vKipcbiAqXG4gKiBVcGRhdGUgdGV4dCBjb250ZW50IHdpdGggMiBib3VuZCB2YWx1ZXMgc3Vycm91bmRlZCBieSBvdGhlciB0ZXh0LlxuICpcbiAqIFVzZWQgd2hlbiBhIHRleHQgbm9kZSBoYXMgMiBpbnRlcnBvbGF0ZWQgdmFsdWVzIGluIGl0OlxuICpcbiAqIGBgYGh0bWxcbiAqIDxkaXY+cHJlZml4e3t2MH19LXt7djF9fXN1ZmZpeDwvZGl2PlxuICogYGBgXG4gKlxuICogSXRzIGNvbXBpbGVkIHJlcHJlc2VudGF0aW9uIGlzOlxuICpcbiAqIGBgYHRzXG4gKiDJtcm1dGV4dEludGVycG9sYXRlMigncHJlZml4JywgdjAsICctJywgdjEsICdzdWZmaXgnKTtcbiAqIGBgYFxuICogQHJldHVybnMgaXRzZWxmLCBzbyB0aGF0IGl0IG1heSBiZSBjaGFpbmVkLlxuICogQHNlZSB0ZXh0SW50ZXJwb2xhdGVWXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXRleHRJbnRlcnBvbGF0ZTIoXG4gICAgcHJlZml4OiBzdHJpbmcsIHYwOiBhbnksIGkwOiBzdHJpbmcsIHYxOiBhbnksIHN1ZmZpeDogc3RyaW5nKTogVHNpY2tsZUlzc3VlMTAwOSB7XG4gIGNvbnN0IGluZGV4ID0gZ2V0U2VsZWN0ZWRJbmRleCgpO1xuICBjb25zdCBsVmlldyA9IGdldExWaWV3KCk7XG4gIGNvbnN0IGludGVycG9sYXRlZCA9IGludGVycG9sYXRpb24yKGxWaWV3LCBwcmVmaXgsIHYwLCBpMCwgdjEsIHN1ZmZpeCk7XG4gIGlmIChpbnRlcnBvbGF0ZWQgIT09IE5PX0NIQU5HRSkge1xuICAgIHRleHRCaW5kaW5nSW50ZXJuYWwobFZpZXcsIGluZGV4LCBpbnRlcnBvbGF0ZWQgYXMgc3RyaW5nKTtcbiAgfVxuICByZXR1cm4gybXJtXRleHRJbnRlcnBvbGF0ZTI7XG59XG5cbi8qKlxuICpcbiAqIFVwZGF0ZSB0ZXh0IGNvbnRlbnQgd2l0aCAzIGJvdW5kIHZhbHVlcyBzdXJyb3VuZGVkIGJ5IG90aGVyIHRleHQuXG4gKlxuICogVXNlZCB3aGVuIGEgdGV4dCBub2RlIGhhcyAzIGludGVycG9sYXRlZCB2YWx1ZXMgaW4gaXQ6XG4gKlxuICogYGBgaHRtbFxuICogPGRpdj5wcmVmaXh7e3YwfX0te3t2MX19LXt7djJ9fXN1ZmZpeDwvZGl2PlxuICogYGBgXG4gKlxuICogSXRzIGNvbXBpbGVkIHJlcHJlc2VudGF0aW9uIGlzOlxuICpcbiAqIGBgYHRzXG4gKiDJtcm1dGV4dEludGVycG9sYXRlMyhcbiAqICdwcmVmaXgnLCB2MCwgJy0nLCB2MSwgJy0nLCB2MiwgJ3N1ZmZpeCcpO1xuICogYGBgXG4gKiBAcmV0dXJucyBpdHNlbGYsIHNvIHRoYXQgaXQgbWF5IGJlIGNoYWluZWQuXG4gKiBAc2VlIHRleHRJbnRlcnBvbGF0ZVZcbiAqIEBjb2RlR2VuQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1dGV4dEludGVycG9sYXRlMyhcbiAgICBwcmVmaXg6IHN0cmluZywgdjA6IGFueSwgaTA6IHN0cmluZywgdjE6IGFueSwgaTE6IHN0cmluZywgdjI6IGFueSxcbiAgICBzdWZmaXg6IHN0cmluZyk6IFRzaWNrbGVJc3N1ZTEwMDkge1xuICBjb25zdCBpbmRleCA9IGdldFNlbGVjdGVkSW5kZXgoKTtcbiAgY29uc3QgbFZpZXcgPSBnZXRMVmlldygpO1xuICBjb25zdCBpbnRlcnBvbGF0ZWQgPSBpbnRlcnBvbGF0aW9uMyhsVmlldywgcHJlZml4LCB2MCwgaTAsIHYxLCBpMSwgdjIsIHN1ZmZpeCk7XG4gIGlmIChpbnRlcnBvbGF0ZWQgIT09IE5PX0NIQU5HRSkge1xuICAgIHRleHRCaW5kaW5nSW50ZXJuYWwobFZpZXcsIGluZGV4LCBpbnRlcnBvbGF0ZWQgYXMgc3RyaW5nKTtcbiAgfVxuICByZXR1cm4gybXJtXRleHRJbnRlcnBvbGF0ZTM7XG59XG5cbi8qKlxuICpcbiAqIFVwZGF0ZSB0ZXh0IGNvbnRlbnQgd2l0aCA0IGJvdW5kIHZhbHVlcyBzdXJyb3VuZGVkIGJ5IG90aGVyIHRleHQuXG4gKlxuICogVXNlZCB3aGVuIGEgdGV4dCBub2RlIGhhcyA0IGludGVycG9sYXRlZCB2YWx1ZXMgaW4gaXQ6XG4gKlxuICogYGBgaHRtbFxuICogPGRpdj5wcmVmaXh7e3YwfX0te3t2MX19LXt7djJ9fS17e3YzfX1zdWZmaXg8L2Rpdj5cbiAqIGBgYFxuICpcbiAqIEl0cyBjb21waWxlZCByZXByZXNlbnRhdGlvbiBpczpcbiAqXG4gKiBgYGB0c1xuICogybXJtXRleHRJbnRlcnBvbGF0ZTQoXG4gKiAncHJlZml4JywgdjAsICctJywgdjEsICctJywgdjIsICctJywgdjMsICdzdWZmaXgnKTtcbiAqIGBgYFxuICogQHJldHVybnMgaXRzZWxmLCBzbyB0aGF0IGl0IG1heSBiZSBjaGFpbmVkLlxuICogQHNlZSDJtcm1dGV4dEludGVycG9sYXRlVlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybV0ZXh0SW50ZXJwb2xhdGU0KFxuICAgIHByZWZpeDogc3RyaW5nLCB2MDogYW55LCBpMDogc3RyaW5nLCB2MTogYW55LCBpMTogc3RyaW5nLCB2MjogYW55LCBpMjogc3RyaW5nLCB2MzogYW55LFxuICAgIHN1ZmZpeDogc3RyaW5nKTogVHNpY2tsZUlzc3VlMTAwOSB7XG4gIGNvbnN0IGluZGV4ID0gZ2V0U2VsZWN0ZWRJbmRleCgpO1xuICBjb25zdCBsVmlldyA9IGdldExWaWV3KCk7XG4gIGNvbnN0IGludGVycG9sYXRlZCA9IGludGVycG9sYXRpb240KGxWaWV3LCBwcmVmaXgsIHYwLCBpMCwgdjEsIGkxLCB2MiwgaTIsIHYzLCBzdWZmaXgpO1xuICBpZiAoaW50ZXJwb2xhdGVkICE9PSBOT19DSEFOR0UpIHtcbiAgICB0ZXh0QmluZGluZ0ludGVybmFsKGxWaWV3LCBpbmRleCwgaW50ZXJwb2xhdGVkIGFzIHN0cmluZyk7XG4gIH1cbiAgcmV0dXJuIMm1ybV0ZXh0SW50ZXJwb2xhdGU0O1xufVxuXG4vKipcbiAqXG4gKiBVcGRhdGUgdGV4dCBjb250ZW50IHdpdGggNSBib3VuZCB2YWx1ZXMgc3Vycm91bmRlZCBieSBvdGhlciB0ZXh0LlxuICpcbiAqIFVzZWQgd2hlbiBhIHRleHQgbm9kZSBoYXMgNSBpbnRlcnBvbGF0ZWQgdmFsdWVzIGluIGl0OlxuICpcbiAqIGBgYGh0bWxcbiAqIDxkaXY+cHJlZml4e3t2MH19LXt7djF9fS17e3YyfX0te3t2M319LXt7djR9fXN1ZmZpeDwvZGl2PlxuICogYGBgXG4gKlxuICogSXRzIGNvbXBpbGVkIHJlcHJlc2VudGF0aW9uIGlzOlxuICpcbiAqIGBgYHRzXG4gKiDJtcm1dGV4dEludGVycG9sYXRlNShcbiAqICdwcmVmaXgnLCB2MCwgJy0nLCB2MSwgJy0nLCB2MiwgJy0nLCB2MywgJy0nLCB2NCwgJ3N1ZmZpeCcpO1xuICogYGBgXG4gKiBAcmV0dXJucyBpdHNlbGYsIHNvIHRoYXQgaXQgbWF5IGJlIGNoYWluZWQuXG4gKiBAc2VlIHRleHRJbnRlcnBvbGF0ZVZcbiAqIEBjb2RlR2VuQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1dGV4dEludGVycG9sYXRlNShcbiAgICBwcmVmaXg6IHN0cmluZywgdjA6IGFueSwgaTA6IHN0cmluZywgdjE6IGFueSwgaTE6IHN0cmluZywgdjI6IGFueSwgaTI6IHN0cmluZywgdjM6IGFueSxcbiAgICBpMzogc3RyaW5nLCB2NDogYW55LCBzdWZmaXg6IHN0cmluZyk6IFRzaWNrbGVJc3N1ZTEwMDkge1xuICBjb25zdCBpbmRleCA9IGdldFNlbGVjdGVkSW5kZXgoKTtcbiAgY29uc3QgbFZpZXcgPSBnZXRMVmlldygpO1xuICBjb25zdCBpbnRlcnBvbGF0ZWQgPSBpbnRlcnBvbGF0aW9uNShsVmlldywgcHJlZml4LCB2MCwgaTAsIHYxLCBpMSwgdjIsIGkyLCB2MywgaTMsIHY0LCBzdWZmaXgpO1xuICBpZiAoaW50ZXJwb2xhdGVkICE9PSBOT19DSEFOR0UpIHtcbiAgICB0ZXh0QmluZGluZ0ludGVybmFsKGxWaWV3LCBpbmRleCwgaW50ZXJwb2xhdGVkIGFzIHN0cmluZyk7XG4gIH1cbiAgcmV0dXJuIMm1ybV0ZXh0SW50ZXJwb2xhdGU1O1xufVxuXG4vKipcbiAqXG4gKiBVcGRhdGUgdGV4dCBjb250ZW50IHdpdGggNiBib3VuZCB2YWx1ZXMgc3Vycm91bmRlZCBieSBvdGhlciB0ZXh0LlxuICpcbiAqIFVzZWQgd2hlbiBhIHRleHQgbm9kZSBoYXMgNiBpbnRlcnBvbGF0ZWQgdmFsdWVzIGluIGl0OlxuICpcbiAqIGBgYGh0bWxcbiAqIDxkaXY+cHJlZml4e3t2MH19LXt7djF9fS17e3YyfX0te3t2M319LXt7djR9fS17e3Y1fX1zdWZmaXg8L2Rpdj5cbiAqIGBgYFxuICpcbiAqIEl0cyBjb21waWxlZCByZXByZXNlbnRhdGlvbiBpczpcbiAqXG4gKiBgYGB0c1xuICogybXJtXRleHRJbnRlcnBvbGF0ZTYoXG4gKiAgICAncHJlZml4JywgdjAsICctJywgdjEsICctJywgdjIsICctJywgdjMsICctJywgdjQsICctJywgdjUsICdzdWZmaXgnKTtcbiAqIGBgYFxuICpcbiAqIEBwYXJhbSBpNCBTdGF0aWMgdmFsdWUgdXNlZCBmb3IgY29uY2F0ZW5hdGlvbiBvbmx5LlxuICogQHBhcmFtIHY1IFZhbHVlIGNoZWNrZWQgZm9yIGNoYW5nZS4gQHJldHVybnMgaXRzZWxmLCBzbyB0aGF0IGl0IG1heSBiZSBjaGFpbmVkLlxuICogQHNlZSB0ZXh0SW50ZXJwb2xhdGVWXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXRleHRJbnRlcnBvbGF0ZTYoXG4gICAgcHJlZml4OiBzdHJpbmcsIHYwOiBhbnksIGkwOiBzdHJpbmcsIHYxOiBhbnksIGkxOiBzdHJpbmcsIHYyOiBhbnksIGkyOiBzdHJpbmcsIHYzOiBhbnksXG4gICAgaTM6IHN0cmluZywgdjQ6IGFueSwgaTQ6IHN0cmluZywgdjU6IGFueSwgc3VmZml4OiBzdHJpbmcpOiBUc2lja2xlSXNzdWUxMDA5IHtcbiAgY29uc3QgaW5kZXggPSBnZXRTZWxlY3RlZEluZGV4KCk7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgY29uc3QgaW50ZXJwb2xhdGVkID1cbiAgICAgIGludGVycG9sYXRpb242KGxWaWV3LCBwcmVmaXgsIHYwLCBpMCwgdjEsIGkxLCB2MiwgaTIsIHYzLCBpMywgdjQsIGk0LCB2NSwgc3VmZml4KTtcbiAgaWYgKGludGVycG9sYXRlZCAhPT0gTk9fQ0hBTkdFKSB7XG4gICAgdGV4dEJpbmRpbmdJbnRlcm5hbChsVmlldywgaW5kZXgsIGludGVycG9sYXRlZCBhcyBzdHJpbmcpO1xuICB9XG4gIHJldHVybiDJtcm1dGV4dEludGVycG9sYXRlNjtcbn1cblxuLyoqXG4gKlxuICogVXBkYXRlIHRleHQgY29udGVudCB3aXRoIDcgYm91bmQgdmFsdWVzIHN1cnJvdW5kZWQgYnkgb3RoZXIgdGV4dC5cbiAqXG4gKiBVc2VkIHdoZW4gYSB0ZXh0IG5vZGUgaGFzIDcgaW50ZXJwb2xhdGVkIHZhbHVlcyBpbiBpdDpcbiAqXG4gKiBgYGBodG1sXG4gKiA8ZGl2PnByZWZpeHt7djB9fS17e3YxfX0te3t2Mn19LXt7djN9fS17e3Y0fX0te3t2NX19LXt7djZ9fXN1ZmZpeDwvZGl2PlxuICogYGBgXG4gKlxuICogSXRzIGNvbXBpbGVkIHJlcHJlc2VudGF0aW9uIGlzOlxuICpcbiAqIGBgYHRzXG4gKiDJtcm1dGV4dEludGVycG9sYXRlNyhcbiAqICAgICdwcmVmaXgnLCB2MCwgJy0nLCB2MSwgJy0nLCB2MiwgJy0nLCB2MywgJy0nLCB2NCwgJy0nLCB2NSwgJy0nLCB2NiwgJ3N1ZmZpeCcpO1xuICogYGBgXG4gKiBAcmV0dXJucyBpdHNlbGYsIHNvIHRoYXQgaXQgbWF5IGJlIGNoYWluZWQuXG4gKiBAc2VlIHRleHRJbnRlcnBvbGF0ZVZcbiAqIEBjb2RlR2VuQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1dGV4dEludGVycG9sYXRlNyhcbiAgICBwcmVmaXg6IHN0cmluZywgdjA6IGFueSwgaTA6IHN0cmluZywgdjE6IGFueSwgaTE6IHN0cmluZywgdjI6IGFueSwgaTI6IHN0cmluZywgdjM6IGFueSxcbiAgICBpMzogc3RyaW5nLCB2NDogYW55LCBpNDogc3RyaW5nLCB2NTogYW55LCBpNTogc3RyaW5nLCB2NjogYW55LFxuICAgIHN1ZmZpeDogc3RyaW5nKTogVHNpY2tsZUlzc3VlMTAwOSB7XG4gIGNvbnN0IGluZGV4ID0gZ2V0U2VsZWN0ZWRJbmRleCgpO1xuICBjb25zdCBsVmlldyA9IGdldExWaWV3KCk7XG4gIGNvbnN0IGludGVycG9sYXRlZCA9XG4gICAgICBpbnRlcnBvbGF0aW9uNyhsVmlldywgcHJlZml4LCB2MCwgaTAsIHYxLCBpMSwgdjIsIGkyLCB2MywgaTMsIHY0LCBpNCwgdjUsIGk1LCB2Niwgc3VmZml4KTtcbiAgaWYgKGludGVycG9sYXRlZCAhPT0gTk9fQ0hBTkdFKSB7XG4gICAgdGV4dEJpbmRpbmdJbnRlcm5hbChsVmlldywgaW5kZXgsIGludGVycG9sYXRlZCBhcyBzdHJpbmcpO1xuICB9XG4gIHJldHVybiDJtcm1dGV4dEludGVycG9sYXRlNztcbn1cblxuLyoqXG4gKlxuICogVXBkYXRlIHRleHQgY29udGVudCB3aXRoIDggYm91bmQgdmFsdWVzIHN1cnJvdW5kZWQgYnkgb3RoZXIgdGV4dC5cbiAqXG4gKiBVc2VkIHdoZW4gYSB0ZXh0IG5vZGUgaGFzIDggaW50ZXJwb2xhdGVkIHZhbHVlcyBpbiBpdDpcbiAqXG4gKiBgYGBodG1sXG4gKiA8ZGl2PnByZWZpeHt7djB9fS17e3YxfX0te3t2Mn19LXt7djN9fS17e3Y0fX0te3t2NX19LXt7djZ9fS17e3Y3fX1zdWZmaXg8L2Rpdj5cbiAqIGBgYFxuICpcbiAqIEl0cyBjb21waWxlZCByZXByZXNlbnRhdGlvbiBpczpcbiAqXG4gKiBgYGB0c1xuICogybXJtXRleHRJbnRlcnBvbGF0ZTgoXG4gKiAgJ3ByZWZpeCcsIHYwLCAnLScsIHYxLCAnLScsIHYyLCAnLScsIHYzLCAnLScsIHY0LCAnLScsIHY1LCAnLScsIHY2LCAnLScsIHY3LCAnc3VmZml4Jyk7XG4gKiBgYGBcbiAqIEByZXR1cm5zIGl0c2VsZiwgc28gdGhhdCBpdCBtYXkgYmUgY2hhaW5lZC5cbiAqIEBzZWUgdGV4dEludGVycG9sYXRlVlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybV0ZXh0SW50ZXJwb2xhdGU4KFxuICAgIHByZWZpeDogc3RyaW5nLCB2MDogYW55LCBpMDogc3RyaW5nLCB2MTogYW55LCBpMTogc3RyaW5nLCB2MjogYW55LCBpMjogc3RyaW5nLCB2MzogYW55LFxuICAgIGkzOiBzdHJpbmcsIHY0OiBhbnksIGk0OiBzdHJpbmcsIHY1OiBhbnksIGk1OiBzdHJpbmcsIHY2OiBhbnksIGk2OiBzdHJpbmcsIHY3OiBhbnksXG4gICAgc3VmZml4OiBzdHJpbmcpOiBUc2lja2xlSXNzdWUxMDA5IHtcbiAgY29uc3QgaW5kZXggPSBnZXRTZWxlY3RlZEluZGV4KCk7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgY29uc3QgaW50ZXJwb2xhdGVkID0gaW50ZXJwb2xhdGlvbjgoXG4gICAgICBsVmlldywgcHJlZml4LCB2MCwgaTAsIHYxLCBpMSwgdjIsIGkyLCB2MywgaTMsIHY0LCBpNCwgdjUsIGk1LCB2NiwgaTYsIHY3LCBzdWZmaXgpO1xuICBpZiAoaW50ZXJwb2xhdGVkICE9PSBOT19DSEFOR0UpIHtcbiAgICB0ZXh0QmluZGluZ0ludGVybmFsKGxWaWV3LCBpbmRleCwgaW50ZXJwb2xhdGVkIGFzIHN0cmluZyk7XG4gIH1cbiAgcmV0dXJuIMm1ybV0ZXh0SW50ZXJwb2xhdGU4O1xufVxuXG4vKipcbiAqIFVwZGF0ZSB0ZXh0IGNvbnRlbnQgd2l0aCA5IG9yIG1vcmUgYm91bmQgdmFsdWVzIG90aGVyIHN1cnJvdW5kZWQgYnkgdGV4dC5cbiAqXG4gKiBVc2VkIHdoZW4gdGhlIG51bWJlciBvZiBpbnRlcnBvbGF0ZWQgdmFsdWVzIGV4Y2VlZHMgOC5cbiAqXG4gKiBgYGBodG1sXG4gKiA8ZGl2PnByZWZpeHt7djB9fS17e3YxfX0te3t2Mn19LXt7djN9fS17e3Y0fX0te3t2NX19LXt7djZ9fS17e3Y3fX0te3t2OH19LXt7djl9fXN1ZmZpeDwvZGl2PlxuICogYGBgXG4gKlxuICogSXRzIGNvbXBpbGVkIHJlcHJlc2VudGF0aW9uIGlzOlxuICpcbiAqIGBgYHRzXG4gKiDJtcm1dGV4dEludGVycG9sYXRlVihcbiAqICBbJ3ByZWZpeCcsIHYwLCAnLScsIHYxLCAnLScsIHYyLCAnLScsIHYzLCAnLScsIHY0LCAnLScsIHY1LCAnLScsIHY2LCAnLScsIHY3LCAnLScsIHY5LFxuICogICdzdWZmaXgnXSk7XG4gKiBgYGBcbiAqLlxuICogQHBhcmFtIHZhbHVlcyBUaGUgYSBjb2xsZWN0aW9uIG9mIHZhbHVlcyBhbmQgdGhlIHN0cmluZ3MgaW4gYmV0d2VlbiB0aG9zZSB2YWx1ZXMsIGJlZ2lubmluZyB3aXRoXG4gKiBhIHN0cmluZyBwcmVmaXggYW5kIGVuZGluZyB3aXRoIGEgc3RyaW5nIHN1ZmZpeC5cbiAqIChlLmcuIGBbJ3ByZWZpeCcsIHZhbHVlMCwgJy0nLCB2YWx1ZTEsICctJywgdmFsdWUyLCAuLi4sIHZhbHVlOTksICdzdWZmaXgnXWApXG4gKlxuICogQHJldHVybnMgaXRzZWxmLCBzbyB0aGF0IGl0IG1heSBiZSBjaGFpbmVkLlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybV0ZXh0SW50ZXJwb2xhdGVWKHZhbHVlczogYW55W10pOiBUc2lja2xlSXNzdWUxMDA5IHtcbiAgY29uc3QgaW5kZXggPSBnZXRTZWxlY3RlZEluZGV4KCk7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgY29uc3QgaW50ZXJwb2xhdGVkID0gaW50ZXJwb2xhdGlvblYobFZpZXcsIHZhbHVlcyk7XG4gIGlmIChpbnRlcnBvbGF0ZWQgIT09IE5PX0NIQU5HRSkge1xuICAgIHRleHRCaW5kaW5nSW50ZXJuYWwobFZpZXcsIGluZGV4LCBpbnRlcnBvbGF0ZWQgYXMgc3RyaW5nKTtcbiAgfVxuICByZXR1cm4gybXJtXRleHRJbnRlcnBvbGF0ZVY7XG59XG4iXX0=