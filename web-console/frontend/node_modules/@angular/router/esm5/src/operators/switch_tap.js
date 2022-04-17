/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { from } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';
/**
 * Perform a side effect through a switchMap for every emission on the source Observable,
 * but return an Observable that is identical to the source. It's essentially the same as
 * the `tap` operator, but if the side effectful `next` function returns an ObservableInput,
 * it will wait before continuing with the original value.
 */
export function switchTap(next) {
    return function (source) {
        return source.pipe(switchMap(function (v) {
            var nextResult = next(v);
            if (nextResult) {
                return from(nextResult).pipe(map(function () { return v; }));
            }
            return from([v]);
        }));
    };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3dpdGNoX3RhcC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvb3BlcmF0b3JzL3N3aXRjaF90YXAudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxFQUE0QyxJQUFJLEVBQUMsTUFBTSxNQUFNLENBQUM7QUFDckUsT0FBTyxFQUFDLEdBQUcsRUFBRSxTQUFTLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUU5Qzs7Ozs7R0FLRztBQUNILE1BQU0sVUFBVSxTQUFTLENBQUksSUFBeUM7SUFFcEUsT0FBTyxVQUFTLE1BQU07UUFDcEIsT0FBTyxNQUFNLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxVQUFBLENBQUM7WUFDNUIsSUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDO1lBQzNCLElBQUksVUFBVSxFQUFFO2dCQUNkLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsY0FBTSxPQUFBLENBQUMsRUFBRCxDQUFDLENBQUMsQ0FBQyxDQUFDO2FBQzVDO1lBQ0QsT0FBTyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ25CLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDTixDQUFDLENBQUM7QUFDSixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge01vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbiwgT2JzZXJ2YWJsZUlucHV0LCBmcm9tfSBmcm9tICdyeGpzJztcbmltcG9ydCB7bWFwLCBzd2l0Y2hNYXB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcblxuLyoqXG4gKiBQZXJmb3JtIGEgc2lkZSBlZmZlY3QgdGhyb3VnaCBhIHN3aXRjaE1hcCBmb3IgZXZlcnkgZW1pc3Npb24gb24gdGhlIHNvdXJjZSBPYnNlcnZhYmxlLFxuICogYnV0IHJldHVybiBhbiBPYnNlcnZhYmxlIHRoYXQgaXMgaWRlbnRpY2FsIHRvIHRoZSBzb3VyY2UuIEl0J3MgZXNzZW50aWFsbHkgdGhlIHNhbWUgYXNcbiAqIHRoZSBgdGFwYCBvcGVyYXRvciwgYnV0IGlmIHRoZSBzaWRlIGVmZmVjdGZ1bCBgbmV4dGAgZnVuY3Rpb24gcmV0dXJucyBhbiBPYnNlcnZhYmxlSW5wdXQsXG4gKiBpdCB3aWxsIHdhaXQgYmVmb3JlIGNvbnRpbnVpbmcgd2l0aCB0aGUgb3JpZ2luYWwgdmFsdWUuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBzd2l0Y2hUYXA8VD4obmV4dDogKHg6IFQpID0+IHZvaWR8T2JzZXJ2YWJsZUlucHV0PGFueT4pOlxuICAgIE1vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbjxUPiB7XG4gIHJldHVybiBmdW5jdGlvbihzb3VyY2UpIHtcbiAgICByZXR1cm4gc291cmNlLnBpcGUoc3dpdGNoTWFwKHYgPT4ge1xuICAgICAgY29uc3QgbmV4dFJlc3VsdCA9IG5leHQodik7XG4gICAgICBpZiAobmV4dFJlc3VsdCkge1xuICAgICAgICByZXR1cm4gZnJvbShuZXh0UmVzdWx0KS5waXBlKG1hcCgoKSA9PiB2KSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gZnJvbShbdl0pO1xuICAgIH0pKTtcbiAgfTtcbn1cbiJdfQ==