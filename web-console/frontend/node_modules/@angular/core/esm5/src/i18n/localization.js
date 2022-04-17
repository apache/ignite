/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { getLocalePluralCase } from './locale_data_api';
/**
 * Returns the plural case based on the locale
 */
export function getPluralCase(value, locale) {
    var plural = getLocalePluralCase(locale)(value);
    switch (plural) {
        case 0:
            return 'zero';
        case 1:
            return 'one';
        case 2:
            return 'two';
        case 3:
            return 'few';
        case 4:
            return 'many';
        default:
            return 'other';
    }
}
/**
 * The locale id that the application is using by default (for translations and ICU expressions).
 */
export var DEFAULT_LOCALE_ID = 'en-US';
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9jYWxpemF0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvaTE4bi9sb2NhbGl6YXRpb24udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxFQUFDLG1CQUFtQixFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFdEQ7O0dBRUc7QUFDSCxNQUFNLFVBQVUsYUFBYSxDQUFDLEtBQVUsRUFBRSxNQUFjO0lBQ3RELElBQU0sTUFBTSxHQUFHLG1CQUFtQixDQUFDLE1BQU0sQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDO0lBRWxELFFBQVEsTUFBTSxFQUFFO1FBQ2QsS0FBSyxDQUFDO1lBQ0osT0FBTyxNQUFNLENBQUM7UUFDaEIsS0FBSyxDQUFDO1lBQ0osT0FBTyxLQUFLLENBQUM7UUFDZixLQUFLLENBQUM7WUFDSixPQUFPLEtBQUssQ0FBQztRQUNmLEtBQUssQ0FBQztZQUNKLE9BQU8sS0FBSyxDQUFDO1FBQ2YsS0FBSyxDQUFDO1lBQ0osT0FBTyxNQUFNLENBQUM7UUFDaEI7WUFDRSxPQUFPLE9BQU8sQ0FBQztLQUNsQjtBQUNILENBQUM7QUFFRDs7R0FFRztBQUNILE1BQU0sQ0FBQyxJQUFNLGlCQUFpQixHQUFHLE9BQU8sQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtnZXRMb2NhbGVQbHVyYWxDYXNlfSBmcm9tICcuL2xvY2FsZV9kYXRhX2FwaSc7XG5cbi8qKlxuICogUmV0dXJucyB0aGUgcGx1cmFsIGNhc2UgYmFzZWQgb24gdGhlIGxvY2FsZVxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0UGx1cmFsQ2FzZSh2YWx1ZTogYW55LCBsb2NhbGU6IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IHBsdXJhbCA9IGdldExvY2FsZVBsdXJhbENhc2UobG9jYWxlKSh2YWx1ZSk7XG5cbiAgc3dpdGNoIChwbHVyYWwpIHtcbiAgICBjYXNlIDA6XG4gICAgICByZXR1cm4gJ3plcm8nO1xuICAgIGNhc2UgMTpcbiAgICAgIHJldHVybiAnb25lJztcbiAgICBjYXNlIDI6XG4gICAgICByZXR1cm4gJ3R3byc7XG4gICAgY2FzZSAzOlxuICAgICAgcmV0dXJuICdmZXcnO1xuICAgIGNhc2UgNDpcbiAgICAgIHJldHVybiAnbWFueSc7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiAnb3RoZXInO1xuICB9XG59XG5cbi8qKlxuICogVGhlIGxvY2FsZSBpZCB0aGF0IHRoZSBhcHBsaWNhdGlvbiBpcyB1c2luZyBieSBkZWZhdWx0IChmb3IgdHJhbnNsYXRpb25zIGFuZCBJQ1UgZXhwcmVzc2lvbnMpLlxuICovXG5leHBvcnQgY29uc3QgREVGQVVMVF9MT0NBTEVfSUQgPSAnZW4tVVMnO1xuIl19