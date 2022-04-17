/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * This file should not be necessary because node resolution should just default to `./di/index`!
 *
 * However it does not seem to work and it breaks:
 *  - //packages/animations/browser/test:test_web_chromium-local
 *  - //packages/compiler-cli/test:extract_i18n
 *  - //packages/compiler-cli/test:ngc
 *  - //packages/compiler-cli/test:perform_watch
 *  - //packages/compiler-cli/test/diagnostics:check_types
 *  - //packages/compiler-cli/test/transformers:test
 *  - //packages/compiler/test:test
 *  - //tools/public_api_guard:core_api
 *
 * Remove this file once the above is solved or wait until `ngc` is deleted and then it should be
 * safe to delete this file.
 */
export * from './di/index';
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9kaS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSDs7Ozs7Ozs7Ozs7Ozs7O0dBZUc7QUFFSCxjQUFjLFlBQVksQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBUaGlzIGZpbGUgc2hvdWxkIG5vdCBiZSBuZWNlc3NhcnkgYmVjYXVzZSBub2RlIHJlc29sdXRpb24gc2hvdWxkIGp1c3QgZGVmYXVsdCB0byBgLi9kaS9pbmRleGAhXG4gKlxuICogSG93ZXZlciBpdCBkb2VzIG5vdCBzZWVtIHRvIHdvcmsgYW5kIGl0IGJyZWFrczpcbiAqICAtIC8vcGFja2FnZXMvYW5pbWF0aW9ucy9icm93c2VyL3Rlc3Q6dGVzdF93ZWJfY2hyb21pdW0tbG9jYWxcbiAqICAtIC8vcGFja2FnZXMvY29tcGlsZXItY2xpL3Rlc3Q6ZXh0cmFjdF9pMThuXG4gKiAgLSAvL3BhY2thZ2VzL2NvbXBpbGVyLWNsaS90ZXN0Om5nY1xuICogIC0gLy9wYWNrYWdlcy9jb21waWxlci1jbGkvdGVzdDpwZXJmb3JtX3dhdGNoXG4gKiAgLSAvL3BhY2thZ2VzL2NvbXBpbGVyLWNsaS90ZXN0L2RpYWdub3N0aWNzOmNoZWNrX3R5cGVzXG4gKiAgLSAvL3BhY2thZ2VzL2NvbXBpbGVyLWNsaS90ZXN0L3RyYW5zZm9ybWVyczp0ZXN0XG4gKiAgLSAvL3BhY2thZ2VzL2NvbXBpbGVyL3Rlc3Q6dGVzdFxuICogIC0gLy90b29scy9wdWJsaWNfYXBpX2d1YXJkOmNvcmVfYXBpXG4gKlxuICogUmVtb3ZlIHRoaXMgZmlsZSBvbmNlIHRoZSBhYm92ZSBpcyBzb2x2ZWQgb3Igd2FpdCB1bnRpbCBgbmdjYCBpcyBkZWxldGVkIGFuZCB0aGVuIGl0IHNob3VsZCBiZVxuICogc2FmZSB0byBkZWxldGUgdGhpcyBmaWxlLlxuICovXG5cbmV4cG9ydCAqIGZyb20gJy4vZGkvaW5kZXgnO1xuIl19