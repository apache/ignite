/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * Defines template and style encapsulation options available for Component's {@link Component}.
 *
 * See {@link Component#encapsulation encapsulation}.
 *
 * @usageNotes
 * ### Example
 *
 * {@example core/ts/metadata/encapsulation.ts region='longform'}
 *
 * @publicApi
 */
export var ViewEncapsulation;
(function (ViewEncapsulation) {
    /**
     * Emulate `Native` scoping of styles by adding an attribute containing surrogate id to the Host
     * Element and pre-processing the style rules provided via {@link Component#styles styles} or
     * {@link Component#styleUrls styleUrls}, and adding the new Host Element attribute to all
     * selectors.
     *
     * This is the default option.
     */
    ViewEncapsulation[ViewEncapsulation["Emulated"] = 0] = "Emulated";
    /**
     * @deprecated v6.1.0 - use {ViewEncapsulation.ShadowDom} instead.
     * Use the native encapsulation mechanism of the renderer.
     *
     * For the DOM this means using the deprecated [Shadow DOM
     * v0](https://w3c.github.io/webcomponents/spec/shadow/) and
     * creating a ShadowRoot for Component's Host Element.
     */
    ViewEncapsulation[ViewEncapsulation["Native"] = 1] = "Native";
    /**
     * Don't provide any template or style encapsulation.
     */
    ViewEncapsulation[ViewEncapsulation["None"] = 2] = "None";
    /**
     * Use Shadow DOM to encapsulate styles.
     *
     * For the DOM this means using modern [Shadow
     * DOM](https://w3c.github.io/webcomponents/spec/shadow/) and
     * creating a ShadowRoot for Component's Host Element.
     */
    ViewEncapsulation[ViewEncapsulation["ShadowDom"] = 3] = "ShadowDom";
})(ViewEncapsulation || (ViewEncapsulation = {}));
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlldy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL21ldGFkYXRhL3ZpZXcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUg7Ozs7Ozs7Ozs7O0dBV0c7QUFDSCxNQUFNLENBQU4sSUFBWSxpQkFnQ1g7QUFoQ0QsV0FBWSxpQkFBaUI7SUFDM0I7Ozs7Ozs7T0FPRztJQUNILGlFQUFZLENBQUE7SUFDWjs7Ozs7OztPQU9HO0lBQ0gsNkRBQVUsQ0FBQTtJQUNWOztPQUVHO0lBQ0gseURBQVEsQ0FBQTtJQUVSOzs7Ozs7T0FNRztJQUNILG1FQUFhLENBQUE7QUFDZixDQUFDLEVBaENXLGlCQUFpQixLQUFqQixpQkFBaUIsUUFnQzVCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIERlZmluZXMgdGVtcGxhdGUgYW5kIHN0eWxlIGVuY2Fwc3VsYXRpb24gb3B0aW9ucyBhdmFpbGFibGUgZm9yIENvbXBvbmVudCdzIHtAbGluayBDb21wb25lbnR9LlxuICpcbiAqIFNlZSB7QGxpbmsgQ29tcG9uZW50I2VuY2Fwc3VsYXRpb24gZW5jYXBzdWxhdGlvbn0uXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICoge0BleGFtcGxlIGNvcmUvdHMvbWV0YWRhdGEvZW5jYXBzdWxhdGlvbi50cyByZWdpb249J2xvbmdmb3JtJ31cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBlbnVtIFZpZXdFbmNhcHN1bGF0aW9uIHtcbiAgLyoqXG4gICAqIEVtdWxhdGUgYE5hdGl2ZWAgc2NvcGluZyBvZiBzdHlsZXMgYnkgYWRkaW5nIGFuIGF0dHJpYnV0ZSBjb250YWluaW5nIHN1cnJvZ2F0ZSBpZCB0byB0aGUgSG9zdFxuICAgKiBFbGVtZW50IGFuZCBwcmUtcHJvY2Vzc2luZyB0aGUgc3R5bGUgcnVsZXMgcHJvdmlkZWQgdmlhIHtAbGluayBDb21wb25lbnQjc3R5bGVzIHN0eWxlc30gb3JcbiAgICoge0BsaW5rIENvbXBvbmVudCNzdHlsZVVybHMgc3R5bGVVcmxzfSwgYW5kIGFkZGluZyB0aGUgbmV3IEhvc3QgRWxlbWVudCBhdHRyaWJ1dGUgdG8gYWxsXG4gICAqIHNlbGVjdG9ycy5cbiAgICpcbiAgICogVGhpcyBpcyB0aGUgZGVmYXVsdCBvcHRpb24uXG4gICAqL1xuICBFbXVsYXRlZCA9IDAsXG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCB2Ni4xLjAgLSB1c2Uge1ZpZXdFbmNhcHN1bGF0aW9uLlNoYWRvd0RvbX0gaW5zdGVhZC5cbiAgICogVXNlIHRoZSBuYXRpdmUgZW5jYXBzdWxhdGlvbiBtZWNoYW5pc20gb2YgdGhlIHJlbmRlcmVyLlxuICAgKlxuICAgKiBGb3IgdGhlIERPTSB0aGlzIG1lYW5zIHVzaW5nIHRoZSBkZXByZWNhdGVkIFtTaGFkb3cgRE9NXG4gICAqIHYwXShodHRwczovL3czYy5naXRodWIuaW8vd2ViY29tcG9uZW50cy9zcGVjL3NoYWRvdy8pIGFuZFxuICAgKiBjcmVhdGluZyBhIFNoYWRvd1Jvb3QgZm9yIENvbXBvbmVudCdzIEhvc3QgRWxlbWVudC5cbiAgICovXG4gIE5hdGl2ZSA9IDEsXG4gIC8qKlxuICAgKiBEb24ndCBwcm92aWRlIGFueSB0ZW1wbGF0ZSBvciBzdHlsZSBlbmNhcHN1bGF0aW9uLlxuICAgKi9cbiAgTm9uZSA9IDIsXG5cbiAgLyoqXG4gICAqIFVzZSBTaGFkb3cgRE9NIHRvIGVuY2Fwc3VsYXRlIHN0eWxlcy5cbiAgICpcbiAgICogRm9yIHRoZSBET00gdGhpcyBtZWFucyB1c2luZyBtb2Rlcm4gW1NoYWRvd1xuICAgKiBET01dKGh0dHBzOi8vdzNjLmdpdGh1Yi5pby93ZWJjb21wb25lbnRzL3NwZWMvc2hhZG93LykgYW5kXG4gICAqIGNyZWF0aW5nIGEgU2hhZG93Um9vdCBmb3IgQ29tcG9uZW50J3MgSG9zdCBFbGVtZW50LlxuICAgKi9cbiAgU2hhZG93RG9tID0gM1xufVxuIl19