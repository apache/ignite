

/**
 * @typedef {{id: string, viewBox: string, content: string}} SpriteSymbol
 */

/**
 * @typedef {{[name: string]: SpriteSymbol}} Icons
 */

export default class IgniteIcon {
    /**
     * @type {Icons}
     */
    _icons = {};

    /**
     * @param {Icons} icons
     */
    registerIcons(icons) {
        return Object.assign(this._icons, icons);
    }

    /**
     * @param {string} name
     */
    getIcon(name) {
        return this._icons[name];
    }

    getAllIcons() {
        return this._icons;
    }
}
