function stringFunctions() {
    if (!String.prototype.startsWith || typeof String.prototype.startsWith != 'function') {
        String.prototype.startsWith = function(prefix) {
            return this.slice(0, prefix.length) === prefix;
        };
    }

    if (!String.prototype.endsWith || typeof String.prototype.endsWith != 'function') {
        String.prototype.endsWith = function(suffix) {
            return this.indexOf(suffix, this.length - suffix.length) !== -1;
        };
    }
}

stringFunctions();