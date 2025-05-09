

export class Cluster {
    /** @type {String} */
    id;

    /** @type {String} */
    name;

    /** @type {Boolean} */
    connected = true;

    /** @type {Boolean} */
    secured;

    constructor({id, name, secured = false}) {
        this.id = id;
        this.name = name;
        this.secured = secured;
    }
}

