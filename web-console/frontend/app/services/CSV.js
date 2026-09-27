

export class CSV {
    getSeparator() {
        return (0.5).toLocaleString().includes(',') ? ';' : ',';
    }

    toJsonString(){
        return JSON.stringify(this);
    }
}
