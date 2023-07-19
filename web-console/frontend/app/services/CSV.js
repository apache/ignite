

export class CSV {
    getSeparator() {
        return (0.5).toLocaleString().includes(',') ? ';' : ',';
    }
}
