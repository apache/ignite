(function() {
    this.CSVKit = {};

    /* Utils */

    var ctor = function() {};
    var inherits = function(child, parent){
        ctor.prototype  = parent.prototype;
        child.prototype = new ctor();
        child.prototype.constructor = child;
    };

    /* CSVKit.Reader */

    CSVKit.Reader = function(options) {
        options = options || {};

        this.separator    = options.separator || ',';
        this.quote_char   = options.quote_char || '"';
        this.escape_char  = options.escape_char || '"';
        this.column_names = options.column_names || [];
        this.columns_from_header = 'columns_from_header' in options ? options.columns_from_header : true;
        this.nested_quotes = 'nested_quotes' in options ? options.nested_quotes : false;
        this.rows = [];

        this.state = {
            rows:               0,
            open_record:        [],
            open_field:         '',
            last_char:          '',
            in_quoted_field:    false
        };
    };

    CSVKit.Reader.prototype.parse = function(data) {
        if (this.state.open_record.length === 0) {
            if (data.charCodeAt(0) === 0xFEFF) {
                data = data.slice(1);
            }
        }

        for (var i = 0; i < data.length; i++) {
            var c = data.charAt(i), next_char;
            switch (c) {
                // escape and separator may be the same char, typically '"'
                case this.escape_char:
                case this.quote_char:
                    var is_escape = false;

                    if (c === this.escape_char) {
                        next_char = data.charAt(i + 1);

                        if (this._is_escapable(next_char)) {
                            this._add_character(next_char);
                            i++;
                            is_escape = true;
                        }
                    }
                    if (!is_escape && (c === this.quote_char)) {
                        if (this.state.open_field && !this.state.in_quoted_field) {
                            this.state.in_quoted_field = true;
                            break;
                        }

                        if (this.state.in_quoted_field) {
                            // closing quote should be followed by separator unless the nested quotes option is set
                            next_char = data.charAt(i + 1);

                            if (next_char && next_char !== '\r' && next_char != '\n' && next_char !== this.separator && this.nested_quotes !== true) {
                                throw new Error("separator expected after a closing quote; found " + next_char);
                            } else {
                                this.state.in_quoted_field = false;
                            }
                        } else if (this.state.open_field === '') {
                            this.state.in_quoted_field = true;
                        }
                    }

                    break;
                case this.separator:
                    if (this.state.in_quoted_field) {
                        this._add_character(c);
                    } else {
                        this._add_field();
                    }
                    break;
                case '\n':
                    // handle CRLF sequence
                    if (!this.state.in_quoted_field && (this.state.last_char === '\r')) {
                        break;
                    }
                case '\r':
                    if (this.state.in_quoted_field) {
                        this._add_character(c);
                    } else {
                        this._add_field();
                        this._add_record();
                    }
                    break;
                default:
                    this._add_character(c);
            }

            this.state.last_char = c;
        }

        if (this.state.in_quoted_field) {
            throw new Error("Input stream ended but closing quotes expected");
        } else {
            if (this.state.open_field) {
                this._add_field();
            }

            if (this.state.open_record.length > 0) {
                this._add_record();
            }
        }
    };

    CSVKit.Reader.prototype._is_escapable = function(c) {
        if ((c === this.escape_char) || (c === this.quote_char)) {
            return true;
        }
        return false;
    };

    CSVKit.Reader.prototype._add_character = function(c) {
        this.state.open_field += c;
    };

    CSVKit.Reader.prototype._add_field = function() {
        this.state.open_record.push(this.state.open_field);
        this.state.open_field = '';
        this.state.in_quoted_field = false;
    };

    CSVKit.Reader.prototype._add_record = function() {
        if (this.columns_from_header && this.state.rows === 0) {
            this.column_names = this.state.open_record;
        } else {
            this.rows.push(this._serialize_record(this.state.open_record));
        }

        this.state.rows++;
        this.state.open_record = [];
        this.state.open_field = '';
        this.state.in_quoted_field = false;
    };

    CSVKit.Reader.prototype._serialize_record = function(record) {
        return record;
    };

    /* CSVKit.ObjectReader */

    CSVKit.ObjectReader = function(options) {
        CSVKit.Reader.call(this, options);
    };
    inherits(CSVKit.ObjectReader, CSVKit.Reader);

    CSVKit.ObjectReader.prototype._serialize_record = function(record) {
        var obj = {};

        for (var i = 0; i < this.column_names.length; i++) {
            obj[this.column_names[i]] = record[i];
        }

        return obj;
    };

    /* CSVKit.Writer */
    
    CSVKit.Writer = function(options) {
        options = options || {};

        this.separator    = options.separator || ',';
        this.quote_char   = options.quote_char || '"';
        this.escape_char  = options.escape_char || '"';
        this.quote_all   = options.quote_all || false;
        this.newline      = '\n';

        CSVKit.Writer.prototype.write = function(rows) {
            var formatted_rows = [];

            for (var i = 0; i < rows.length; i++) {
                formatted_rows.push(this._serialize_row(rows[i]));
             }

            return formatted_rows.join(this.newline);
        };

        CSVKit.Writer.prototype._serialize_row = function(row) {
            var formatted_cells = [];

            for (var i = 0; i < row.length; i++) {
                formatted_cells.push(this._serialize_cell(row[i]));
            }

            return formatted_cells.join(this.separator);
        };

        CSVKit.Writer.prototype._serialize_cell = function(cell) {
            if (cell.indexOf(this.quote_char) >= 0) {
                cell = cell.replace(new RegExp(this.quote_char, 'g'), this.escape_char + this.quote_char);
            }

            if (this.quote_all || cell.indexOf(this.separator) >= 0 || cell.indexOf(this.newline) >= 0) {
                return this.quote_char + cell + this.quote_char;
            }

            return cell;
        };
    }

    /* CSVKit.ObjectWriter */

    CSVKit.ObjectWriter = function(options) {
        CSVKit.Writer.call(this, options);

        if (!('column_names' in options)) {
            throw "The column_names option is required.";
        }

        this.column_names = options.column_names;
    };
    inherits(CSVKit.ObjectWriter, CSVKit.Writer);

    CSVKit.ObjectWriter.prototype.write = function(rows) {
        var header = {};

        for (var i = 0; i < this.column_names.length; i++) {
            header[this.column_names[i]] = this.column_names[i];
        }

        rows.splice(0, 0, header); 

        return CSVKit.Writer.prototype.write.call(this, rows);
    }

    CSVKit.ObjectWriter.prototype._serialize_row = function(row) {
        var cells = [];
        
        for (var i = 0; i < this.column_names.length; i++) {
            cells.push(row[this.column_names[i]]);
        }

        return CSVKit.Writer.prototype._serialize_row.call(this, cells);
    };

}).call(this);
