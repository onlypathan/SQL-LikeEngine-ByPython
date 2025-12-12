class CSVParser:
    def __init__(self, delimiter=','):
        # Store the delimiter used to separate fields (default: comma)
        self.delimiter = delimiter

    def parse(self, filepath):
        # Infer a Python type for a cell value: int -> float -> str (empty -> None)
        def infer(v):
            v = v.strip()
            if v == "":
                return None
            for cast in (int, float):
                try:
                    return cast(v)
                except ValueError:
                    pass
            return v  # fallback: keep as string

        # Minimal CSV tokenizer (no csv module): handles delimiter, quotes, escaped quotes, newlines in quotes
        def parse_rows(stream, delim):
            data = stream.read()  # reads entire file; simple and fine for moderate sizes
            row, field, in_quotes = [], [], False
            i, n = 0, len(data)

            while i < n:
                ch = data[i]

                if in_quotes:
                    if ch == '"':
                        # Handle escaped quote ("") inside quoted field
                        if i + 1 < n and data[i + 1] == '"':
                            field.append('"')
                            i += 1  # skip the escape char
                        else:
                            in_quotes = False  # closing quote
                    else:
                        field.append(ch)
                else:
                    if ch == '"':
                        in_quotes = True  # start quoted field
                    elif ch == delim:
                        # End of field: push and reset
                        row.append(''.join(field))
                        field = []
                    elif ch == '\n':
                        # End of record (line): push last field, yield the row, reset
                        row.append(''.join(field))
                        field = []
                        yield row
                        row = []
                    elif ch == '\r':
                        # Ignore CR; CRLF will be finalized by the '\n'
                        pass
                    else:
                        field.append(ch)

                i += 1

            # Flush final field/row at EOF (if file doesn't end with newline)
            if field or row:
                row.append(''.join(field))
                yield row

        # Open file and stream through our tokenizer
        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            rows = parse_rows(f, self.delimiter)

            # First row is the header; strip whitespace
            headers = [h.strip() for h in next(rows)]

            # Yield each subsequent row as a dict: header -> inferred value
            for r in rows:
                # Align row length to header length (pad or truncate)
                if len(r) < len(headers):
                    r += [''] * (len(headers) - len(r))
                elif len(r) > len(headers):
                    r = r[:len(headers)]

                # Infer types per cell and produce a row dictionary
                yield dict(zip(headers, (infer(x) for x in r)))
