class CSVParser:
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def parse(self, filepath):
        def infer(v):
            v = v.strip()
            if v == "": return None
            for cast in (int, float):
                try: return cast(v)
                except ValueError: pass
            return v

        def parse_rows(stream, delim):
            data = stream.read()
            row, field, in_quotes = [], [], False
            i, n = 0, len(data)
            while i < n:
                ch = data[i]
                if in_quotes:
                    if ch == '"':
                        if i + 1 < n and data[i+1] == '"':
                            field.append('"'); i += 1
                        else:
                            in_quotes = False
                    else:
                        field.append(ch)
                else:
                    if ch == '"':
                        in_quotes = True
                    elif ch == delim:
                        row.append(''.join(field)); field = []
                    elif ch == '\n':
                        row.append(''.join(field)); field = []
                        yield row; row = []
                    elif ch == '\r':
                        pass
                    else:
                        field.append(ch)
                i += 1
            if field or row:
                row.append(''.join(field)); yield row

        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            rows = parse_rows(f, self.delimiter)
            headers = [h.strip() for h in next(rows)]
            for r in rows:
                if len(r) < len(headers):
                    r += [''] * (len(headers) - len(r))
                elif len(r) > len(headers):
                    r = r[:len(headers)]
                yield dict(zip(headers, (infer(x) for x in r)))
