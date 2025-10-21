class CSVParser:
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def parse(self, filepath):
        import csv
        def infer(value):
            value = value.strip()
            if not value:
                return None
            for cast in (int, float):
                try:
                    return cast(value)
                except ValueError:
                    continue
            return value

        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            headers = [h.strip() for h in next(reader)]
            for row_values in reader:
                values = [infer(val) for val in row_values]
                yield dict(zip(headers, values))