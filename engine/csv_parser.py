class CSVParser:
    def __init__(self, delimiter=','):
        # Store the delimiter used to separate values in the CSV file
        self.delimiter = delimiter

    def parse(self, filepath):
        import csv

        # Infer the most appropriate Python type for a CSV value
        def infer(value):
            # Remove surrounding whitespace
            value = value.strip()

            # Treat empty fields as None
            if not value:
                return None

            # Try casting to int or float (in that order)
            for cast in (int, float):
                try:
                    return cast(value)
                except ValueError:
                    continue

            # Fallback: return the value as a string
            return value

        # Open the CSV file safely with UTF-8 encoding
        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            # Create a CSV reader using the specified delimiter
            reader = csv.reader(f, delimiter=self.delimiter)

            # Read and clean header names
            headers = [h.strip() for h in next(reader)]

            # Iterate through each data row
            for row_values in reader:
                # Infer data types for each cell in the row
                values = [infer(val) for val in row_values]

                # Yield each row as a dictionary (header → value)
                yield dict(zip(headers, values))
