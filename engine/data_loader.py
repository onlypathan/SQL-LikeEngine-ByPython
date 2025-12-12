class DataLoader:
    def __init__(self, db, parser):
        # Store reference to the database engine
        self.db = db
        # Store reference to the CSV parser
        self.parser = parser

    def create_tables(self):
        # Create ZIP code lookup table with primary key and index
        self.db.create_table(
            "zip_code",
            primary_key="Zip_Code_ID",
            indexes=["Zip_Code"]
        )

        # Create demographics table linked to ZIP codes
        self.db.create_table(
            "demographics_info",
            primary_key="Demographics_Info_ID",
            indexes=["F_Zip_Code_ID"]
        )

        # Create inspection table linked to restaurants
        self.db.create_table(
            "inspection_info",
            primary_key="Inspection_Info_ID",
            indexes=["F_Restaurant_Info_ID"]
        )

        # Create restaurant table with frequently queried indexed fields
        self.db.create_table(
            "restaurant_info",
            primary_key="Restaurant_Info_ID",
            indexes=["F_Zip_Code_ID", "Restaurant_Name", "Categories"]
        )

    def load_all(self):
        # Initialize all required tables
        self.create_tables()

        # Load each CSV file into its corresponding table
        self.load_csv("../data/zip_code.csv", "zip_code")
        self.load_csv("../data/demographics_info.csv", "demographics_info")
        self.load_csv("../data/inspection_info.csv", "inspection_info")
        self.load_csv("../data/restaurant_info.csv", "restaurant_info")

    def load_csv(self, filepath, table_name):
        # Parse each row from the CSV file
        for row in self.parser.parse(filepath):
            # Insert parsed row into the specified table
            self.db.insert(table_name, row)
