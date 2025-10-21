class DataLoader:
    def __init__(self, db, parser):
        self.db = db
        self.parser = parser

    def create_tables(self):
        self.db.create_table("zip_code", primary_key="Zip_Code_ID", indexes=["Zip_Code"])
        self.db.create_table("demographics_info", primary_key="Demographics_Info_ID", indexes=["F_Zip_Code_ID"])
        self.db.create_table("inspection_info", primary_key="Inspection_Info_ID", indexes=["F_Restaurant_Info_ID"])
        self.db.create_table("restaurant_info", primary_key="Restaurant_Info_ID", indexes=["F_Zip_Code_ID", "Restaurant_Name", "Categories"])

    def load_all(self):
        self.create_tables()
        self.load_csv("data/zip_code.csv", "zip_code")
        self.load_csv("data/demographics_info.csv", "demographics_info")
        self.load_csv("data/inspection_info.csv", "inspection_info")
        self.load_csv("data/restaurant_info.csv", "restaurant_info")

    def load_csv(self, filepath, table_name):
        for row in self.parser.parse(filepath):
            self.db.insert(table_name, row)