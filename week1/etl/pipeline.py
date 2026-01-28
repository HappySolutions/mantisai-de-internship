import logging
import csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler()
    ]
)


class CsvETLPipeline:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

    def extract(self):
        logging.info("Starting extraction")
        try:
            with open(self.input_path, "r") as file:
                reader = csv.DictReader(file)
                data = list(reader)
                logging.info(f"Extracted {len(data)} records")
            return data
        except FileNotFoundError:
            logging.error("Input file not found")
            raise

    def transform(self, data):
        logging.info("Starting transformation")
        clean_rows = []
        seen = set()
        for row in data:
            key = (row["name"], row["age"])
            if key not in seen and row["age"].isdigit():
                seen.add(key)
                clean_rows.append(row)
        logging.info(f"Records after transform: len{(clean_rows)}")
        return clean_rows

    def load(self, data):
        logging.info("Starting load")
        with open(self.output_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=["name", "age"])
            writer.writeheader()
            writer.writerows(data)
        logging.info("Load compleated successfully")

    def run(self):
        logging.info("ETL job started")
        data = self.extract()
        data = self.transform(data)
        self.load(data)
        logging.info("ETL job finished")


if __name__ == "__main__":
    pipeline = CsvETLPipeline("data/raw/users.csv",
                              "data/processed/clean_users_2.csv")
    pipeline.run()
