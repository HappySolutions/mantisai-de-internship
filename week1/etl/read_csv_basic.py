# import csv

# with open("data/raw/users.csv", "r") as file:
#     reader = csv.DictReader(file)

#     for row in reader:
#         print(row)

# clean_rows = []

# with open("data/raw/users.csv", "r") as file:
#     reader = csv.DictReader(file)

#     for row in reader:
#         age = row["age"]

#         if age.isdigit():
#             clean_rows.append(row)

# print(clean_rows)

# with open("data/processed/clean_users.csv", "w", newline="") as file:
#     writer = csv.DictWriter(file, fieldnames=["name", "age"])
#     writer.writeheader()
#     writer.writerows(clean_rows)

# print("ETL finished")

import csv


def extract(file_path):
    """Read CSV and return list of dictionties"""
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        return list(reader)


def transform(data):
    """Clean the data"""
    clean_rows = []
    for row in data:
        age = row["age"]
        if age.isdigit():
            clean_rows.append(row)
    return clean_rows


def load(data, output_path):
    """Write cleaned data to new CSV"""
    with open(output_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["name", "age"])
        writer.writeheader()
        writer.writerows(data)


def run_etl(input_path, output_path):
    data = extract(input_path)
    data = transform(data)
    load(data, output_path)
    print("ETL finished")


if __name__ == "__main__":
    run_etl("data/raw/users.csv", "data/processed/clean_users_1.csv")
