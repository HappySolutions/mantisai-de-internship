import os
import csv
from etl.pipeline import CsvETLPipeline

"""Test No. 1 for the extraction"""


def test_trnsform_filters_invalide_ages():
    pipeline = CsvETLPipeline("input.csv", "output.csv")

    input_data = [
        {"name": "Ahmed", "age": "25"},
        {"name": "Sara", "age": ""},
        {"name": "Lina", "age": "twenty"},
        {"name": "Omar", "age": "30"},
    ]

    result = pipeline.transform(input_data)

    assert len(result) == 2
    assert result[0]["name"] == "Ahmed"
    assert result[1]["name"] == "Omar"


"""Test No. 2 do simple Extract"""


def test_extract_reads_csv(tmp_path):
    test_file = tmp_path / "test.csv"

    with open(test_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "age"])
        writer.writeheader()
        writer.writerows([
            {"name": "Ahmed", "age": "25"}
        ])

    pipeline = CsvETLPipeline(str(test_file), "out.csv")
    data = pipeline.extract()

    assert len(data) == 1
    assert data[0]["name"] == "Ahmed"


"""Test No. 3 for Load"""


def test_load_creates_output_file(tmp_path):
    output_file = tmp_path / "out.csv"

    pipeline = CsvETLPipeline("input.csv", str(output_file))

    data = [
        {"name": "Ahmed", "age": "25"},
        {"name": "Omar", "age": "30"}
    ]

    pipeline.load(data)

    assert os.path.exists(output_file)
