"""Loop on List"""

numbers = ["10", "20", None, "30"]
clean_numbers = []
errors = (TypeError, ValueError)

for value in numbers:
    try:
        key = int(value)
        clean_numbers.append(key)
    except errors:
        pass

print(clean_numbers)

"""Dict processing"""
records = [
    {"name": " Ali ", "age": "30"},
    {"name": None, "age": "twenty"},
    {"name": "Sara", "age": "25"},
]

clean_records = []
errors = (TypeError, ValueError)


def clean_record_and_validate(record, errors):
    name = record.get("name")
    age = record.get("age")
    if name is None:
        return None
    clean_name = name.strip()
    if len(clean_name) == 0:
        return None
    try:
        clean_age = int(age)
    except errors:
        return None
    clean_record = {
        "name": clean_name.lower(),
        "age": clean_age
    }
    return clean_record


for record in records:
    clean_records_output = clean_record_and_validate(record, errors)
    if clean_records_output is not None:
        clean_records.append(clean_records_output)

print(clean_records)


def say_hello(name: str) -> str:
    if not isinstance(name, str):
        return "Hello"
    return "Hello " + name


print(say_hello(10))
