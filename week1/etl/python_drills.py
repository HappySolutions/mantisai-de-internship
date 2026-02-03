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


def say_hello(name: str) -> str:
    if not isinstance(name, str):
        return "Hello"
    return "Hello " + name


print(say_hello(10))
