
def say_hello(name: str) -> str:
    if not isinstance(name, str):
        return "Hello"
    return "Hello " + name


print(say_hello(10))
