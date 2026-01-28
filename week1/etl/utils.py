def safe_string(valu: str) -> str:
    if valu is None:
        return ""
    return str(valu).strip().lower()


print(safe_string("       DATA"))
print(safe_string(10))
print(safe_string(None))
