import re

def __camel_to_snake(input_str: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', input_str).lower()

def column_names_to_snake(columns: list) -> list:
    return list(map(__camel_to_snake, columns))
