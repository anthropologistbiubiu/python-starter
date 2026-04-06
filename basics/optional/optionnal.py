from typing import Optional

def divide(a: int, b: int) -> Optional[float]:
    if b == 0:
        return None
    else:
        return a / b

result = divide(10, 2)
if result is not None:
    print("Result:", result)
else:
    print("Division by zero!")

result = divide(10, 0)
if result is not None:
    print("Result:", result)
else:
    print("Division by zero!")
