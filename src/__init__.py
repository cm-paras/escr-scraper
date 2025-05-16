# In a very early loading file (like __init__.py or settings.py)
import os

# Store the original getenv function
original_getenv = os.getenv


# Define our monkey-patched version
def fixed_getenv(name, default=None):
    value = original_getenv(name, default)

    if value is None:
        return None

    if isinstance(value, str) and ("\\x3a" in value or "\\x3A" in value):
        value = value.replace("\\x3a", ":").replace("\\x3A", ":")

    return value


# Replace the original with our fixed version
os.getenv = fixed_getenv
