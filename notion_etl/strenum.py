from enum import Enum, auto


class StrEnum(str, Enum):
    """String Enum, backported for Python versions <3.11."""

    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, (str, auto)):
            raise TypeError(f"Values of StrEnums must be strings: {value!r} is a {type(value)}")
        return str.__new__(cls, value)

    def __str__(self):
        return str(self.value)

    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()
