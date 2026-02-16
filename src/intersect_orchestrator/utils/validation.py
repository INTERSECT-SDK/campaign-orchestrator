"""
Validation utilities for JSON schemas.
"""

from typing import Any

from jsonschema import Draft202012Validator as SchemaValidator

_metavalidator = SchemaValidator(
    SchemaValidator.META_SCHEMA, format_checker=SchemaValidator.FORMAT_CHECKER
)


def validate_schema(json_schema: dict[str, Any]) -> list[str]:
    """Checks that json_schema is itself a valid JSON schema - specifically, against draft 2020-12.

    Returns a list of error strings. If list is empty, there were no errors.
    """
    # custom impl. of check_schema() , gather all errors instead of throwing on first error
    return [
        f'{error.json_path} : {error.message}' for error in _metavalidator.iter_errors(json_schema)
    ]
