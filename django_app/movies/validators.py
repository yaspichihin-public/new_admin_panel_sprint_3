"""Validators for field models."""
from django.core.validators import (
    MaxValueValidator,
    MinValueValidator,
)

rating_validator = [
    MaxValueValidator(100),
    MinValueValidator(0),
]
