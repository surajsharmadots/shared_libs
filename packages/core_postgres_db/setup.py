# shared_libs setup.py

from setuptools import setup, find_packages

setup(
    name='core_postgres_db',  # Package name
    version='0.1.0',             # Version
    py_modules=['core_postgres_db'],
    install_requires=[
        'SQLAlchemy>=2.0',
        'tenacity',
        'psycopg2-binary',
        'asyncpg',
        'pydantic',          # For Pydantic BaseSettings and data validation
        'pydantic-settings', # If using the structured settings module
    ],
    description='Shared database interface and utilities for all Postgresql projects.',
)
