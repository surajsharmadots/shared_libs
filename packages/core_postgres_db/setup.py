from setuptools import setup, find_packages
import os

# README read karna
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    # Basic metadata
    name='core_postgres_db',
    version='0.1.0',
    author="Suraj Sharma",
    author_email="spsurajsharma72@gmail.com",
    
    # Description
    description='Shared database interface and utilities for all Postgresql projects.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    
    # Project URLs
    url="https://github.com/surajsharmadots/shared_libs",
    
    # Package structure - IMPORTANT
    packages=find_packages(include=['core_postgres_db', 'core_postgres_db.*']),
    
    # Python requirements
    python_requires=">=3.8",
    
    # Dependencies
    install_requires=[
        'SQLAlchemy>=2.0.0',
        'tenacity>=8.2.0',
        'psycopg2-binary>=2.9.0',
        'asyncpg>=0.28.0',
        'pydantic>=2.0.0',
        'pydantic-settings>=2.0.0',
        'python-dotenv>=1.0.0',  # MUST for .env support
    ],
    
    # Optional dependencies
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-asyncio>=0.20.0',
        ]
    },
    
    # Package data
    include_package_data=True,
    
    # Classifiers for PyPI
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    
    # Keywords for search
    keywords="postgresql, database, sqlalchemy, async, sync, crud, ecommerce",
)