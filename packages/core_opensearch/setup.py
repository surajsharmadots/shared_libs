"""
Setup configuration for core_opensearch package
"""
from setuptools import setup, find_packages
import os

# Read version from __init__.py
with open(os.path.join("core_opensearch", "__init__.py"), "r") as f:
    for line in f:
        if line.startswith("__version__"):
            version = line.split("=")[1].strip().strip('"').strip("'")
            break
    else:
        version = "0.1.0"

# Read requirements
with open("requirements.txt", "r") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

with open("requirements-dev.txt", "r") as f:
    dev_requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

# Read long description
with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="core-opensearch",
    version=version,
    author="Suraj Sharma",
    author_email="spsurajsharma72@gmail.com",
    description="Async-first OpenSearch client for e-commerce search and analytics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/surajsharmadots/shared_libs",
    packages=find_packages(include=["core_opensearch", "core_opensearch.*"]),
    package_data={
        "core_opensearch": ["py.typed"],
    },
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
        "aws": ["boto3>=1.28.0", "aws-requests-auth>=0.4.3"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Framework :: AsyncIO",
        "Framework :: FastAPI",
    ],
    keywords="opensearch, elasticsearch, search, analytics, ecommerce, fastapi, async",
    zip_safe=False,
)