"""
Setup script for MEPS Python package.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="meps-python",
    version="0.1.0",
    author="MEPS Python Migration Team",
    author_email="meps@example.com",
    description="Python package for analyzing Medical Expenditure Panel Survey (MEPS) data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/COG-GTM/sas_repo_example",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Healthcare Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "pyreadstat>=1.2.0",
        "statsmodels>=0.14.0",
        "requests>=2.28.0",
        "openpyxl>=3.0.0",
        "scipy>=1.9.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.990",
        ],
    },
    entry_points={
        "console_scripts": [
            "meps=meps:main",
        ],
    },
)
