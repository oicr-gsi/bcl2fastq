from setuptools import setup, find_packages

setup(
    name="bcl2fastq-support",
    version="0.0.0",
    description="Helper scripts for bcl2fastq workflow",
    author="Andre Masella",
    author_email="andre.masella@oicr.on.ca",
    python_requires=">=3.4.0",
    packages=find_packages(exclude=["test"]),
    scripts=[
        "bcl2fastq-sample-sheet",
    ],
)
