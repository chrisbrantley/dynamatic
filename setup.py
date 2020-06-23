import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dynamatic",
    version="1.0.0-beta.5",
    author="Chris Brantley",
    description="A DynamoDB library based on boto3",
    long_description=long_description,
    url="https://github.com/chrisbrantley/dynamatic",
    packages=["dynamatic"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["boto3>=1.9"],
    python_requires=">=3.6",
)

