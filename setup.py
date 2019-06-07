import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ocean_drilling_db",
    version="0.0.1",
    author="Richard D Berg",
    author_email="rickdberg@gmail.com",
    description="A package for integrating DSDP, ODP, and IODP ocean drilling databases",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rickdberg/ocean_drilling_db",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

