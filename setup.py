from setuptools import find_packages,setup


setup(
    name= "sensor",
    version= "0.0.1",
    author= "Shivang Shritu",
    author_email= "shivang.cse.nitnagaland@gmail.com",
    packages= find_packages(),
    install_requires=get_requirements(),
)