from setuptools import setup, find_packages

setup(
    name="dag-generator",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Jinja2>=3.0.0",
        "PyYAML>=6.0",
        "click>=8.0.0",
        "SQLAlchemy>=1.4.0",
        "pydantic>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "dag-generator=dag_generator.cli:main",
        ],
    },
)