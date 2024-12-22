from setuptools import setup, find_packages

setup(
    name="payment_system",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "apache-beam",
        "happybase",
        "kafka-python",
        "fastapi",
        "uvicorn",
        "faker",
        "python-dateutil",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "black",
            "flake8",
        ],
    },
    python_requires=">=3.11",
)
