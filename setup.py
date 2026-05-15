from setuptools import setup, find_packages

setup(
    name="anvil-p02-mnemosyne",
    version="1.0.0",
    description="Mnemosyne — Persistent Operational Memory for Autonomous SRE",
    author="Team Mnemosyne",
    packages=find_packages(exclude=["tests*", "writeup*", "bench*", "scripts*"]),
    python_requires=">=3.10",
    install_requires=[
        "sentence-transformers>=2.7.0",
        "scikit-learn>=1.4.0",
        "numpy>=1.26.4",
        "networkx>=3.3",
        "python-dotenv>=1.0.1",
        "anthropic>=0.25.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.1.0",
            "pytest-cov>=5.0.0",
            "black>=24.4.2",
        ]
    },
)
