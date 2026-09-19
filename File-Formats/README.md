# Big Data File Formats

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Big Data](https://img.shields.io/badge/big--data-Parquet%20%7C%20Avro-orange)

A comprehensive reference and practical guide demonstrating the usage, architecture, and best practices for big data file formats like **Apache Parquet** and **Apache Avro** using Python.

##  Overview

This repository serves as a study and implementation reference for working with popular big data file formats. It contains practical Python examples and theoretical documentation to help you understand how these formats work under the hood and how to leverage them for high-performance analytical workloads.

### Key Technologies Covered:

*   **[Apache Parquet](./Parquet/parquet.md):** An open-source, columnar storage format optimized for read-heavy analytical workloads.
    *   *Topics included:* Architecture, Column Selection, Partitioning, Z-Ordering, Schema Evolution, and Metadata Inspection.
*   **Apache Avro:** A row-based storage format widely used for data serialization and streaming architectures.
    *   *Topics included:* Schema definition and Schema Evolution.

##  Getting Started

### Prerequisites

*   Python 3.10 or higher

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/file-formats.git
    cd file-formats
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: You can also use `pip install -e .` if you want to install it as a local package based on `pyproject.toml`)*

##  Project Structure

```text
├── avro/                   # Apache Avro examples and study material
│   └── schema_evolution/   # Examples of Avro schema evolution
├── Parquet/                # Apache Parquet examples and study material
│   ├── 01_intro.py         # Introduction script to Parquet in Python
│   ├── column_selection/   # Column pruning and selection examples
│   ├── metadata_inspection/# How to read and inspect Parquet footer metadata
│   ├── parquet.md          # Comprehensive study reference on Parquet architecture
│   ├── Partitioning/       # Data partitioning strategies
│   ├── schema_evolution/   # Handling schema changes in Parquet
│   └── z_ordoring/         # Z-order indexing for performance optimization
├── main.py                 # Entry point script
├── pyproject.toml          # Python project metadata and dependencies
└── requirements.txt        # Required Python packages
```

##  Documentation & Study Guides

Don't miss the detailed study references included in this repository:
*   [Apache Parquet Study Reference](./Parquet/parquet.md) - Deep dive into Parquet's columnar architecture, compression, and query optimization features.

##  Contributing

Contributions are welcome! If you have examples for other formats (like ORC, Iceberg, Delta Lake) or want to improve existing scripts, feel free to open a Pull Request.

##  License

This project is open-source and available under the standard MIT License.
