#!/usr/bin/env python3
"""
Convert Jupyter notebook to Databricks notebook format.
This script takes a Jupyter notebook and converts it to a format that can be imported into Databricks.
"""

import json
import sys
import base64
import os


def convert_jupyter_to_databricks(input_path, output_path=None):
    """
    Convert a Jupyter notebook to Databricks SOURCE format.

    Args:
        input_path: Path to the Jupyter notebook
        output_path: Path to save the converted notebook (optional)

    Returns:
        The converted notebook as a string with Python source code
    """
    # Read the Jupyter notebook
    with open(input_path, "r") as f:
        notebook = json.load(f)

    # Extract code cells and convert to Python source
    source_code = []

    # Add a header comment
    source_code.append("# Databricks notebook source")
    source_code.append("")

    for cell_index, cell in enumerate(notebook.get("cells", [])):
        cell_type = cell.get("cell_type")
        source = "".join(cell.get("source", []))

        # Add cell separator command except for the first cell
        if cell_index > 0:
            source_code.append("# COMMAND ----------")
            source_code.append("")

        if cell_type == "markdown":
            # Convert markdown cells to Databricks markdown format
            source_code.append("# MAGIC %md")
            # Add MAGIC prefix to each line of markdown
            for line in source.split("\n"):
                source_code.append(f"# MAGIC {line}")
        elif cell_type == "code":
            # Add code cells directly
            source_code.append(source)

    # Join all lines with newlines
    result = "\n".join(source_code)

    # If output path is provided, write the content to the file
    if output_path:
        with open(output_path, "w") as f:
            f.write(result)

    return result


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python jupyter_to_databricks.py <input_notebook.ipynb> [output_notebook.json]"
        )
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        result = convert_jupyter_to_databricks(input_path, output_path)
        if not output_path:
            print(result)
        else:
            print(f"Converted notebook saved to {output_path}")

        # Also print base64 encoded version for API usage
        # base64_content = base64.b64encode(result.encode()).decode()
        # print(f"\nBase64 encoded content for API usage:")
        # print(base64_content)
    except Exception as e:
        print(f"Error converting notebook: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
