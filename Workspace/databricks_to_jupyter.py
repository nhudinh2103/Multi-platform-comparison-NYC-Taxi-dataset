#!/usr/bin/env python3
"""
Convert Databricks notebook to Jupyter notebook format.
This script takes a Databricks notebook and converts it to a Jupyter notebook.
"""

import json
import sys
import re
import os
from pathlib import Path


def convert_databricks_to_jupyter(input_path, output_path=None):
    """
    Convert a Databricks notebook to Jupyter notebook format.

    Args:
        input_path: Path to the Databricks notebook (.py)
        output_path: Path to save the converted notebook (optional)

    Returns:
        If output_path is provided: The path to the converted notebook
        If output_path is not provided: The converted notebook as a JSON object
    """
    # Read the Databricks notebook
    with open(input_path, "r") as f:
        content = f.read()

    # Split the notebook into cells using the command separator
    cell_pattern = r"# COMMAND ----------"
    cells = re.split(cell_pattern, content)

    # Remove the Databricks notebook source header if present
    if cells and "# Databricks notebook source" in cells[0]:
        cells[0] = cells[0].replace("# Databricks notebook source", "").strip()
        if not cells[0]:  # If the cell is now empty, remove it
            cells = cells[1:]

    # Initialize the Jupyter notebook structure
    notebook = {
        "cells": [],
        "metadata": {
            "application/vnd.databricks.v1+notebook": {
                "notebookName": Path(input_path).stem,
                "dashboards": [],
                "notebookMetadata": {"pythonIndentUnit": 4},
                "language": "python",
                "widgets": {},
                "notebookOrigID": 0,
            },
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.10",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    # Process each cell
    for cell_content in cells:
        cell_content = cell_content.strip()
        if not cell_content:
            continue

        # Check if it's a markdown cell (starts with # MAGIC %md)
        if re.match(r"# MAGIC %md", cell_content):
            # Extract markdown content by removing the MAGIC prefix
            markdown_lines = []
            for line in cell_content.split("\n"):
                if line.startswith("# MAGIC %md"):
                    # Skip the first line with the %md directive
                    continue
                elif line.startswith("# MAGIC "):
                    # Remove the MAGIC prefix
                    markdown_lines.append(line[8:])
                else:
                    markdown_lines.append(line)

            # Create a markdown cell
            notebook["cells"].append(
                {"cell_type": "markdown", "metadata": {}, "source": markdown_lines}
            )
        # Check if it's a SQL cell (starts with # MAGIC %sql)
        elif re.match(r"# MAGIC %sql", cell_content):
            # Extract SQL content
            sql_lines = []
            for line in cell_content.split("\n"):
                if line.startswith("# MAGIC %sql"):
                    # Replace with %%sql magic command
                    sql_lines.append("%%sql")
                elif line.startswith("# MAGIC "):
                    # Remove the MAGIC prefix
                    sql_lines.append(line[8:])
                else:
                    sql_lines.append(line)

            # Create a code cell with SQL
            notebook["cells"].append(
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "source": sql_lines,
                    "outputs": [],
                }
            )
        else:
            # It's a regular code cell
            # Create a code cell
            notebook["cells"].append(
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "source": cell_content.split("\n"),
                    "outputs": [],
                }
            )

    # If output_path is provided, write to file
    if output_path:
        # Write the Jupyter notebook
        with open(output_path, "w") as f:
            json.dump(notebook, f, indent=2)
        return output_path

    # If no output_path, return the notebook object
    return notebook


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python databricks_to_jupyter.py <input_notebook.py> [output_notebook.ipynb]"
        )
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        result = convert_databricks_to_jupyter(input_path, output_path)
        if output_path:
            print(f"Converted notebook saved to {result}")
        else:
            # Print the converted notebook to stdout
            print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error converting notebook: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
