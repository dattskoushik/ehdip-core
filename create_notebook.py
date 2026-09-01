import json
import sys

def create_notebook(filepath, content):
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [line + '\n' for line in content.split('\n')]
            }
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5
    }
    with open(filepath, 'w') as f:
        json.dump(notebook, f, indent=2)

if __name__ == "__main__":
    filepath = sys.argv[1]
    with open(sys.argv[2], 'r') as f:
        content = f.read()
    create_notebook(filepath, content)
