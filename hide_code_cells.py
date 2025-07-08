#!/usr/bin/env python3
"""
Script to hide all code cells in a Jupyter notebook for presentation.
"""

import json
import sys

def hide_all_code_cells(notebook_path):
    """Hide all code cells in the notebook."""

    # Read the notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)

    # Process each cell
    for cell_idx, cell in enumerate(notebook['cells']):
        if cell['cell_type'] == 'code':
            # Initialize metadata if it doesn't exist
            if 'metadata' not in cell:
                cell['metadata'] = {}

            # Hide the input (code)
            cell['metadata']['hide_input'] = True

            # Set slide type based on whether it has outputs
            has_outputs = 'outputs' in cell and cell['outputs']
            if has_outputs:
                cell['metadata']['slideshow'] = {'slide_type': 'slide'}
                print(f"Cell {cell_idx}: Code hidden, outputs shown")
            else:
                cell['metadata']['slideshow'] = {'slide_type': 'skip'}
                print(f"Cell {cell_idx}: Code hidden, cell skipped")

        elif cell['cell_type'] == 'markdown':
            # Initialize metadata if it doesn't exist
            if 'metadata' not in cell:
                cell['metadata'] = {}

            content = ''.join(cell['source'])
            lines = content.strip().split('\n')

            # Skip specific empty cells (10, 13, 14)
            if cell_idx in [10, 13, 14]:
                cell['metadata']['slideshow'] = {'slide_type': 'skip'}
                print(f"Cell {cell_idx}: Empty cell skipped")
            # Show first markdown cell (title) and explanatory markdown cells
            elif cell_idx == 0:
                # Title slide
                cell['metadata']['slideshow'] = {'slide_type': 'slide'}
                print(f"Cell {cell_idx}: Title slide shown")
            elif len(lines) == 1 and lines[0].startswith('#'):
                # Skip pure headers
                cell['metadata']['slideshow'] = {'slide_type': 'skip'}
                print(f"Cell {cell_idx}: Header skipped")
            else:
                # Show explanatory markdown (longer content)
                cell['metadata']['slideshow'] = {'slide_type': 'slide'}
                print(f"Cell {cell_idx}: Explanatory markdown shown")

    # Save the modified notebook
    with open(notebook_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Modified notebook saved: {notebook_path}")

if __name__ == "__main__":
    notebook_path = "notebooks/Kraftwerksanalyse_komplett.ipynb"
    hide_all_code_cells(notebook_path)
