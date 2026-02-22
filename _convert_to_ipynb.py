#!/usr/bin/env python3
import json, re, sys, os

def convert(py_path, ipynb_path):
    with open(py_path, 'r') as f:
        lines = f.readlines()
    cells, current_lines, current_type, current_title = [], [], None, None

    def flush():
        nonlocal current_lines, current_type, current_title
        if current_type is None:
            return
        body = current_lines[:]
        while body and body[-1].strip() == '':
            body.pop()
        if not body and current_type == 'code':
            current_lines, current_type, current_title = [], None, None
            return
        cell = {"cell_type": current_type, "metadata": {},
                "source": [l + ('\n' if i < len(body) - 1 else '') for i, l in enumerate(body)]}
        if current_type == 'code':
            cell["execution_count"] = None
            cell["outputs"] = []
        if current_title:
            cell["metadata"]["application/vnd.databricks.v1+cell"] = {"title": current_title}
        cells.append(cell)
        current_lines, current_type, current_title = [], None, None

    skip_first, i = True, 0
    while i < len(lines):
        line = lines[i].rstrip('\n')
        if skip_first and line.strip() == '# Databricks notebook source':
            skip_first = False; i += 1; continue
        if line.strip() == '# COMMAND ----------':
            flush(); i += 1; continue
        m = re.match(r'^# DBTITLE\s+\d+,(.*)$', line)
        if m:
            current_title = m.group(1).strip(); i += 1; continue
        if line.strip().startswith('# MAGIC %md'):
            flush(); current_type = 'markdown'
            rest = line.strip().replace('# MAGIC %md', '', 1).strip()
            if rest: current_lines.append(rest)
            i += 1; continue
        if current_type == 'markdown' and line.strip().startswith('# MAGIC'):
            content = line.split('# MAGIC', 1)[1]
            if content.startswith(' '): content = content[1:]
            current_lines.append(content); i += 1; continue
        if current_type == 'markdown' and not line.strip().startswith('# MAGIC'):
            flush()
        if current_type != 'markdown':
            if current_type is None: current_type = 'code'
            current_lines.append(line)
        i += 1
    flush()

    nb = {"nbformat": 4, "nbformat_minor": 5,
          "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                       "language_info": {"name": "python", "version": "3.12.0"}},
          "cells": cells}
    os.makedirs(os.path.dirname(ipynb_path), exist_ok=True)
    with open(ipynb_path, 'w') as f:
        json.dump(nb, f, indent=1)
    print(f"  ✅ {os.path.basename(py_path)} → {ipynb_path}")

if __name__ == '__main__':
    base = os.path.dirname(__file__)
    for name in (sys.argv[1:] or ['04_query_intelligence']):
        convert(os.path.join(base, 'src', f'{name}.py'), os.path.join(base, 'notebooks', f'{name}.ipynb'))
