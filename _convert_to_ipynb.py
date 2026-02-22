#!/usr/bin/env python3
import json, re, sys, os
def convert(py_path, ipynb_path):
    with open(py_path, 'r') as f: lines = f.readlines()
    cells, cl, ct, title = [], [], None, None
    def flush():
        nonlocal cl, ct, title
        if ct is None: return
        b = cl[:]
        while b and b[-1].strip() == '': b.pop()
        if not b and ct == 'code': cl, ct, title = [], None, None; return
        cell = {"cell_type": ct, "metadata": {}, "source": [l+('\n' if i<len(b)-1 else '') for i,l in enumerate(b)]}
        if ct == 'code': cell["execution_count"] = None; cell["outputs"] = []
        if title: cell["metadata"]["application/vnd.databricks.v1+cell"] = {"title": title}
        cells.append(cell); cl, ct, title = [], None, None
    skip, i = True, 0
    while i < len(lines):
        line = lines[i].rstrip('\n')
        if skip and line.strip() == '# Databricks notebook source': skip = False; i+=1; continue
        if line.strip() == '# COMMAND ----------': flush(); i+=1; continue
        m = re.match(r'^# DBTITLE\s+\d+,(.*)$', line)
        if m: title = m.group(1).strip(); i+=1; continue
        if line.strip().startswith('# MAGIC %md'):
            flush(); ct = 'markdown'; rest = line.strip().replace('# MAGIC %md','',1).strip()
            if rest: cl.append(rest)
            i+=1; continue
        if ct == 'markdown' and line.strip().startswith('# MAGIC'):
            c = line.split('# MAGIC',1)[1]
            if c.startswith(' '): c = c[1:]
            cl.append(c); i+=1; continue
        if ct == 'markdown' and not line.strip().startswith('# MAGIC'): flush()
        if ct != 'markdown':
            if ct is None: ct = 'code'
            cl.append(line)
        i += 1
    flush()
    nb = {"nbformat":4,"nbformat_minor":5,"metadata":{"kernelspec":{"display_name":"Python 3","language":"python","name":"python3"},"language_info":{"name":"python","version":"3.12.0"}},"cells":cells}
    os.makedirs(os.path.dirname(ipynb_path), exist_ok=True)
    with open(ipynb_path,'w') as f: json.dump(nb, f, indent=1)
    print(f"  ✅ {os.path.basename(py_path)} → {ipynb_path}")
if __name__ == '__main__':
    base = os.path.dirname(__file__)
    for name in (sys.argv[1:] or ['07_genie_deployment']):
        convert(os.path.join(base,'src',f'{name}.py'), os.path.join(base,'notebooks',f'{name}.ipynb'))
