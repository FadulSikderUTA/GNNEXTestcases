#!/usr/bin/env python3
"""
Schema reporter for UDF DOT graphs.

What it does
------------
1) Recursively finds all UDF DOTs under a root directory
   (default glob: **/udf/CFG_CALL_original_udf_filtered.dot).

2) For every file f:
   - Parses nodes robustly (handles multi-line quoted CODE).
   - Builds T(f): the set of node types (from 'label="..."').
   - For each node type T in f:
       * P_any_f(T): union of attribute keys seen on any T-node in f.
       * P_all_f(T): intersection of attribute keys seen on every T-node in f.
     (Both exclude the special 'label' key.)

3) Across files:
   - Node-types:
       * intersectional_types = ⋂_f T(f)
       * non_intersectional_types = (⋃_f T(f)) \ intersectional_types
       * presence lists per type (which files they appear in)
   - For each node type T (intersectional or not):
       * LENIENT (per-file "any"):
           P_intersection_any(T) = ⋂_{f∈F_T} P_any_f(T)
           P_union_any(T)        = ⋃_{f∈F_T} P_any_f(T)
           P_non_any(T)          = P_union_any(T) \ P_intersection_any(T)
       * STRICT (per-file "all nodes"):
           P_intersection_all(T) = ⋂_{f∈F_T} P_all_f(T)
           P_union_all(T)        = ⋃_{f∈F_T} P_all_f(T)
           P_non_all(T)          = P_union_all(T) \ P_intersection_all(T)
       * also preserves by-file detail for T:
           by_file: { file: { "any": [...], "all": [...] }, ... }

   - Cross-type property views (dataset level):
       * LENIENT across types:
           inter_types_any = ⋂_T P_union_any(T)
           union_types_any = ⋃_T P_union_any(T)
           non_types_any   = union_types_any \ inter_types_any
           coverage_any[p] = sorted list of types T where p ∈ P_union_any(T)
       * STRICT across types:
           inter_types_all = ⋂_T P_intersection_all(T)   # very strict
           union_types_all = ⋃_T P_intersection_all(T)
           non_types_all   = union_types_all \ inter_types_all

   - Global property union:
       global_property_union = union_types_any

Outputs (JSON, sorted)
----------------------
<out_dir>/schema_report/
  - node_types.json
  - properties_by_type.json
  - properties_cross_types.json
  - properties_global.json

Usage
-----
python schema_report.py \
  --root /home/fadul/GNNTestcases/pipeline_artifacts \
  --out  /home/fadul/GNNTestcases/pipeline_artifacts/schema_report \
  --pattern "udf/CFG_CALL_original_udf_filtered.dot"

Notes
-----
- Only node lines are parsed; edge lines (e.g., CFG/CALL) are ignored.
- A property key is considered "present" if the key exists on the node
  regardless of whether its value is an empty string (matches your exports).
- The parser is a small state machine that tolerates multi-line quoted values
  (e.g., CODE) and avoids false positives inside quotes.
"""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple, Iterable, Optional
from dataclasses import dataclass, field
from datetime import datetime

# ---------------------------
# DOT parsing (nodes only)
# ---------------------------

def _iter_udf_dot_files(root: Path, pattern: str) -> List[Path]:
    """
    Recursively find files matching the given pattern fragment.
    E.g., pattern="udf/CFG_CALL_original_udf_filtered.dot"
    """
    # Use simple walk to be platform-neutral regarding globstar.
    out = []
    pat_parts = Path(pattern).parts
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            p = Path(dirpath) / fn
            # Fast check: file name must match last part
            if fn != pat_parts[-1]:
                continue
            # Check the tail of the relative path matches the pattern tail
            rel = p.relative_to(root)
            rel_parts = rel.parts
            if len(rel_parts) >= len(pat_parts) and tuple(rel_parts[-len(pat_parts):]) == pat_parts:
                out.append(p)
    out.sort()
    return out

def _node_start(line: str) -> Optional[Tuple[str, str]]:
    """
    If this line starts a node block like:
        "123" [ ...   (possibly continues)
    return (node_id, remainder_after_open_bracket)
    Else return None.

    Reject edge lines that look like:
        "src" -> "dst" [ ...
    """
    s = line.strip()
    if not s.startswith('"'):
        return None
    # Reject edges
    if '->' in s:
        return None
    # Find node id
    i = 1
    n = len(s)
    while i < n and s[i] != '"':
        i += 1
    if i >= n:
        return None
    node_id = s[1:i]
    j = i + 1
    # Skip spaces
    while j < n and s[j].isspace():
        j += 1
    if j < n and s[j] == '[':
        # Capture remainder after the first '['
        return node_id, s[j+1:]
    return None

def _scan_until_node_end(accum: List[str], line: str, in_string: bool) -> Tuple[bool, bool]:
    """
    Append to accum until the closing sequence ']' followed by ';' appears
    outside double quotes. Return (done, in_string_new).
    """
    i = 0
    n = len(line)
    escape = False
    done = False
    buf = []
    while i < n:
        c = line[i]
        buf.append(c)
        if in_string:
            if not escape and c == '\\':
                escape = True
            elif escape:
                escape = False
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == ']':
                # Look ahead for optional spaces then ';'
                k = i + 1
                while k < n and line[k].isspace():
                    buf.append(line[k])
                    k += 1
                if k < n and line[k] == ';':
                    buf.append(';')
                    # We include up to just before '];' in accum; ignore the '];'
                    # Remove the last 2 chars we just appended (']' + following spaces + ';')
                    # Easier: truncate buf to exclude '];' and spaces
                    # Find position of ']' in buf: current position is len(buf)-1 (was ']')
                    # We'll slice the buf to exclude ']' and any following spaces and ';'
                    # First, compute how many trailing chars to drop from buf (from i where ']' was)
                    # But simpler: we will not append '];' actually: revert to before ']' wrote.
                    # Rebuild without the trailing detected end
                    buf = buf[:-1]  # drop the ']'
                    # Remove any spaces we appended after ']' (if any)
                    while buf and buf[-1].isspace():
                        buf.pop()
                    # Do not include ';'
                    done = True
                    # Consume the rest of line but ignore
                    i = n  # will break
                    break
        i += 1
    if buf:
        accum.append(''.join(buf))
    return done, in_string

def _parse_attr_pairs(attr_text: str) -> Dict[str, str]:
    """
    Parse key=value pairs from text inside the brackets, respecting quotes.
    Returns dict of key -> string value (without quotes, unescaped simple escapes).
    We only need keys, but we also want the 'label' value for node type.
    """
    d: Dict[str, str] = {}
    i, n = 0, len(attr_text)
    in_string = False
    escape = False
    while i < n:
        # Skip whitespace and commas
        while i < n and attr_text[i] in ' \t\r\n,':
            i += 1
        if i >= n:
            break
        # Parse key (identifier)
        start = i
        if not (attr_text[i].isalpha() or attr_text[i] == '_'):
            # Not a key; advance one char (defensive)
            i += 1
            continue
        i += 1
        while i < n and (attr_text[i].isalnum() or attr_text[i] == '_'):
            i += 1
        key = attr_text[start:i]
        # Skip spaces
        while i < n and attr_text[i].isspace():
            i += 1
        # Expect '='
        if i >= n or attr_text[i] != '=':
            # malformed; skip
            continue
        i += 1
        # Skip spaces
        while i < n and attr_text[i].isspace():
            i += 1
        # Parse value
        if i < n and attr_text[i] == '"':
            # Quoted string
            i += 1
            val_chars = []
            escape = False
            while i < n:
                c = attr_text[i]
                if escape:
                    # keep escaped char as-is but unescape common \'\"\\
                    if c == 'n':
                        val_chars.append('\n')
                    elif c == 't':
                        val_chars.append('\t')
                    else:
                        val_chars.append(c)
                    escape = False
                else:
                    if c == '\\':
                        escape = True
                    elif c == '"':
                        i += 1
                        break
                    else:
                        val_chars.append(c)
                i += 1
            val = ''.join(val_chars)
        else:
            # Bare token: read until whitespace or comma
            start_val = i
            while i < n and (not attr_text[i].isspace()) and attr_text[i] not in ',]':
                i += 1
            val = attr_text[start_val:i]
        d[key] = val
    return d

@dataclass
class FileTypeProps:
    types_in_file: Set[str] = field(default_factory=set)
    # Per type in this file:
    p_any: Dict[str, Set[str]] = field(default_factory=dict)  # union over nodes of type
    p_all: Dict[str, Set[str]] = field(default_factory=dict)  # intersection over nodes of type

def parse_dot_file(path: Path) -> FileTypeProps:
    """
    Streaming parse of a DOT file to collect:
      - node types present in file
      - per-type property sets: union across nodes ("any") and intersection across nodes ("all")
    """
    out = FileTypeProps()
    with path.open('r', encoding='utf-8', errors='replace') as f:
        in_node = False
        node_id = None
        in_string = False
        attr_accum: List[str] = []
        for raw in f:
            if not in_node:
                maybe = _node_start(raw)
                if maybe is None:
                    continue
                node_id, remainder = maybe
                in_node = True
                attr_accum = []
                done, in_string = _scan_until_node_end(attr_accum, remainder, in_string=False)
                if done:
                    # Process node now
                    attr_text = ''.join(attr_accum)
                    attrs = _parse_attr_pairs(attr_text)
                    _update_file_props(out, attrs)
                    in_node = False
                    node_id = None
                    attr_accum = []
                # else continue accumulating
            else:
                done, in_string = _scan_until_node_end(attr_accum, raw, in_string=in_string)
                if done:
                    attr_text = ''.join(attr_accum)
                    attrs = _parse_attr_pairs(attr_text)
                    _update_file_props(out, attrs)
                    in_node = False
                    node_id = None
                    attr_accum = []
    return out

def _update_file_props(ftp: FileTypeProps, attrs: Dict[str, str]) -> None:
    """
    Given parsed attributes for one node, update:
      - set of node types in file
      - P_any(T) and P_all(T) for that node's type
    """
    if 'label' not in attrs:
        return
    node_type = attrs['label']
    # Keys on this node excluding 'label'
    keys = set(attrs.keys()) - {'label'}
    ftp.types_in_file.add(node_type)
    # union ("any")
    if node_type not in ftp.p_any:
        ftp.p_any[node_type] = set()
    ftp.p_any[node_type].update(keys)
    # intersection ("all")
    if node_type not in ftp.p_all:
        # initialize with keys of first node of this type
        ftp.p_all[node_type] = set(keys)
    else:
        ftp.p_all[node_type] &= keys

# ---------------------------
# Set ops across files
# ---------------------------

def sorted_list(s: Iterable[str]) -> List[str]:
    return sorted(s)

def compute_reports(root: Path, pattern: str, out_dir: Path) -> None:
    files = _iter_udf_dot_files(root, pattern)
    if not files:
        raise SystemExit(f"No files matched pattern '{pattern}' under {root}")

    # Per-file summaries
    per_file: Dict[str, FileTypeProps] = {}
    rel_files: List[str] = []
    for p in files:
        rel = str(p.relative_to(root))
        rel_files.append(rel)
        per_file[rel] = parse_dot_file(p)

    # Node type presence
    types_by_file = {rel: props.types_in_file for rel, props in per_file.items()}
    all_types: Set[str] = set().union(*types_by_file.values()) if types_by_file else set()
    inter_types: Set[str] = set.intersection(*types_by_file.values()) if types_by_file else set()
    non_inter_types: Set[str] = all_types - inter_types

    # Presence map per type
    present_in: Dict[str, List[str]] = {
        t: sorted([rel for rel, props in per_file.items() if t in props.types_in_file])
        for t in all_types
    }

    # Per-type property aggregation across files (lenient & strict)
    by_type_report: Dict[str, dict] = {}
    # Keep for cross-type derived views
    union_any_by_type: Dict[str, Set[str]] = {}
    inter_all_by_type: Dict[str, Set[str]] = {}

    for t in sorted(all_types):
        # Collect files where T exists
        FT = [rel for rel, props in per_file.items() if t in props.types_in_file]
        # Per-file sets
        per_file_any = {rel: per_file[rel].p_any[t] for rel in FT}
        per_file_all = {rel: per_file[rel].p_all[t] for rel in FT}

        # LENIENT across files for T
        P_union_any = set().union(*per_file_any.values()) if per_file_any else set()
        P_inter_any = set.intersection(*per_file_any.values()) if per_file_any else set()
        P_non_any   = P_union_any - P_inter_any

        # STRICT across files for T
        P_union_all = set().union(*per_file_all.values()) if per_file_all else set()
        P_inter_all = set.intersection(*per_file_all.values()) if per_file_all else set()
        P_non_all   = P_union_all - P_inter_all

        # Record for cross-type later
        union_any_by_type[t] = P_union_any
        inter_all_by_type[t] = P_inter_all

        # by-file detail (both views)
        by_file_detail = {
            rel: {
                "any": sorted_list(per_file_any[rel]),
                "all": sorted_list(per_file_all[rel]),
            }
            for rel in FT
        }

        by_type_report[t] = {
            "files_present_in": FT,
            "lenient": {
                "intersection": sorted_list(P_inter_any),
                "non_intersection": sorted_list(P_non_any),
                "union": sorted_list(P_union_any)
            },
            "strict": {
                "intersection_all_nodes": sorted_list(P_inter_all),
                "non_intersection_all_nodes": sorted_list(P_non_all),
                "union_all_nodes": sorted_list(P_union_all)
            },
            "by_file": by_file_detail
        }

    # Cross-type property views
    if all_types:
        inter_types_any = set.intersection(*union_any_by_type.values()) if union_any_by_type else set()
        union_types_any = set().union(*union_any_by_type.values()) if union_any_by_type else set()
        non_types_any   = union_types_any - inter_types_any

        inter_types_all = set.intersection(*inter_all_by_type.values()) if inter_all_by_type else set()
        union_types_all = set().union(*inter_all_by_type.values()) if inter_all_by_type else set()
        non_types_all   = union_types_all - inter_types_all
    else:
        inter_types_any = union_types_any = non_types_any = set()
        inter_types_all = union_types_all = non_types_all = set()

    # Coverage (lenient): for each property, which types have it (in union_any)
    coverage_any: Dict[str, List[str]] = {}
    for t, props in union_any_by_type.items():
        for p in props:
            coverage_any.setdefault(p, []).append(t)
    for p in coverage_any:
        coverage_any[p].sort()

    # Global property union (lenient)
    global_union = sorted_list(union_types_any)

    # Write outputs
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().isoformat() + "Z"

    node_types_json = {
        "generated_at_utc": ts,
        "root": str(root),
        "pattern": pattern,
        "files": rel_files,
        "intersectional_types": sorted_list(inter_types),
        "non_intersectional_types": [
            {"type": t, "present_in": present_in[t]} for t in sorted_list(non_inter_types)
        ],
        "presence_matrix": {
            t: {rel: (1 if t in types_by_file[rel] else 0) for rel in rel_files}
            for t in sorted_list(all_types)
        }
    }
    (out_dir / "node_types.json").write_text(json.dumps(node_types_json, indent=2), encoding="utf-8")

    properties_by_type_json = {
        "generated_at_utc": ts,
        "root": str(root),
        "pattern": pattern,
        "types": by_type_report
    }
    (out_dir / "properties_by_type.json").write_text(json.dumps(properties_by_type_json, indent=2), encoding="utf-8")

    properties_cross_types_json = {
        "generated_at_utc": ts,
        "root": str(root),
        "pattern": pattern,
        "lenient": {
            "intersection_across_types": sorted_list(inter_types_any),
            "union_across_types": sorted_list(union_types_any),
            "non_intersection_across_types": sorted_list(non_types_any),
            "coverage": coverage_any
        },
        "strict": {
            "intersection_across_types_all_nodes": sorted_list(inter_types_all),
            "union_across_types_all_nodes": sorted_list(union_types_all),
            "non_intersection_across_types_all_nodes": sorted_list(non_types_all)
        }
    }
    (out_dir / "properties_cross_types.json").write_text(json.dumps(properties_cross_types_json, indent=2), encoding="utf-8")

    properties_global_json = {
        "generated_at_utc": ts,
        "root": str(root),
        "pattern": pattern,
        "global_property_union": global_union
    }
    (out_dir / "properties_global.json").write_text(json.dumps(properties_global_json, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser(description="Build schema reports from UDF DOT graphs.")
    ap.add_argument("--root", required=True, type=Path, help="Root directory (e.g., /home/fadul/GNNTestcases/pipeline_artifacts)")
    ap.add_argument("--out", required=True, type=Path, help="Output directory for schema_report/*.json")
    ap.add_argument("--pattern", default="udf/CFG_CALL_original_udf_filtered.dot",
                    help="Tail path to match under root (default: %(default)s)")
    args = ap.parse_args()
    compute_reports(args.root, args.pattern, args.out)

if __name__ == "__main__":
    main()