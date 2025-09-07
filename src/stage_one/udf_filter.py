#!/usr/bin/env python3
"""
UDF Filter - Extracts User-Defined Functions from CFG+CALL subgraphs
Uses line-copying approach to preserve original DOT formatting exactly.
Keeps only user-defined methods, their CFG bodies, and CALL edges between UDFs.
"""

import os
import sys
import argparse
import re
from collections import defaultdict, deque

def extract_node_minimal_info(dot_content):
    """Extract minimal node info for UDF detection without complex parsing"""
    nodes_info = {}
    
    lines = dot_content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for node pattern: "node_id" [
        node_match = re.match(r'"([^"]+)"\s*\[', line)
        if node_match:
            node_id = node_match.group(1)
            
            # Collect full node definition for minimal parsing
            if line.endswith('"];'):
                full_def = line
            else:
                full_lines = [line]
                j = i + 1
                while j < len(lines) and j < len(lines):
                    next_line = lines[j]
                    full_lines.append(next_line)
                    if next_line.rstrip().endswith('"];'):
                        break
                    j += 1
                i = j
                full_def = '\n'.join(full_lines)
            
            # Extract just the essential attributes for UDF detection
            attrs = {}
            
            # Extract label
            label_match = re.search(r'label="([^"]*)"', full_def)
            if label_match:
                attrs['label'] = label_match.group(1)
            
            # Extract IS_EXTERNAL
            external_match = re.search(r'IS_EXTERNAL="([^"]*)"', full_def)
            if external_match:
                attrs['IS_EXTERNAL'] = external_match.group(1)
            
            # Extract NAME
            name_match = re.search(r'NAME="([^"]*)"', full_def)
            if name_match:
                attrs['NAME'] = name_match.group(1)
            
            # Extract FULL_NAME
            full_name_match = re.search(r'FULL_NAME="([^"]*)"', full_def)
            if full_name_match:
                attrs['FULL_NAME'] = full_name_match.group(1)
            
            # Extract FILENAME
            filename_match = re.search(r'FILENAME="([^"]*)"', full_def)
            if filename_match:
                attrs['FILENAME'] = filename_match.group(1)
            
            # Extract AST_PARENT_FULL_NAME
            ast_parent_match = re.search(r'AST_PARENT_FULL_NAME="([^"]*)"', full_def)
            if ast_parent_match:
                attrs['AST_PARENT_FULL_NAME'] = ast_parent_match.group(1)
            
            nodes_info[node_id] = attrs
        
        i += 1
    
    return nodes_info

def is_user_defined_method(node_attrs):
    """
    Determine if a METHOD node is user-defined (UDF).
    Criteria:
    - IS_EXTERNAL != "true"
    - Not from <includes> or <empty> files
    - Not synthetic <operator>.* methods
    - Keep main function as UDF
    """
    label = node_attrs.get('label', '')
    if label != 'METHOD':
        return False
    
    # Check IS_EXTERNAL attribute
    is_external = node_attrs.get('IS_EXTERNAL', 'false')
    if is_external.lower() == 'true':
        return False
    
    # Get method properties
    name = node_attrs.get('NAME', '')
    full_name = node_attrs.get('FULL_NAME', '')
    filename = node_attrs.get('FILENAME', '')
    ast_parent = node_attrs.get('AST_PARENT_FULL_NAME', '')
    
    # Filter out synthetic/operator methods
    if name.startswith('<operator>') or full_name.startswith('<operator>'):
        return False
    if name == '<clinit>' or name == '<global>':
        return False
    
    # Filter out include-driven or empty sources
    if filename in ('<includes>', '<empty>', ''):
        return False
    if '<includes>' in ast_parent:
        return False
    
    # Keep main function as UDF (as requested)
    # All other methods that pass the filters are UDFs
    return True

def extract_edges_simple(dot_content):
    """Extract edges from DOT content - return as raw strings with metadata"""
    edge_lines = []
    cfg_adjacency = defaultdict(list)
    call_edges = []
    cfg_edges = []
    
    for line in dot_content.split('\n'):
        line_stripped = line.strip()
        
        # Look for edge pattern: "source" -> "target" [label="EDGE_TYPE" ...];
        edge_match = re.match(r'"([^"]+)"\s*->\s*"([^"]+)"\s*\[.*label="([^"]+)".*\];', line_stripped)
        if edge_match:
            source = edge_match.group(1)
            target = edge_match.group(2)
            edge_type = edge_match.group(3)
            
            edge_info = {
                'source': source,
                'target': target,
                'type': edge_type,
                'line': line  # Store original line
            }
            
            if edge_type == 'CFG':
                cfg_adjacency[source].append(target)
                cfg_edges.append(edge_info)
            elif edge_type == 'CALL':
                call_edges.append(edge_info)
            
            edge_lines.append(edge_info)
    
    return edge_lines, cfg_adjacency, cfg_edges, call_edges

def extract_nodes_simple(dot_content, node_ids):
    """Extract node definitions for given node IDs - return as raw strings"""
    node_lines = {}
    
    lines = dot_content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for node pattern: "node_id" [
        node_match = re.match(r'"([^"]+)"\s*\[', line)
        if node_match:
            node_id = node_match.group(1)
            
            if node_id in node_ids:
                # Found a target node - collect the full definition
                original_line = lines[i]  # Keep original indentation
                
                # If line doesn't end with "];, we need to collect more lines
                if not line.endswith('"];'):
                    full_lines = [original_line]
                    j = i + 1
                    while j < len(lines):
                        next_original_line = lines[j]
                        full_lines.append(next_original_line)
                        if next_original_line.rstrip().endswith('"];'):
                            break
                        j += 1
                    i = j  # Skip the lines we've consumed
                    
                    # Join all lines preserving original formatting exactly
                    complete_node = '\n'.join(full_lines)
                    node_lines[node_id] = complete_node
                else:
                    # Single line definition
                    node_lines[node_id] = original_line
        
        i += 1
    
    return node_lines

def filter_udf_subgraph(dot_content):
    """
    Filter to keep only:
    1. UDF METHOD nodes
    2. All CFG-reachable nodes from each UDF
    3. CFG edges between kept nodes
    4. CALL edges where both endpoints are kept and target is UDF METHOD
    """
    
    # Extract minimal node info for UDF detection
    print("Extracting node information for UDF detection...")
    nodes_info = extract_node_minimal_info(dot_content)
    print(f"Extracted info for {len(nodes_info)} nodes")
    
    # Identify UDF methods
    udf_methods = set()
    method_nodes = set()
    
    for node_id, attrs in nodes_info.items():
        if attrs.get('label') == 'METHOD':
            method_nodes.add(node_id)
            if is_user_defined_method(attrs):
                udf_methods.add(node_id)
    
    print(f"Found {len(method_nodes)} METHOD nodes, {len(udf_methods)} are UDFs")
    if udf_methods:
        print(f"UDF methods: {', '.join(list(udf_methods)[:5])}")
    
    # Extract edges
    print("Extracting edges...")
    edge_lines, cfg_adjacency, cfg_edges, call_edges = extract_edges_simple(dot_content)
    print(f"Found {len(cfg_edges)} CFG edges and {len(call_edges)} CALL edges")
    
    # Collect all CFG-reachable nodes from each UDF method
    kept_nodes = set()
    
    def bfs_cfg_reachable(start_node):
        """BFS to find all CFG-reachable nodes from start_node"""
        visited = set()
        queue = deque([start_node])
        visited.add(start_node)
        
        while queue:
            current = queue.popleft()
            for neighbor in cfg_adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        
        return visited
    
    # For each UDF, keep all CFG-reachable nodes
    print("Finding CFG-reachable nodes from UDFs...")
    for udf_id in udf_methods:
        reachable = bfs_cfg_reachable(udf_id)
        kept_nodes.update(reachable)
        print(f"UDF {udf_id}: {len(reachable)} CFG-reachable nodes")
    
    # Extract raw node definitions for kept nodes
    print("Extracting node definitions...")
    node_definitions = extract_nodes_simple(dot_content, kept_nodes)
    print(f"Extracted definitions for {len(node_definitions)} out of {len(kept_nodes)} nodes")
    
    # Filter edges
    kept_edges = []
    
    # Keep CFG edges where both endpoints are kept
    for edge in cfg_edges:
        if edge['source'] in kept_nodes and edge['target'] in kept_nodes:
            kept_edges.append(edge)
    
    # Keep CALL edges only if:
    # - Source is in kept nodes (call site in UDF body)  
    # - Target is a UDF METHOD
    for edge in call_edges:
        if edge['source'] in kept_nodes and edge['target'] in udf_methods:
            kept_edges.append(edge)
    
    cfg_kept = sum(1 for e in kept_edges if e['type'] == 'CFG')
    call_kept = sum(1 for e in kept_edges if e['type'] == 'CALL')
    
    print(f"\nFiltering results:")
    print(f"  Nodes: {len(nodes_info)} -> {len(node_definitions)}")
    print(f"  Edges: {len(edge_lines)} -> {len(kept_edges)}")
    print(f"  CFG edges kept: {cfg_kept}")
    print(f"  CALL edges kept: {call_kept}")
    
    missing_nodes = kept_nodes - set(node_definitions.keys())
    if missing_nodes:
        print(f"⚠️  {len(missing_nodes)} nodes referenced in edges but not defined")
    
    return node_definitions, kept_edges

def write_dot_file(filepath, node_definitions, edges, original_name="filtered"):
    """Write filtered nodes and edges to DOT file using raw line copying"""
    
    with open(filepath, 'w') as f:
        f.write(f'digraph udf_{original_name} {{\n')
        f.write('  // UDF-filtered subgraph\n')
        f.write('  // Only user-defined functions and their bodies\n')
        f.write(f'  // Nodes: {len(node_definitions)}, Edges: {len(edges)}\n\n')
        
        # Write nodes (preserve original formatting exactly)
        f.write('  // Node definitions\n')
        for node_id in sorted(node_definitions.keys()):
            node_def = node_definitions[node_id]
            
            # Handle both single-line and multi-line node definitions
            if '\n' in node_def:
                # Multi-line: preserve original indentation exactly
                lines = node_def.split('\n')
                # Add 2-space indent only to the first line, preserve rest as-is
                f.write(f'  {lines[0]}\n')
                for line in lines[1:]:
                    f.write(f'{line}\n')  # Don't add extra indent
            else:
                # Single line
                f.write(f'  {node_def}\n')
        
        f.write('\n  // Edge definitions\n')
        # Write edges (preserve original formatting)
        for edge in edges:
            f.write(f'  {edge["line"]}\n')
        
        f.write('}\n')

def main():
    parser = argparse.ArgumentParser(
        description='Filter CFG+CALL subgraph to keep only user-defined functions'
    )
    parser.add_argument('input_dot', help='Input DOT file (CFG+CALL subgraph)')
    parser.add_argument('output_dir', help='Output directory for filtered DOT file')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_dot):
        print(f"Error: Input file not found: {args.input_dot}")
        sys.exit(1)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Get base name for output
    base_name = os.path.splitext(os.path.basename(args.input_dot))[0]
    output_file = os.path.join(args.output_dir, f'{base_name}_udf_filtered.dot')
    
    print(f"Processing: {args.input_dot}")
    print("="*60)
    
    # Read input DOT file
    with open(args.input_dot, 'r') as f:
        dot_content = f.read()
    
    # Filter to keep only UDF subgraph
    node_definitions, kept_edges = filter_udf_subgraph(dot_content)
    
    # Write output using line-copying approach
    write_dot_file(output_file, node_definitions, kept_edges, base_name)
    
    print("\n" + "="*60)
    print("✅ COMPLETED")
    print(f"📁 Output: {output_file}")
    print(f"📊 Final graph: {len(node_definitions)} nodes, {len(kept_edges)} edges")

if __name__ == '__main__':
    main()