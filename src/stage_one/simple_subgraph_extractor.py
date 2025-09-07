#!/usr/bin/env python3
"""
Simple, direct subgraph extractor that preserves original DOT format.
No complex parsing - just find edges, find connected nodes, copy as-is.
"""

import os
import sys
import re
import argparse

def extract_edges_simple(dot_content, edge_types):
    """Find all edges of specified types - return as raw strings"""
    edge_lines = []
    connected_nodes = set()
    
    # Simple regex to find edge lines
    for line in dot_content.split('\n'):
        line = line.strip()
        
        # Look for edge pattern: "node1" -> "node2" [label="EDGE_TYPE" ...];
        edge_match = re.match(r'"([^"]+)"\s*->\s*"([^"]+)"\s*\[.*label="([^"]+)".*\];', line)
        if edge_match:
            source = edge_match.group(1)
            target = edge_match.group(2) 
            edge_type = edge_match.group(3)
            
            if edge_type in edge_types:
                edge_lines.append(line)
                connected_nodes.add(source)
                connected_nodes.add(target)
    
    return edge_lines, connected_nodes

def extract_nodes_simple(dot_content, node_ids):
    """Find all node definitions for given node IDs - return as raw strings"""
    node_lines = []
    found_nodes = set()
    
    # Split content and track multiline definitions
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
                current_line = line
                original_line = lines[i]  # Keep original indentation
                
                # If line doesn't end with "]; we need to collect more lines
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
                    node_lines.append(complete_node)
                else:
                    # Single line definition
                    node_lines.append(original_line)
                
                found_nodes.add(node_id)
        
        i += 1
    
    return node_lines, found_nodes

def create_simple_subgraph(dot_content, edge_types, output_file):
    """Create subgraph by directly copying relevant lines from original"""
    
    print(f"Extracting edges of types: {', '.join(edge_types)}...")
    edge_lines, connected_nodes = extract_edges_simple(dot_content, edge_types)
    print(f"Found {len(edge_lines)} edges connecting {len(connected_nodes)} nodes")
    
    print("Extracting node definitions...")
    node_lines, found_nodes = extract_nodes_simple(dot_content, connected_nodes)
    print(f"Found definitions for {len(found_nodes)} out of {len(connected_nodes)} nodes")
    
    # Report missing nodes
    missing_nodes = connected_nodes - found_nodes
    if missing_nodes:
        print(f"Missing node definitions: {len(missing_nodes)} nodes")
        for node_id in list(missing_nodes)[:5]:  # Show first 5
            print(f"  - {node_id}")
        if len(missing_nodes) > 5:
            print(f"  - ... and {len(missing_nodes) - 5} more")
    
    # Write the subgraph
    subgraph_name = "_".join(edge_types)
    
    with open(output_file, 'w') as f:
        f.write(f'digraph subgraph_{subgraph_name} {{\n')
        f.write(f'  // Direct extraction from original DOT file\n')
        f.write(f'  // Edge types: {", ".join(edge_types)}\n')
        f.write(f'  // Nodes: {len(found_nodes)}, Edges: {len(edge_lines)}\n\n')
        
        # Write nodes (preserve original formatting)
        f.write('  // Node definitions\n')
        for node_def in node_lines:
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
        for line in edge_lines:
            f.write(f'  {line}\n')
        
        f.write('}\n')
    
    print(f"Created subgraph: {output_file}")
    return len(found_nodes), len(edge_lines), missing_nodes


def main():
    parser = argparse.ArgumentParser(
        description='Simple subgraph extractor - direct copying from original DOT'
    )
    parser.add_argument('input_dot', help='Input DOT file path')
    parser.add_argument('output_dir', help='Output directory')
    parser.add_argument('--edge-types', nargs='+', default=['CFG', 'CALL'],
                       help='Edge types to extract (default: CFG CALL)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_dot):
        print(f"Error: Input file not found: {args.input_dot}")
        sys.exit(1)
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    print(f"Reading DOT file: {args.input_dot}")
    with open(args.input_dot, 'r') as f:
        dot_content = f.read()
    
    edge_types = set(args.edge_types)
    subgraph_name = "_".join(args.edge_types)
    
    # File path for extracted subgraph
    output_file = os.path.join(args.output_dir, f'{subgraph_name}_original.dot')
    
    print("\n" + "="*60)
    print("EXTRACTING SUBGRAPH")
    print("="*60)
    
    node_count, edge_count, missing_nodes = create_simple_subgraph(
        dot_content, edge_types, output_file)
    
    print("\n" + "="*60)
    print("✅ COMPLETED")
    print("="*60)
    print(f"📁 Output directory: {os.path.abspath(args.output_dir)}")
    print(f"📊 Extracted: {node_count} nodes, {edge_count} edges")
    print(f"📄 File created: {subgraph_name}_original.dot")
    
    if missing_nodes:
        print(f"⚠️  {len(missing_nodes)} nodes referenced in edges but not defined")

if __name__ == '__main__':
    main()