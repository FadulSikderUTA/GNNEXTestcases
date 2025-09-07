#!/usr/bin/env python3
"""
HTML DOT Generator - Decoupled Script
Takes a DOT file and creates HTML-encoded version with PNG visualization.
Shows node ID, node type, and all attributes for better visualization.
"""

import os
import sys
import argparse
import re
import html
import subprocess
import tempfile
from collections import defaultdict

def sanitize_for_dot_label_content(text_input):
    """Sanitize text for DOT label content"""
    if text_input is None:
        return ""
    if not isinstance(text_input, str):
        text_input = str(text_input)
    # Remove carriage returns and replace newlines with spaces
    text_input = text_input.replace('\r', '').replace('\n', ' ')
    # Escape double quotes for DOT label
    text_input = text_input.replace('"', '\\"')
    # Handle other potentially problematic characters for DOT
    text_input = text_input.replace('{', '\\{').replace('}', '\\}')
    text_input = text_input.replace('<', '\\<').replace('>', '\\>')
    text_input = text_input.replace('|', '\\|')
    # Remove any other non-printable characters
    text_input = ''.join(c if c.isprintable() else ' ' for c in text_input)
    return text_input

def parse_dot_file(dot_file_path):
    """Parse DOT file and extract nodes and edges with attributes"""
    print(f"Parsing DOT file: {dot_file_path}")
    
    nodes = {}
    edges = []
    
    with open(dot_file_path, 'r') as f:
        content = f.read()
    
    # Extract node definitions
    # Pattern: "node_id" [attributes];
    node_pattern = r'"([^"]+)"\s*\[(.*?)\];'
    
    # Split content into lines for multiline node handling
    lines = content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for node pattern: "node_id" [
        node_match = re.match(r'"([^"]+)"\s*\[', line)
        if node_match:
            node_id = node_match.group(1)
            
            # Collect full node definition (may span multiple lines)
            if line.endswith('"];'):
                # Single line node
                full_line = line
            else:
                # Multiline node - collect until we find the closing ];
                full_lines = [line]
                j = i + 1
                while j < len(lines):
                    next_line = lines[j]
                    full_lines.append(next_line)
                    if next_line.rstrip().endswith('"];'):
                        break
                    j += 1
                i = j  # Skip the lines we've consumed
                full_line = '\n'.join(full_lines)
            
            # Extract attributes from the full node definition
            attr_match = re.search(r'"[^"]+"\s*\[(.*?)\];', full_line, re.DOTALL)
            if attr_match:
                attrs_str = attr_match.group(1)
                attributes = parse_attributes(attrs_str)
                nodes[node_id] = attributes
        
        # Look for edge pattern: "source" -> "target" [attributes];
        edge_match = re.match(r'"([^"]+)"\s*->\s*"([^"]+)"\s*\[(.*?)\];', line)
        if edge_match:
            source = edge_match.group(1)
            target = edge_match.group(2)
            attrs_str = edge_match.group(3)
            attributes = parse_attributes(attrs_str)
            edges.append({
                'source': source,
                'target': target,
                'attributes': attributes
            })
        
        i += 1
    
    print(f"Found {len(nodes)} nodes and {len(edges)} edges")
    return nodes, edges

def parse_attributes(attrs_str):
    """Parse attribute string into dictionary"""
    attributes = {}
    
    # Handle complex attribute parsing with quoted values
    # Pattern: key="value" or key=value
    attr_pattern = r'(\w+)=(?:"([^"]*(?:\\.[^"]*)*)"|([^\s]+))'
    
    matches = re.findall(attr_pattern, attrs_str)
    for match in matches:
        key = match[0]
        # Use quoted value if present, otherwise unquoted value
        value = match[1] if match[1] else match[2]
        attributes[key] = value
    
    return attributes

def create_html_label_with_node_id(node_id, node_attributes):
    """
    Create HTML label showing:
    1. Node ID (at the top)
    2. Node Type (centered, from label attribute)
    3. All other attributes
    """
    # Get node type from label attribute
    node_type = node_attributes.get('label', 'UNKNOWN').strip('"')
    
    # Start with node ID at the top
    html_rows = [
        f'<TR><TD COLSPAN="2" BGCOLOR="lightblue" ALIGN="CENTER"><B>ID: {html.escape(node_id)}</B></TD></TR>'
    ]
    
    # Add node type in the center
    html_rows.append(
        f'<TR><TD COLSPAN="2" BGCOLOR="lightgray" ALIGN="CENTER"><B>{html.escape(node_type)}</B></TD></TR>'
    )
    
    # Sort attributes for consistent display (skip label since we used it for node type)
    sorted_attrs = sorted(node_attributes.items())
    
    for key, value in sorted_attrs:
        if key.lower() != 'label':  # Skip label since we already used it
            # Ensure value is a string and clean it
            value_str = str(value).strip('"')
            
            # Escape HTML special characters
            escaped_key = html.escape(key)
            escaped_value = html.escape(value_str)
            
            # Replace newlines with HTML breaks
            escaped_value = escaped_value.replace("\\n", "<BR/>").replace("\n", "<BR/>")
            
            # Truncate very long values for readability
            if len(escaped_value) > 100:
                escaped_value = escaped_value[:97] + "..."
            
            html_rows.append(
                f'<TR><TD ALIGN="LEFT"><B>{escaped_key}</B></TD><TD ALIGN="LEFT">{escaped_value}</TD></TR>'
            )
    
    table_content = "".join(html_rows)
    # The outer '<' and '>' are crucial for Graphviz to interpret this as HTML
    return f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">{table_content}</TABLE>>'

def get_node_color(node_type):
    """Get color for node based on its type"""
    color_map = {
        'BINDING': '#FFCCCC',
        'BLOCK': '#CCFFCC',
        'CALL': '#CCCCFF',
        'METHOD': '#FFCCCC',
        'TYPE_DECL': '#E6CCFF',
        'TYPE': '#FFE6CC',
        'TYPE_REF': '#FFE6CC',
        'LITERAL': '#FFFFCC',
        'CONTROL_STRUCTURE': '#D2B4DE',
        'FILE': '#A9A9A9',
        'META_DATA': '#ADD8E6',
        'NAMESPACE_BLOCK': '#E0FFFF',
        'IDENTIFIER': '#F5F5F5',
        'FIELD_IDENTIFIER': '#F5F5F5',
        'LOCAL': '#FFE6F7',
        'METHOD_PARAMETER_IN': '#E6E6FA',
        'METHOD_PARAMETER_OUT': '#E6E6FA',
        'METHOD_RETURN': '#F5F5F5',
        'METHOD_REF': '#F5F5F5',
        'MODIFIER': '#FAFAFA',
        'NAMESPACE': '#E0FFFF',
        'MEMBER': '#FFE6CC',
        'RETURN': '#F5F5F5',
        'DEPENDENCY': '#D3D3D3',
        'IMPORT': '#D3D3D3'
    }
    return color_map.get(node_type, '#F5F5F5')  # Default: Light Grey

def create_html_dot_file(input_dot_path, output_dot_path, nodes, edges):
    """Create HTML-encoded DOT file"""
    print(f"Creating HTML-encoded DOT file: {output_dot_path}")
    
    # Extract base name for graph
    base_name = os.path.splitext(os.path.basename(input_dot_path))[0]
    
    dot_content = []
    dot_content.append(f'digraph html_{base_name} {{')
    dot_content.append('  rankdir=LR;')
    dot_content.append('  node [shape=plain];')  # Plain shape for HTML tables
    dot_content.append('  edge [fontname="Arial", fontsize=9];')
    dot_content.append('')
    
    # Add nodes with HTML labels
    for node_id, node_attrs in nodes.items():
        html_label = create_html_label_with_node_id(node_id, node_attrs)
        
        # Get node type for coloring
        node_type = node_attrs.get('label', 'UNKNOWN').strip('"')
        fillcolor = get_node_color(node_type)
        
        dot_content.append(f'  "{node_id}" [label={html_label}, fillcolor="{fillcolor}", style="filled"];')
    
    dot_content.append('')
    
    # Add edges with their attributes
    for edge in edges:
        source = edge['source']
        target = edge['target']
        edge_attrs = edge['attributes']
        
        # Create edge label
        edge_label = edge_attrs.get('label', 'EDGE').strip('"')
        
        # Add any additional edge attributes
        extra_attrs = []
        for key, value in edge_attrs.items():
            if key.lower() not in ['label']:
                clean_value = str(value).strip('"')[:30]  # Truncate long values
                extra_attrs.append(f"{key}={clean_value}")
        
        if extra_attrs:
            edge_label = f"{edge_label}\\n{', '.join(extra_attrs[:2])}"  # Show first 2 extra attrs
        
        dot_content.append(f'  "{source}" -> "{target}" [label="{sanitize_for_dot_label_content(edge_label)}"];')
    
    dot_content.append('}')
    
    # Write HTML DOT file
    with open(output_dot_path, 'w') as f:
        f.write('\n'.join(dot_content))
    
    print(f"Created HTML DOT file: {output_dot_path}")
    return '\n'.join(dot_content)

def generate_png(dot_content, png_path):
    """Generate PNG from DOT content using dot command"""
    print(f"Generating PNG: {png_path}")
    
    try:
        # Write dot content to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.dot', delete=False) as temp_dot:
            temp_dot.write(dot_content)
            temp_dot_path = temp_dot.name
        
        # Generate PNG using dot command
        cmd = ['dot', '-Tpng', temp_dot_path, '-o', png_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Clean up temporary file
        os.unlink(temp_dot_path)
        
        if result.returncode == 0:
            print(f"Successfully generated PNG: {png_path}")
            return True
        else:
            print(f"Error generating PNG: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error generating PNG: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='HTML DOT Generator - Creates HTML-encoded DOT file and PNG visualization'
    )
    parser.add_argument('input_dot', help='Input DOT file path')
    parser.add_argument('output_dir', help='Output directory for HTML DOT and PNG files')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_dot):
        print(f"Error: Input file not found: {args.input_dot}")
        sys.exit(1)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Generate output file names
    base_name = os.path.splitext(os.path.basename(args.input_dot))[0]
    html_dot_path = os.path.join(args.output_dir, f"{base_name}_html.dot")
    png_path = os.path.join(args.output_dir, f"{base_name}_html.png")
    
    print("\n" + "="*60)
    print("HTML DOT GENERATOR")
    print("="*60)
    
    # Parse input DOT file
    nodes, edges = parse_dot_file(args.input_dot)
    
    if not nodes:
        print("Error: No nodes found in the input DOT file")
        sys.exit(1)
    
    # Create HTML DOT file
    dot_content = create_html_dot_file(args.input_dot, html_dot_path, nodes, edges)
    
    # Generate PNG
    png_success = generate_png(dot_content, png_path)
    
    print("\n" + "="*60)
    print("✅ COMPLETED")
    print("="*60)
    print(f"📁 Output directory: {os.path.abspath(args.output_dir)}")
    print(f"📊 Processed: {len(nodes)} nodes, {len(edges)} edges")
    print(f"📄 Files created:")
    print(f"   1. {base_name}_html.dot (HTML-encoded DOT)")
    if png_success:
        print(f"   2. {base_name}_html.png (PNG visualization)")
    else:
        print(f"   2. ❌ Failed to generate PNG")

if __name__ == '__main__':
    main()