#!/usr/bin/env python3
"""
Verification script to validate CFG/CALL subgraph extraction correctness.
This script verifies that:
1. All CFG/CALL edges from original are in subgraph
2. All nodes connected to CFG/CALL edges are in subgraph  
3. No extra nodes or edges are included
4. Node attributes match exactly
"""

import os
import sys
import re
import argparse
from collections import defaultdict

class SubgraphVerifier:
    def __init__(self, original_dot, extracted_dot, edge_types):
        self.original_dot = original_dot
        self.extracted_dot = extracted_dot
        self.edge_types = set(edge_types)
        
        # Results storage
        self.original_edges = []
        self.original_nodes = {}
        self.extracted_edges = []
        self.extracted_nodes = {}
        
        # Analysis results
        self.verification_results = {}
        
    def parse_edges(self, dot_content):
        """Parse all edges from DOT content"""
        edges = []
        
        for line in dot_content.split('\n'):
            line = line.strip()
            
            # Look for edge pattern: "node1" -> "node2" [label="EDGE_TYPE" ...];
            edge_match = re.match(r'"([^"]+)"\s*->\s*"([^"]+)"\s*\[.*label="([^"]+)".*\];', line)
            if edge_match:
                source = edge_match.group(1)
                target = edge_match.group(2) 
                edge_type = edge_match.group(3)
                
                edges.append({
                    'source': source,
                    'target': target,
                    'type': edge_type,
                    'raw_line': line
                })
        
        return edges
    
    def parse_nodes(self, dot_content):
        """Parse all nodes from DOT content"""
        nodes = {}
        lines = dot_content.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            # Look for node pattern: "node_id" [
            node_match = re.match(r'"([^"]+)"\s*\[', line)
            if node_match:
                node_id = node_match.group(1)
                
                # Collect full definition (handle multiline)
                full_definition_lines = [lines[i]]
                
                if not line.endswith('"];'):
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j]
                        full_definition_lines.append(next_line)
                        if next_line.rstrip().endswith('"];'):
                            break
                        j += 1
                    i = j
                
                # Parse attributes from full definition
                full_definition = '\n'.join(full_definition_lines)
                attributes = self.extract_node_attributes(full_definition)
                
                nodes[node_id] = {
                    'attributes': attributes,
                    'raw_definition': full_definition
                }
            
            i += 1
        
        return nodes
    
    def extract_node_attributes(self, node_definition):
        """Extract attributes from node definition"""
        attributes = {}
        
        # Simple attribute extraction - look for key="value" patterns
        attr_pattern = r'(\w+)="([^"]*(?:"[^"]*"[^"]*)*)"'
        
        for match in re.finditer(attr_pattern, node_definition, re.DOTALL):
            key = match.group(1)
            value = match.group(2)
            attributes[key] = value
        
        return attributes
    
    def load_and_parse_files(self):
        """Load and parse both original and extracted DOT files"""
        print("Loading original DOT file...")
        with open(self.original_dot, 'r') as f:
            original_content = f.read()
        
        print("Loading extracted DOT file...")
        with open(self.extracted_dot, 'r') as f:
            extracted_content = f.read()
        
        print("Parsing original edges and nodes...")
        self.original_edges = self.parse_edges(original_content)
        self.original_nodes = self.parse_nodes(original_content)
        
        print("Parsing extracted edges and nodes...")
        self.extracted_edges = self.parse_edges(extracted_content)
        self.extracted_nodes = self.parse_nodes(extracted_content)
        
        print(f"Original: {len(self.original_edges)} edges, {len(self.original_nodes)} nodes")
        print(f"Extracted: {len(self.extracted_edges)} edges, {len(self.extracted_nodes)} nodes")
    
    def verify_edge_extraction(self):
        """Verify that all target edges are correctly extracted"""
        print("\n" + "="*60)
        print("EDGE EXTRACTION VERIFICATION")
        print("="*60)
        
        # Filter original edges for target types
        original_target_edges = [e for e in self.original_edges if e['type'] in self.edge_types]
        
        # Group edges by type for analysis
        original_by_type = defaultdict(list)
        extracted_by_type = defaultdict(list)
        
        for edge in original_target_edges:
            original_by_type[edge['type']].append(edge)
            
        for edge in self.extracted_edges:
            extracted_by_type[edge['type']].append(edge)
        
        # Check counts by type
        edge_verification = {'passed': True, 'issues': []}
        
        for edge_type in self.edge_types:
            original_count = len(original_by_type[edge_type])
            extracted_count = len(extracted_by_type[edge_type])
            
            print(f"{edge_type} edges: {original_count} original → {extracted_count} extracted")
            
            if original_count != extracted_count:
                edge_verification['passed'] = False
                edge_verification['issues'].append(f"{edge_type}: count mismatch ({original_count} vs {extracted_count})")
        
        # Check for unwanted edge types
        unwanted_types = set(extracted_by_type.keys()) - self.edge_types
        if unwanted_types:
            edge_verification['passed'] = False
            edge_verification['issues'].append(f"Unwanted edge types found: {unwanted_types}")
            print(f"❌ Unwanted edge types: {unwanted_types}")
        
        # Detailed edge comparison
        missing_edges = []
        extra_edges = []
        
        # Create edge signatures for comparison
        original_edge_sigs = set()
        for edge in original_target_edges:
            sig = f"{edge['source']}->{edge['target']}:{edge['type']}"
            original_edge_sigs.add(sig)
        
        extracted_edge_sigs = set()
        for edge in self.extracted_edges:
            sig = f"{edge['source']}->{edge['target']}:{edge['type']}"
            extracted_edge_sigs.add(sig)
        
        missing_edges = original_edge_sigs - extracted_edge_sigs
        extra_edges = extracted_edge_sigs - original_edge_sigs
        
        if missing_edges:
            edge_verification['passed'] = False
            edge_verification['issues'].append(f"Missing {len(missing_edges)} edges")
            print(f"❌ Missing edges: {len(missing_edges)}")
            for edge in list(missing_edges)[:5]:  # Show first 5
                print(f"   - {edge}")
        
        if extra_edges:
            edge_verification['passed'] = False  
            edge_verification['issues'].append(f"Extra {len(extra_edges)} edges")
            print(f"❌ Extra edges: {len(extra_edges)}")
            for edge in list(extra_edges)[:5]:  # Show first 5
                print(f"   - {edge}")
        
        if edge_verification['passed']:
            print("✅ Edge extraction: PASSED")
        else:
            print("❌ Edge extraction: FAILED")
            for issue in edge_verification['issues']:
                print(f"   - {issue}")
        
        self.verification_results['edges'] = edge_verification
        return edge_verification['passed']
    
    def verify_node_extraction(self):
        """Verify that all nodes connected to target edges are extracted"""
        print("\n" + "="*60)
        print("NODE EXTRACTION VERIFICATION") 
        print("="*60)
        
        # Find all nodes that should be in subgraph
        expected_nodes = set()
        original_target_edges = [e for e in self.original_edges if e['type'] in self.edge_types]
        
        for edge in original_target_edges:
            expected_nodes.add(edge['source'])
            expected_nodes.add(edge['target'])
        
        # Get actual extracted nodes
        actual_nodes = set(self.extracted_nodes.keys())
        
        print(f"Expected nodes: {len(expected_nodes)}")
        print(f"Extracted nodes: {len(actual_nodes)}")
        
        node_verification = {'passed': True, 'issues': []}
        
        # Check for missing nodes
        missing_nodes = expected_nodes - actual_nodes
        if missing_nodes:
            node_verification['passed'] = False
            node_verification['issues'].append(f"Missing {len(missing_nodes)} nodes")
            print(f"❌ Missing nodes: {len(missing_nodes)}")
            for node in list(missing_nodes)[:5]:
                print(f"   - {node}")
        
        # Check for extra nodes  
        extra_nodes = actual_nodes - expected_nodes
        if extra_nodes:
            node_verification['passed'] = False
            node_verification['issues'].append(f"Extra {len(extra_nodes)} nodes")
            print(f"❌ Extra nodes: {len(extra_nodes)}")
            for node in list(extra_nodes)[:5]:
                print(f"   - {node}")
        
        if node_verification['passed']:
            print("✅ Node extraction: PASSED")
        else:
            print("❌ Node extraction: FAILED")
            for issue in node_verification['issues']:
                print(f"   - {issue}")
        
        self.verification_results['nodes'] = node_verification
        return node_verification['passed']
    
    def verify_node_attributes(self):
        """Verify that node attributes match between original and extracted"""
        print("\n" + "="*60)
        print("NODE ATTRIBUTES VERIFICATION")
        print("="*60)
        
        attr_verification = {'passed': True, 'issues': []}
        mismatched_attrs = []
        
        # Check attributes for nodes that exist in both
        common_nodes = set(self.original_nodes.keys()) & set(self.extracted_nodes.keys())
        
        for node_id in common_nodes:
            original_attrs = self.original_nodes[node_id]['attributes']
            extracted_attrs = self.extracted_nodes[node_id]['attributes']
            
            # Compare attributes
            if original_attrs != extracted_attrs:
                mismatched_attrs.append(node_id)
                
                # Find specific differences
                missing_attrs = set(original_attrs.keys()) - set(extracted_attrs.keys())
                extra_attrs = set(extracted_attrs.keys()) - set(original_attrs.keys())
                
                if missing_attrs or extra_attrs:
                    attr_verification['passed'] = False
                    if len(mismatched_attrs) <= 3:  # Show details for first few
                        print(f"❌ Node {node_id} attribute mismatch:")
                        if missing_attrs:
                            print(f"   Missing: {missing_attrs}")
                        if extra_attrs:
                            print(f"   Extra: {extra_attrs}")
                else:
                    # Check for value differences
                    for key in original_attrs:
                        if key in extracted_attrs:
                            if original_attrs[key] != extracted_attrs[key]:
                                attr_verification['passed'] = False
                                if len(mismatched_attrs) <= 3:
                                    print(f"❌ Node {node_id} value mismatch in '{key}':")
                                    print(f"   Original: {original_attrs[key][:100]}...")
                                    print(f"   Extracted: {extracted_attrs[key][:100]}...")
                                break
        
        if mismatched_attrs:
            attr_verification['passed'] = False
            attr_verification['issues'].append(f"{len(mismatched_attrs)} nodes with attribute mismatches")
            print(f"❌ Attribute mismatches: {len(mismatched_attrs)} nodes")
        else:
            print("✅ Node attributes: PASSED")
        
        self.verification_results['attributes'] = attr_verification
        return attr_verification['passed']
    
    def generate_summary_report(self):
        """Generate a summary report of verification results"""
        print("\n" + "="*60)
        print("VERIFICATION SUMMARY")
        print("="*60)
        
        all_passed = True
        for category, result in self.verification_results.items():
            status = "✅ PASSED" if result['passed'] else "❌ FAILED"
            print(f"{category.upper()}: {status}")
            if not result['passed']:
                all_passed = False
                for issue in result['issues']:
                    print(f"   - {issue}")
        
        print("\n" + "="*60)
        if all_passed:
            print("🎉 OVERALL VERIFICATION: ✅ PASSED")
            print("The subgraph extraction is CORRECT!")
        else:
            print("🚨 OVERALL VERIFICATION: ❌ FAILED")
            print("The subgraph extraction has issues that need to be fixed.")
        print("="*60)
        
        return all_passed
    
    def run_verification(self):
        """Run complete verification process"""
        print("🔍 Starting subgraph extraction verification...")
        print(f"Original: {self.original_dot}")
        print(f"Extracted: {self.extracted_dot}")
        print(f"Target edge types: {', '.join(self.edge_types)}")
        
        # Load and parse files
        self.load_and_parse_files()
        
        # Run all verifications
        edges_ok = self.verify_edge_extraction()
        nodes_ok = self.verify_node_extraction()  
        attrs_ok = self.verify_node_attributes()
        
        # Generate summary
        overall_passed = self.generate_summary_report()
        
        return overall_passed

def main():
    parser = argparse.ArgumentParser(
        description='Verify CFG/CALL subgraph extraction correctness'
    )
    parser.add_argument('original_dot', help='Original full DOT file path')
    parser.add_argument('extracted_dot', help='Extracted subgraph DOT file path')
    parser.add_argument('--edge-types', nargs='+', default=['CFG', 'CALL'],
                       help='Edge types that should be extracted (default: CFG CALL)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.original_dot):
        print(f"Error: Original DOT file not found: {args.original_dot}")
        sys.exit(1)
        
    if not os.path.exists(args.extracted_dot):
        print(f"Error: Extracted DOT file not found: {args.extracted_dot}")
        sys.exit(1)
    
    # Run verification
    verifier = SubgraphVerifier(args.original_dot, args.extracted_dot, args.edge_types)
    success = verifier.run_verification()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()