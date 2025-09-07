#!/usr/bin/env python3
"""
Verification script to validate UDF filtering correctness.
This script verifies that:
1. Only UDF METHOD nodes and their CFG-reachable nodes are kept
2. CFG edges between kept nodes are preserved
3. CALL edges only go from UDF bodies to UDF METHODs
4. Node definitions match exactly between original and filtered
5. No data corruption during filtering
"""

import os
import sys
import re
import argparse
import json
from collections import defaultdict, deque

class UDFFilterVerifier:
    def __init__(self, original_dot, filtered_dot):
        self.original_dot = original_dot
        self.filtered_dot = filtered_dot
        
        # Data storage
        self.original_edges = []
        self.original_nodes = {}
        self.filtered_edges = []
        self.filtered_nodes = {}
        
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
                    'raw_line': line.strip()
                })
        
        return edges
    
    def parse_nodes(self, dot_content):
        """Parse all nodes from DOT content with raw definitions"""
        nodes = {}
        lines = dot_content.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            # Look for node pattern: "node_id" [
            node_match = re.match(r'"([^"]+)"\s*\[', line)
            if node_match:
                node_id = node_match.group(1)
                
                # Collect full definition preserving original formatting
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
                
                # Store both raw definition and parsed attributes
                full_definition = '\n'.join(full_definition_lines)
                attributes = self.extract_node_attributes(full_definition)
                
                nodes[node_id] = {
                    'attributes': attributes,
                    'raw_definition': full_definition.strip()
                }
            
            i += 1
        
        return nodes
    
    def extract_node_attributes(self, node_definition):
        """Extract key attributes for UDF analysis"""
        attributes = {}
        
        # Extract essential attributes for UDF verification
        patterns = {
            'label': r'label="([^"]*)"',
            'IS_EXTERNAL': r'IS_EXTERNAL="([^"]*)"',
            'NAME': r'NAME="([^"]*)"',
            'FULL_NAME': r'FULL_NAME="([^"]*)"',
            'FILENAME': r'FILENAME="([^"]*)"',
            'AST_PARENT_FULL_NAME': r'AST_PARENT_FULL_NAME="([^"]*)"'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, node_definition, re.DOTALL)
            if match:
                attributes[key] = match.group(1)
        
        return attributes
    
    def is_user_defined_method(self, node_attrs):
        """Check if node is a UDF METHOD using same logic as filter"""
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
        
        return True
    
    def load_and_parse_files(self):
        """Load and parse both original and filtered DOT files"""
        print("Loading original CFG+CALL subgraph...")
        with open(self.original_dot, 'r') as f:
            original_content = f.read()
        
        print("Loading UDF-filtered subgraph...")
        with open(self.filtered_dot, 'r') as f:
            filtered_content = f.read()
        
        print("Parsing original edges and nodes...")
        self.original_edges = self.parse_edges(original_content)
        self.original_nodes = self.parse_nodes(original_content)
        
        print("Parsing filtered edges and nodes...")
        self.filtered_edges = self.parse_edges(filtered_content)
        self.filtered_nodes = self.parse_nodes(filtered_content)
        
        print(f"Original: {len(self.original_edges)} edges, {len(self.original_nodes)} nodes")
        print(f"Filtered: {len(self.filtered_edges)} edges, {len(self.filtered_nodes)} nodes")
    
    def verify_udf_identification(self):
        """Verify UDF identification logic matches expected results"""
        print("\n" + "="*60)
        print("UDF IDENTIFICATION VERIFICATION")
        print("="*60)
        
        # Identify UDFs in original
        original_udfs = set()
        original_methods = set()
        
        for node_id, node_data in self.original_nodes.items():
            attrs = node_data['attributes']
            if attrs.get('label') == 'METHOD':
                original_methods.add(node_id)
                if self.is_user_defined_method(attrs):
                    original_udfs.add(node_id)
        
        # Check that all UDF METHODs are in filtered graph
        filtered_method_nodes = set()
        filtered_udf_nodes = set()
        
        for node_id, node_data in self.filtered_nodes.items():
            attrs = node_data['attributes']
            if attrs.get('label') == 'METHOD':
                filtered_method_nodes.add(node_id)
                if self.is_user_defined_method(attrs):
                    filtered_udf_nodes.add(node_id)
        
        udf_verification = {'passed': True, 'issues': []}
        
        print(f"Original UDF METHODs: {len(original_udfs)}")
        print(f"Filtered UDF METHODs: {len(filtered_udf_nodes)}")
        
        # Check UDF preservation
        missing_udfs = original_udfs - filtered_udf_nodes
        if missing_udfs:
            udf_verification['passed'] = False
            udf_verification['issues'].append(f"Missing UDF methods: {missing_udfs}")
            print(f"❌ Missing UDF methods: {len(missing_udfs)}")
            for udf in list(missing_udfs)[:3]:
                print(f"   - {udf}")
        
        # Check no external methods made it through
        external_methods_in_filtered = filtered_method_nodes - original_udfs
        if external_methods_in_filtered:
            udf_verification['passed'] = False
            udf_verification['issues'].append(f"External methods present: {len(external_methods_in_filtered)}")
            print(f"❌ Non-UDF methods in filtered graph: {len(external_methods_in_filtered)}")
            for method in list(external_methods_in_filtered)[:3]:
                attrs = self.filtered_nodes[method]['attributes']
                print(f"   - {method}: {attrs.get('NAME', 'UNKNOWN')}")
        
        if udf_verification['passed']:
            print("✅ UDF identification: PASSED")
        else:
            print("❌ UDF identification: FAILED")
        
        self.verification_results['udf_identification'] = udf_verification
        return udf_verification['passed']
    
    def verify_cfg_reachability(self):
        """Verify CFG reachability from UDFs is correct"""
        print("\n" + "="*60)
        print("CFG REACHABILITY VERIFICATION")
        print("="*60)
        
        # Build CFG adjacency from original
        cfg_adjacency = defaultdict(list)
        original_cfg_edges = [e for e in self.original_edges if e['type'] == 'CFG']
        
        for edge in original_cfg_edges:
            cfg_adjacency[edge['source']].append(edge['target'])
        
        # Find UDFs in original
        original_udfs = set()
        for node_id, node_data in self.original_nodes.items():
            attrs = node_data['attributes']
            if attrs.get('label') == 'METHOD' and self.is_user_defined_method(attrs):
                original_udfs.add(node_id)
        
        # Calculate expected CFG-reachable nodes
        def bfs_cfg_reachable(start_node):
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
        
        expected_nodes = set()
        for udf_id in original_udfs:
            reachable = bfs_cfg_reachable(udf_id)
            expected_nodes.update(reachable)
        
        actual_nodes = set(self.filtered_nodes.keys())
        
        cfg_verification = {'passed': True, 'issues': []}
        
        print(f"Expected CFG-reachable nodes: {len(expected_nodes)}")
        print(f"Actual filtered nodes: {len(actual_nodes)}")
        
        # Check for missing nodes
        missing_nodes = expected_nodes - actual_nodes
        if missing_nodes:
            cfg_verification['passed'] = False
            cfg_verification['issues'].append(f"Missing CFG-reachable nodes: {len(missing_nodes)}")
            print(f"❌ Missing nodes: {len(missing_nodes)}")
            for node in list(missing_nodes)[:5]:
                print(f"   - {node}")
        
        # Check for extra nodes (shouldn't be CFG-reachable from UDFs)
        extra_nodes = actual_nodes - expected_nodes
        if extra_nodes:
            cfg_verification['passed'] = False
            cfg_verification['issues'].append(f"Extra non-CFG-reachable nodes: {len(extra_nodes)}")
            print(f"❌ Extra nodes: {len(extra_nodes)}")
            for node in list(extra_nodes)[:5]:
                print(f"   - {node}")
        
        if cfg_verification['passed']:
            print("✅ CFG reachability: PASSED")
        else:
            print("❌ CFG reachability: FAILED")
        
        self.verification_results['cfg_reachability'] = cfg_verification
        return cfg_verification['passed']
    
    def verify_edge_filtering(self):
        """Verify edge filtering follows UDF rules"""
        print("\n" + "="*60)
        print("EDGE FILTERING VERIFICATION")
        print("="*60)
        
        edge_verification = {'passed': True, 'issues': []}
        
        # Get sets for analysis
        filtered_node_ids = set(self.filtered_nodes.keys())
        original_udfs = set()
        for node_id, node_data in self.original_nodes.items():
            attrs = node_data['attributes']
            if attrs.get('label') == 'METHOD' and self.is_user_defined_method(attrs):
                original_udfs.add(node_id)
        
        # Check CFG edges - should all be between kept nodes
        filtered_cfg_edges = [e for e in self.filtered_edges if e['type'] == 'CFG']
        original_cfg_edges = [e for e in self.original_edges if e['type'] == 'CFG']
        
        invalid_cfg_edges = []
        for edge in filtered_cfg_edges:
            if edge['source'] not in filtered_node_ids or edge['target'] not in filtered_node_ids:
                invalid_cfg_edges.append(edge)
        
        if invalid_cfg_edges:
            edge_verification['passed'] = False
            edge_verification['issues'].append(f"CFG edges with missing endpoints: {len(invalid_cfg_edges)}")
            print(f"❌ Invalid CFG edges: {len(invalid_cfg_edges)}")
        
        # Check CALL edges - should only go to UDF METHODs from kept nodes
        filtered_call_edges = [e for e in self.filtered_edges if e['type'] == 'CALL']
        
        invalid_call_edges = []
        for edge in filtered_call_edges:
            source_in_kept = edge['source'] in filtered_node_ids
            target_is_udf = edge['target'] in original_udfs
            
            if not (source_in_kept and target_is_udf):
                invalid_call_edges.append(edge)
        
        if invalid_call_edges:
            edge_verification['passed'] = False
            edge_verification['issues'].append(f"CALL edges violating UDF rules: {len(invalid_call_edges)}")
            print(f"❌ Invalid CALL edges: {len(invalid_call_edges)}")
        
        # Summary
        print(f"CFG edges: {len(filtered_cfg_edges)} kept")
        print(f"CALL edges: {len(filtered_call_edges)} kept")
        
        if edge_verification['passed']:
            print("✅ Edge filtering: PASSED")
        else:
            print("❌ Edge filtering: FAILED")
        
        self.verification_results['edge_filtering'] = edge_verification
        return edge_verification['passed']
    
    def verify_node_integrity(self):
        """Verify node definitions match exactly between original and filtered"""
        print("\n" + "="*60)
        print("NODE INTEGRITY VERIFICATION")
        print("="*60)
        
        integrity_verification = {'passed': True, 'issues': []}
        
        # Check that all filtered nodes exist in original with identical definitions
        corrupted_nodes = []
        
        for node_id, filtered_data in self.filtered_nodes.items():
            if node_id not in self.original_nodes:
                integrity_verification['passed'] = False
                integrity_verification['issues'].append(f"Node {node_id} not found in original")
                continue
            
            original_data = self.original_nodes[node_id]
            
            # Compare raw definitions (should be identical)
            original_raw = original_data['raw_definition'].strip()
            filtered_raw = filtered_data['raw_definition'].strip()
            
            if original_raw != filtered_raw:
                corrupted_nodes.append(node_id)
                integrity_verification['passed'] = False
                
                if len(corrupted_nodes) <= 3:  # Show details for first few
                    print(f"❌ Node {node_id} definition corruption:")
                    print(f"   Original length: {len(original_raw)}")
                    print(f"   Filtered length: {len(filtered_raw)}")
                    
                    # Show where they differ
                    min_len = min(len(original_raw), len(filtered_raw))
                    for i in range(min_len):
                        if original_raw[i] != filtered_raw[i]:
                            print(f"   First difference at position {i}")
                            print(f"   Original: ...{original_raw[max(0, i-20):i+20]}...")
                            print(f"   Filtered: ...{filtered_raw[max(0, i-20):i+20]}...")
                            break
        
        if corrupted_nodes:
            integrity_verification['issues'].append(f"Corrupted node definitions: {len(corrupted_nodes)}")
            print(f"❌ Corrupted nodes: {len(corrupted_nodes)}")
        else:
            print("✅ Node integrity: PASSED")
        
        self.verification_results['node_integrity'] = integrity_verification
        return integrity_verification['passed']
    
    def generate_verification_report(self, output_path=None):
        """Generate detailed verification report"""
        report = {
            'files': {
                'original_dot': self.original_dot,
                'filtered_dot': self.filtered_dot
            },
            'counts': {
                'original_nodes': len(self.original_nodes),
                'original_edges': len(self.original_edges),
                'filtered_nodes': len(self.filtered_nodes),
                'filtered_edges': len(self.filtered_edges)
            },
            'verification_results': self.verification_results,
            'overall_passed': all(result['passed'] for result in self.verification_results.values())
        }
        
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"📄 Verification report saved: {output_path}")
        
        return report
    
    def generate_summary_report(self):
        """Generate summary of all verification results"""
        print("\n" + "="*60)
        print("UDF FILTERING VERIFICATION SUMMARY")
        print("="*60)
        
        all_passed = True
        for category, result in self.verification_results.items():
            status = "✅ PASSED" if result['passed'] else "❌ FAILED"
            print(f"{category.upper().replace('_', ' ')}: {status}")
            if not result['passed']:
                all_passed = False
                for issue in result['issues']:
                    print(f"   - {issue}")
        
        print("\n" + "="*60)
        if all_passed:
            print("🎉 OVERALL UDF FILTERING: ✅ PASSED")
            print("The UDF filtering is CORRECT!")
        else:
            print("🚨 OVERALL UDF FILTERING: ❌ FAILED")
            print("The UDF filtering has issues that need to be fixed.")
        print("="*60)
        
        return all_passed
    
    def run_verification(self):
        """Run complete UDF filtering verification"""
        print("🔍 Starting UDF filtering verification...")
        print(f"Original CFG+CALL: {self.original_dot}")
        print(f"UDF-filtered: {self.filtered_dot}")
        
        # Load and parse files
        self.load_and_parse_files()
        
        # Run all verifications
        udf_ok = self.verify_udf_identification()
        cfg_ok = self.verify_cfg_reachability()
        edge_ok = self.verify_edge_filtering()
        integrity_ok = self.verify_node_integrity()
        
        # Generate summary
        overall_passed = self.generate_summary_report()
        
        return overall_passed

def main():
    parser = argparse.ArgumentParser(
        description='Verify UDF filtering correctness'
    )
    parser.add_argument('original_dot', help='Original CFG+CALL subgraph DOT file')
    parser.add_argument('filtered_dot', help='UDF-filtered DOT file')
    parser.add_argument('--output-report', help='Output path for JSON verification report')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.original_dot):
        print(f"Error: Original DOT file not found: {args.original_dot}")
        sys.exit(1)
        
    if not os.path.exists(args.filtered_dot):
        print(f"Error: Filtered DOT file not found: {args.filtered_dot}")
        sys.exit(1)
    
    # Run verification
    verifier = UDFFilterVerifier(args.original_dot, args.filtered_dot)
    success = verifier.run_verification()
    
    # Generate report if requested
    if args.output_report:
        verifier.generate_verification_report(args.output_report)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()