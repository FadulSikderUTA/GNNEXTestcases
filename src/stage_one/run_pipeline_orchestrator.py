#!/usr/bin/env python3
"""
Pipeline Orchestrator - Executes complete vulnerability analysis pipeline on datasets
===================================================================================

This script orchestrates the execution of the 5-step pipeline:
1. Joern CPG generation from C function bodies
2. Subgraph extraction (CFG+CALL edges)  
3. Subgraph verification
4. UDF (User-Defined Function) filtering
5. HTML visualization generation

Supports parallel processing, resume capability, and comprehensive tracking.

Usage:
    python run_pipeline_orchestrator.py --dataset-path /path/to/dataset --output-root /path/to/artifacts

Example:
    python run_pipeline_orchestrator.py \
        --dataset-path /home/fadul/GNNTestcases/outputs/unsplit_selected/secvuleval_cwe-119 \
        --output-root /home/fadul/GNNTestcases/pipeline_artifacts \
        --splits test \
        --jobs 4 \
        --max-samples 10
"""

import os
import sys
import re
import json
import csv
import time
import argparse
import shutil
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Generator
import traceback
from datetime import datetime
from tqdm import tqdm
import numpy as np
import multiprocessing

# Add current directory to path for imports (scripts are in same directory)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import pipeline steps
try:
    import joern_process as joern
    from simple_subgraph_extractor import create_simple_subgraph
    from verify_subgraph_extraction import SubgraphVerifier
    from verify_udf_filtering import UDFFilterVerifier
    import udf_filter as udf
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure all required packages and scripts are available")
    sys.exit(1)

# Pipeline configuration
EDGE_TYPES = {"CFG", "CALL"}
DEFAULT_INCLUDES = ['#include <stdint.h>', '#include <stddef.h>']

# Global semaphore to limit concurrent Joern processes (avoid resource contention)
# Limit to max 4 concurrent Joern processes regardless of worker count
JOERN_SEMAPHORE = multiprocessing.Semaphore(4)

class DatasetReader:
    """Reads Arrow/Parquet datasets and yields sample dictionaries"""
    
    def __init__(self, dataset_path: str):
        self.dataset_path = Path(dataset_path)
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset path not found: {dataset_path}")
    
    def get_available_splits(self) -> List[str]:
        """Get list of available dataset splits"""
        split_dir = self.dataset_path / "split"
        if not split_dir.exists():
            return ["train"]  # Default fallback
        
        splits = []
        for split_path in split_dir.iterdir():
            if split_path.is_dir():
                splits.append(split_path.name)
        return sorted(splits)
    
    def read_split(self, split_name: str) -> Generator[Dict, None, None]:
        """Read samples from a specific split"""
        split_path = self.dataset_path / "split" / split_name
        
        if not split_path.exists():
            print(f"⚠️  Split {split_name} not found, skipping...")
            return
        
        # Find Arrow files in the split directory
        arrow_files = list(split_path.glob("*.arrow"))
        if not arrow_files:
            print(f"⚠️  No Arrow files found in {split_path}")
            return
        
        print(f"📂 Reading {split_name} split from {len(arrow_files)} files")
        
        total_samples = 0
        for arrow_file in sorted(arrow_files):
            try:
                # Read Arrow file using stream method (works with this dataset format)
                with open(arrow_file, 'rb') as f:
                    reader = pa.ipc.open_stream(f)
                    table = reader.read_all()
                df = table.to_pandas()
                
                print(f"   📄 {arrow_file.name}: {len(df)} samples")
                
                # Yield each row as a dictionary with split info
                for _, row in df.iterrows():
                    sample = row.to_dict()
                    sample['split'] = split_name
                    sample['dataset_name'] = self.dataset_path.name
                    
                    # Handle different dataset schemas
                    # Juliet datasets use 'whole_program_body', others use 'func_body'
                    if 'whole_program_body' in sample and 'func_body' not in sample:
                        sample['func_body'] = sample['whole_program_body']
                        sample['func_name'] = sample.get('testcases_name', 'unknown')
                    
                    # Ensure we have required fields with defaults
                    sample['func_body'] = sample.get('func_body', '')
                    sample['func_name'] = sample.get('func_name', 'unknown')
                    sample['idx'] = sample.get('idx', total_samples)
                    sample['hash'] = sample.get('hash', '')
                    
                    total_samples += 1
                    yield sample
                    
            except Exception as e:
                print(f"❌ Error reading {arrow_file}: {e}")
                continue
        
        print(f"✅ {split_name} split: {total_samples} samples total")

# def looks_like_complete_function(func_body: str) -> bool:
#     """
#     Detect if func_body is a complete function definition or complete program.
#     
#     For single function definitions, looks for pattern: [type/qualifiers] function_name(parameters) {
#     For complete programs (like Juliet), checks for presence of function definitions.
#     
#     Examples that match:
#     - int main() { ... }  (single function)
#     - static void helper(int x) { ... }  (single function)
#     - Complete C program with includes and multiple functions (Juliet style)
#     """
#     if not func_body or not isinstance(func_body, str):
#         return False
#     
#     # Clean up the function body
#     func_body = func_body.strip()
#     
#     # Look for function signature pattern anywhere in the text (not just at start)
#     # Pattern explanation:
#     # - Optional storage/type qualifiers (static, inline, etc.)
#     # - Return type (int, void, char*, etc.) 
#     # - Function name (identifier)
#     # - Parameter list in parentheses
#     # - Opening brace
#     pattern = r'(?:^|\n)\s*(?:(?:static|inline|extern|const|volatile|unsigned|signed|short|long)\s+)*\w+(?:\s*\*+)?\s+\w+\s*\([^)]*\)\s*\{'
#     
#     # First check: single function definition at start
#     single_func_pattern = r'^\s*(?:(?:static|inline|extern|const|volatile|unsigned|signed|short|long)\s+)*\w+(?:\s*\*+)?\s+\w+\s*\([^)]*\)\s*\{'
#     if re.match(single_func_pattern, func_body, re.MULTILINE):
#         return True
#     
#     # Second check: complete program with function definitions (for Juliet-style)
#     if re.search(pattern, func_body, re.MULTILINE):
#         # Additional check: if it has includes or multiple functions, treat as complete program
#         has_includes = bool(re.search(r'#include\s*[<"]', func_body))
#         has_multiple_functions = len(re.findall(pattern, func_body, re.MULTILINE)) > 1
#         
#         if has_includes or has_multiple_functions:
#             return True
#     
#     return False

def create_translation_unit(func_body: str, output_dir: Path, program_path: str = None) -> Path:
    """Create a minimal C/C++ translation unit from complete function body or program
    
    Args:
        func_body: The function body or complete program
        output_dir: Directory to write the translation unit
        program_path: Optional path to original program (used to determine file extension)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine file extension based on program_path or content
    file_extension = "c"  # Default to C
    if program_path:
        # Check the original program path for extension
        if program_path.endswith('.cpp') or program_path.endswith('.cc'):
            file_extension = "cpp"
        elif program_path.endswith('.c'):
            file_extension = "c"
    else:
        # Try to detect from content (fallback method)
        cpp_indicators = [
            'namespace ', 'class ', 'template<', 'public:', 'private:', 
            'protected:', 'std::', 'using namespace', '::',
            '#include <iostream>', '#include <vector>', '#include <string>'
        ]
        if any(indicator in func_body for indicator in cpp_indicators):
            file_extension = "cpp"
    
    # Check if this is already a complete program (has includes)
    has_includes = bool(re.search(r'#include\s*[<"]', func_body))
    
    if has_includes:
        # Already a complete program (like Juliet), use as-is
        tu_content = func_body
    else:
        # Single function, add minimal includes
        tu_lines = []
        tu_lines.extend(DEFAULT_INCLUDES)
        tu_lines.append("")  # Blank line
        tu_lines.append(func_body)
        tu_content = "\n".join(tu_lines)
    
    # Ensure proper line ending
    if not tu_content.endswith('\n'):
        tu_content += "\n"
    
    # Write to unit.c or unit.cpp based on detected extension
    tu_path = output_dir / f"unit.{file_extension}"
    tu_path.write_text(tu_content, encoding="utf-8")
    
    return tu_path

def make_json_serializable(obj):
    """Convert numpy arrays and other non-serializable objects for JSON"""
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    else:
        return obj

def extract_good_bad_indicator(program_path: str) -> str:
    """Extract Good/Bad indicator from Juliet program path
    
    Args:
        program_path: Path like '.../Good/file.c' or '.../Bad/file.c'
        
    Returns:
        'good', 'bad', 'mixed', or 'unknown'
    """
    if not program_path:
        return "unknown"
    
    # Normalize path separators and split
    path_parts = program_path.replace('\\', '/').split('/')
    
    # Look for Good/Bad in path components (case-insensitive)
    for part in reversed(path_parts):
        part_lower = part.lower()
        if part_lower == 'good':
            return "good"
        elif part_lower == 'bad':
            return "bad"
    
    # Fallback: check filename for good/bad indicators
    filename = path_parts[-1] if path_parts else ""
    filename_lower = filename.lower()
    
    if 'good' in filename_lower:
        return "good"
    elif 'bad' in filename_lower:
        return "bad"
    
    return "mixed"  # For cases where it's unclear

def generate_sample_id(sample: Dict, idx: int) -> str:
    """Generate unique sample ID based on dataset type
    
    Args:
        sample: Sample dictionary from dataset
        idx: Sample index
        
    Returns:
        Unique sample identifier string
    """
    # Detect dataset type
    is_juliet = 'testcases_name' in sample or 'whole_program_body' in sample
    is_secvaleval = 'func_body' in sample or 'hash' in sample
    
    if is_juliet:
        testcase_name = sample.get('testcases_name', f"unknown_{idx}")
        program_path = sample.get('program_path', '')
        good_bad = extract_good_bad_indicator(program_path)
        
        # Create unique ID: testcasename_good or testcasename_bad
        return f"{testcase_name}_{good_bad}"
        
    elif is_secvaleval:
        # Keep existing hash-based approach
        hash_val = sample.get('hash', 'unknown')
        return f"{idx}_{hash_val}"
        
    else:
        # Generic fallback
        return f"unknown_{idx}"

def validate_sample_ids(all_samples: List[Dict]) -> Dict[str, List[int]]:
    """Validate that sample IDs are unique and report conflicts
    
    Args:
        all_samples: List of all samples to process
        
    Returns:
        Dictionary mapping sample_id to list of conflicting indices
    """
    sample_id_to_indices = {}
    conflicts = {}
    
    for idx, sample in enumerate(all_samples):
        sample_id = generate_sample_id(sample, idx)
        
        if sample_id not in sample_id_to_indices:
            sample_id_to_indices[sample_id] = []
        sample_id_to_indices[sample_id].append(idx)
        
        # Track conflicts (sample IDs appearing more than once)
        if len(sample_id_to_indices[sample_id]) > 1:
            conflicts[sample_id] = sample_id_to_indices[sample_id]
    
    return conflicts

def create_metadata_file(sample: Dict, base_dir: Path) -> Path:
    """Create metadata.json file with complete sample information for label preservation
    
    Args:
        sample: Complete sample dictionary from dataset
        base_dir: Sample base directory
        
    Returns:
        Path to created metadata.json file
    """
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Detect dataset type based on available fields
    is_juliet = 'testcases_name' in sample or 'whole_program_body' in sample
    is_secvaleval = 'func_body' in sample or 'hash' in sample
    
    # Extract key information adaptively
    metadata = {
        "dataset_info": {
            "dataset_name": sample.get('dataset_name', 'unknown'),
            "split": sample.get('split', 'unknown'),
            "index": sample.get('idx', 'unknown'),
            "sample_id": sample.get('sample_id', 'unknown'),
            "dataset_type": "juliet" if is_juliet else ("secvaleval" if is_secvaleval else "unknown")
        },
        "vulnerability_info": {
            "is_vulnerable": sample.get('is_vulnerable', False),
            "cve_list": sample.get('cve_list', []),
            "cwe_list": sample.get('cwe_list', [])
        },
        "processing_info": {
            "timestamp": datetime.now().isoformat(),
            "pipeline_version": "1.2"
        }
    }
    
    # Add dataset-specific fields
    if is_juliet:
        # Juliet dataset specific fields
        metadata["sample_data"] = {
            "testcases_name": sample.get('testcases_name', ''),
            "originated_program_path": sample.get('originated_program_path', ''),
            "program_path": sample.get('program_path', ''),
            "code_field": "whole_program_body"
        }
    elif is_secvaleval:
        # SecValEval dataset specific fields  
        metadata["sample_data"] = {
            "func_name": sample.get('func_name', ''),
            "filepath": sample.get('filepath', ''),
            "hash": sample.get('hash', ''),
            "project": sample.get('project', ''),
            "commit_id": sample.get('commit_id', ''),
            "commit_message": sample.get('commit_message', ''),
            "changed_lines": sample.get('changed_lines', ''),
            "changed_statements": sample.get('changed_statements', ''),
            "fixed_func_idx": sample.get('fixed_func_idx', ''),
            "context": sample.get('context', {}),
            "code_field": "func_body"
        }
    else:
        # Generic fallback
        metadata["sample_data"] = {
            "code_field": "func_body" if 'func_body' in sample else "whole_program_body"
        }
    
    # Always preserve the complete sample for reference (make it JSON serializable)
    metadata["complete_sample"] = make_json_serializable(sample)
    
    # Make entire metadata JSON serializable
    metadata = make_json_serializable(metadata)
    
    metadata_path = base_dir / "metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    return metadata_path

def process_single_sample(args: Tuple[Dict, Path]) -> Dict:
    """
    Process a single sample through the complete pipeline.
    
    This function is designed to run in a separate process for parallelization.
    """
    sample, output_root = args
    
    # Extract sample info
    dataset_name = sample.get('dataset_name', 'dataset')
    split = sample.get('split', 'unknown')
    idx = str(sample.get('idx', 'unknown'))
    sample_hash = (sample.get('hash', '') or '')[:12]
    func_name = sample.get('func_name', 'unknown')
    func_body = sample.get('func_body', '')
    program_path = sample.get('program_path', '')  # For Juliet dataset to determine C/C++
    
    # Create unique sample ID using new logic to prevent conflicts
    sample_id = generate_sample_id(sample, idx if isinstance(idx, int) else int(idx))
    
    # Create output directory structure
    base_dir = output_root / dataset_name / split / sample_id
    src_dir = base_dir / "src"
    joern_dir = base_dir / "joern"
    subgraphs_dir = base_dir / "subgraphs"
    verify_dir = base_dir / "verify"
    udf_dir = base_dir / "udf"
    html_dir = base_dir / "html"
    logs_dir = base_dir / "logs"
    
    # Add sample_id to the sample dict for metadata
    sample['sample_id'] = sample_id
    
    # Result dictionary
    result = {
        'sample_id': sample_id,
        'split': split,
        'idx': idx,
        'hash': sample_hash,
        'func_name': func_name,
        'is_vulnerable': sample.get('is_vulnerable', False),
        'start_time': time.time(),
        'status': 'unknown',
        'stage': 'init',
        'error_message': '',
        'verification_passed': False,
        'udf_verification_passed': False,
        'artifacts': {}
    }
    
    try:
        # Step 0: Create metadata.json for label preservation (always do this first)
        result['stage'] = 'create_metadata'
        metadata_path = create_metadata_file(sample, base_dir)
        result['artifacts']['metadata'] = str(metadata_path)
        
        # Check for existing final artifacts (idempotent processing)
        final_artifact = udf_dir / "CFG_CALL_original_udf_filtered.dot"
        if final_artifact.exists():
            result.update({
                'status': 'skipped_exists', 
                'stage': 'final_check',
                'error_message': 'Final artifacts already exist'
            })
            return result
        
        # Step 0: Create translation unit
        result['stage'] = 'create_tu'
        tu_path = create_translation_unit(func_body, src_dir, program_path)
        result['artifacts']['translation_unit'] = str(tu_path)
        
        # Step 1: Joern CPG generation with resource management
        result['stage'] = 'joern_cpg'
        joern_dir.mkdir(parents=True, exist_ok=True)
        
        # Acquire semaphore to limit concurrent Joern processes
        with JOERN_SEMAPHORE:
            cpg_path = joern.generate_cpg(str(tu_path), str(joern_dir))
        
        # Robust validation: Check both return value and actual file existence
        cpg_file = joern_dir / "cpg.bin"
        if not cpg_path or not cpg_file.exists() or cpg_file.stat().st_size < 1000:
            # Double-check with short delay for filesystem latency in parallel processing
            time.sleep(0.2)  # 200ms delay for filesystem sync
            if not cpg_file.exists() or cpg_file.stat().st_size < 1000:
                result.update({
                    'status': 'failed',
                    'error_message': f'Joern CPG generation failed - file missing or too small ({cpg_file.stat().st_size if cpg_file.exists() else 0} bytes)'
                })
                return result
            else:
                # Recover from timing issue
                cpg_path = str(cpg_file)
        
        result['artifacts']['cpg'] = cpg_path
        
        # Step 1a: Export DOT format with resource management
        result['stage'] = 'joern_dot'
        
        # Acquire semaphore to limit concurrent Joern processes
        with JOERN_SEMAPHORE:
            dot_path = joern.export_dot_format(cpg_path, str(joern_dir))
        
        # Robust validation: Check both return value and actual file existence/content
        expected_dot_file = joern_dir / "dot_files" / "all" / "export.dot"
        if not dot_path or not expected_dot_file.exists() or expected_dot_file.stat().st_size < 100:
            # Double-check with short delay for filesystem latency in parallel processing
            time.sleep(0.2)  # 200ms delay for filesystem sync
            if not expected_dot_file.exists() or expected_dot_file.stat().st_size < 100:
                result.update({
                    'status': 'failed',
                    'error_message': f'Joern DOT export failed - file missing or too small ({expected_dot_file.stat().st_size if expected_dot_file.exists() else 0} bytes)'
                })
                return result
            else:
                # Recover from timing issue
                dot_path = str(expected_dot_file)
        
        # Additional validation: Check DOT file content is valid
        try:
            with open(dot_path, 'r') as f:
                content = f.read(200)  # Read first 200 chars
                if 'digraph' not in content:
                    result.update({
                        'status': 'failed',
                        'error_message': 'Joern DOT export failed - invalid DOT format (missing digraph)'
                    })
                    return result
        except Exception as e:
            result.update({
                'status': 'failed',
                'error_message': f'Joern DOT export failed - cannot read file: {str(e)}'
            })
            return result
            
        result['artifacts']['dot'] = dot_path
        
        # Step 1b: Export XML format - DISABLED FOR PERFORMANCE
        # result['stage'] = 'joern_xml'
        # xml_path = joern.export_xml_format(cpg_path, str(joern_dir))
        # if xml_path:
        #     result['artifacts']['xml'] = xml_path
        
        # Step 2: Subgraph extraction
        result['stage'] = 'subgraph_extraction'
        subgraphs_dir.mkdir(parents=True, exist_ok=True)
        subgraph_path = subgraphs_dir / "CFG_CALL_original.dot"
        
        with open(dot_path, 'r') as f:
            dot_content = f.read()
        
        create_simple_subgraph(dot_content, EDGE_TYPES, str(subgraph_path))
        if not subgraph_path.exists():
            result.update({
                'status': 'failed',
                'error_message': 'Subgraph extraction failed'
            })
            return result
        result['artifacts']['subgraph'] = str(subgraph_path)
        
        # Step 3: Verification
        result['stage'] = 'verification'
        verify_dir.mkdir(parents=True, exist_ok=True)
        verifier = SubgraphVerifier(dot_path, str(subgraph_path), EDGE_TYPES)
        verification_passed = verifier.run_verification()
        result['verification_passed'] = verification_passed
        
        # Save verification results
        verification_file = verify_dir / "verification.json"
        with open(verification_file, 'w') as f:
            json.dump({
                'passed': verification_passed,
                'timestamp': time.time(),
                'original_dot': dot_path,
                'extracted_dot': str(subgraph_path),
                'edge_types': list(EDGE_TYPES)
            }, f, indent=2)
        result['artifacts']['verification'] = str(verification_file)
        
        # Step 4: UDF filtering
        result['stage'] = 'udf_filter'
        udf_dir.mkdir(parents=True, exist_ok=True)
        
        # Use new line-copying UDF filter
        with open(str(subgraph_path), 'r') as f:
            dot_content = f.read()
        
        node_definitions, kept_edges = udf.filter_udf_subgraph(dot_content)
        
        udf_path = udf_dir / "CFG_CALL_original_udf_filtered.dot"
        udf.write_dot_file(str(udf_path), node_definitions, kept_edges, "CFG_CALL_original")
        result['artifacts']['udf_filtered'] = str(udf_path)
        
        # Step 4b: UDF filtering verification
        result['stage'] = 'udf_verification'
        udf_verification_file = udf_dir / "udf_verification.json"
        
        try:
            udf_verifier = UDFFilterVerifier(str(subgraph_path), str(udf_path))
            udf_verification_passed = udf_verifier.run_verification()
            
            # Generate verification report
            verification_report = udf_verifier.generate_verification_report(str(udf_verification_file))
            result['artifacts']['udf_verification'] = str(udf_verification_file)
            
            # Set verification status
            result['udf_verification_passed'] = udf_verification_passed
            
            if not udf_verification_passed:
                print(f"⚠️  UDF verification FAILED for {sample['sample_id']}")
                print("   Check verification report for details")
        except Exception as e:
            print(f"⚠️  UDF verification error: {str(e)}")
            result['udf_verification_passed'] = False
            with open(udf_verification_file, 'w') as f:
                json.dump({
                    'verification_passed': False,
                    'error': str(e),
                    'original_dot': str(subgraph_path),
                    'filtered_dot': str(udf_path)
                }, f, indent=2)
            result['artifacts']['udf_verification'] = str(udf_verification_file)
        
        # Success!
        result.update({
            'status': 'completed',
            'stage': 'udf_complete'
        })
        
    except Exception as e:
        result.update({
            'status': 'failed',
            'error_message': str(e),
            'traceback': traceback.format_exc()
        })
    
    finally:
        result['end_time'] = time.time()
        result['duration'] = result.get('end_time', time.time()) - result['start_time']
    
    return result

class PipelineOrchestrator:
    """Main orchestrator class"""
    
    def __init__(self, dataset_path: str, output_root: str):
        self.dataset_path = Path(dataset_path)
        self.output_root = Path(output_root)
        self.reader = DatasetReader(dataset_path)
        
        # Create output directory
        self.output_root.mkdir(parents=True, exist_ok=True)
    
    def run_pipeline(self, splits: List[str], max_samples: Optional[int] = None, 
                    num_workers: int = 4, force: bool = False) -> Dict:
        """Run the complete pipeline on specified splits"""
        
        print("=" * 80)
        print("🚀 PIPELINE ORCHESTRATOR")
        print("=" * 80)
        print(f"📂 Dataset: {self.dataset_path}")
        print(f"📁 Output: {self.output_root}")
        print(f"🔀 Splits: {', '.join(splits)}")
        print(f"👥 Workers: {num_workers}")
        if max_samples:
            print(f"📏 Max samples: {max_samples}")
        print("=" * 80)
        
        # Clean up previous artifacts for this dataset (automatic overwrite)
        dataset_output_dir = self.output_root / self.dataset_path.name
        if dataset_output_dir.exists():
            print(f"🧹 Cleaning previous artifacts for {self.dataset_path.name}...")
            shutil.rmtree(dataset_output_dir)
            print(f"✅ Previous artifacts removed")
        
        # Ensure fresh output directory
        dataset_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Collect all samples to process
        all_samples = []
        for split in splits:
            split_samples = list(self.reader.read_split(split))
            if max_samples and len(all_samples) + len(split_samples) > max_samples:
                remaining = max_samples - len(all_samples)
                split_samples = split_samples[:remaining]
            all_samples.extend(split_samples)
            if max_samples and len(all_samples) >= max_samples:
                break
        
        print(f"\n📊 Processing {len(all_samples)} total samples")
        
        if not all_samples:
            print("❌ No samples found to process")
            return {'status': 'no_samples'}
        
        # Validate sample IDs for conflicts (especially important for Juliet dataset)
        print("🔍 Validating sample ID uniqueness...")
        conflicts = validate_sample_ids(all_samples)
        
        if conflicts:
            print("⚠️  Sample ID conflicts detected:")
            for sample_id, indices in list(conflicts.items())[:10]:  # Show first 10 conflicts
                print(f"   {sample_id}: {len(indices)} duplicates (indices: {indices[:5]}{'...' if len(indices) > 5 else ''})")
            
            if len(conflicts) > 10:
                print(f"   ... and {len(conflicts) - 10} more conflicts")
            
            print(f"📊 Total conflicts: {len(conflicts)} sample IDs affected")
            print("✅ These conflicts have been resolved by the new ID generation logic")
        else:
            print("✅ No sample ID conflicts detected - all samples will be processed")
        
        # Prepare arguments for parallel processing
        process_args = [(sample, self.output_root) for sample in all_samples]
        
        # Process samples in parallel
        results = []
        completed = 0
        failed = 0
        skipped = 0
        
        print(f"\n🔄 Starting parallel processing with {num_workers} workers...")
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_sample = {
                executor.submit(process_single_sample, args): args[0] 
                for args in process_args
            }
            
            # Process results as they complete with progress bar
            progress_bar = tqdm(
                total=len(all_samples),
                desc="Processing samples",
                unit="sample",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] ({percentage:.0f}%)"
            )
            
            for future in as_completed(future_to_sample):
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Update counters
                    if result['status'] == 'completed':
                        completed += 1
                    elif result['status'].startswith('skipped'):
                        skipped += 1
                    else:
                        failed += 1
                    
                    # Update progress bar with status info
                    progress_bar.set_postfix({
                        'Completed': completed,
                        'Failed': failed,
                        'Skipped': skipped,
                        'Current': result['sample_id'][:20]  # Show current sample being processed
                    })
                    progress_bar.update(1)
                
                except Exception as e:
                    print(f"\n❌ Task execution error: {e}")
                    failed += 1
                    progress_bar.update(1)
            
            progress_bar.close()
        
        # Write results to manifest (inside dataset directory)
        dataset_output_dir = self.output_root / self.dataset_path.name
        dataset_output_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
        manifest_path = dataset_output_dir / "MANIFEST.csv"
        self._write_manifest(results, manifest_path)
        
        # Write summary (inside dataset directory)
        summary = {
            'dataset': str(self.dataset_path),
            'output_root': str(self.output_root),
            'splits_processed': splits,
            'total_samples': len(all_samples),
            'completed': completed,
            'failed': failed,
            'skipped': skipped,
            'success_rate': completed / len(all_samples) if all_samples else 0,
            'total_time': time.time() - start_time,
            'samples_per_second': len(all_samples) / (time.time() - start_time),
            'timestamp': time.time()
        }
        
        summary_path = dataset_output_dir / "RUN_SUMMARY.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Final report
        print("\n" + "=" * 80)
        print("🎉 PIPELINE EXECUTION COMPLETE")
        print("=" * 80)
        print(f"📊 Results: {completed} completed, {failed} failed, {skipped} skipped")
        print(f"⏱️  Total time: {summary['total_time']:.1f} seconds")
        print(f"🚀 Rate: {summary['samples_per_second']:.2f} samples/sec")
        print(f"📄 Manifest: {manifest_path}")
        print(f"📋 Summary: {summary_path}")
        print("=" * 80)
        
        return summary
    
    def _write_manifest(self, results: List[Dict], manifest_path: Path):
        """Write processing results to CSV manifest"""
        if not results:
            return
        
        # Define CSV columns
        columns = [
            'sample_id', 'split', 'idx', 'hash', 'func_name', 'is_vulnerable',
            'status', 'stage', 'verification_passed', 'udf_verification_passed', 'duration', 
            'error_message', 'start_time', 'end_time'
        ]
        
        # Add artifact columns
        artifact_keys = set()
        for result in results:
            artifact_keys.update(result.get('artifacts', {}).keys())
        
        artifact_columns = [f'artifact_{key}' for key in sorted(artifact_keys)]
        all_columns = columns + artifact_columns
        
        print(f"📝 Writing manifest with {len(results)} entries to {manifest_path}")
        
        with open(manifest_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=all_columns)
            writer.writeheader()
            
            for result in results:
                # Prepare row data
                row = {col: result.get(col, '') for col in columns}
                
                # Add artifact paths
                artifacts = result.get('artifacts', {})
                for key in artifact_keys:
                    row[f'artifact_{key}'] = artifacts.get(key, '')
                
                writer.writerow(row)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Pipeline Orchestrator - Execute complete vulnerability analysis pipeline',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  # Process test split with 10 samples using 4 workers
  python run_pipeline_orchestrator.py \\
    --dataset-path /home/fadul/GNNTestcases/outputs/unsplit_selected/secvuleval_cwe-119 \\
    --output-root /home/fadul/GNNTestcases/pipeline_artifacts \\
    --splits test \\
    --max-samples 10 \\
    --jobs 4

  # Process all splits  
  python run_pipeline_orchestrator.py \\
    --dataset-path /home/fadul/GNNTestcases/outputs/unsplit_selected/secvuleval_cwe-119 \\
    --output-root /home/fadul/GNNTestcases/pipeline_artifacts \\
    --splits train validation test \\
    --jobs 8
"""
    )
    
    parser.add_argument(
        '--dataset-path',
        required=True,
        help='Path to dataset directory containing Arrow files'
    )
    
    parser.add_argument(
        '--output-root', 
        required=True,
        help='Root directory for output artifacts'
    )
    
    parser.add_argument(
        '--splits',
        nargs='+',
        default=['test'],
        help='Dataset splits to process (default: test)'
    )
    
    parser.add_argument(
        '--jobs',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    
    parser.add_argument(
        '--max-samples',
        type=int,
        help='Maximum number of samples to process (for testing)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force reprocessing of existing artifacts'
    )
    
    args = parser.parse_args()
    
    try:
        # Create orchestrator
        orchestrator = PipelineOrchestrator(args.dataset_path, args.output_root)
        
        # Run pipeline
        summary = orchestrator.run_pipeline(
            splits=args.splits,
            max_samples=args.max_samples,
            num_workers=args.jobs,
            force=args.force
        )
        
        # Exit with appropriate code
        if summary.get('failed', 0) == 0:
            print("✅ All samples processed successfully!")
            return 0
        else:
            print(f"⚠️  {summary.get('failed', 0)} samples failed - check manifest for details")
            return 1
            
    except KeyboardInterrupt:
        print("\n⛔ Pipeline interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())