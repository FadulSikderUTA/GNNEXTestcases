#!/usr/bin/env python3
"""
Post-Processing HTML Generator
==============================

This script generates HTML DOT files and PNG visualizations for pipeline artifacts
that have already been processed by the main pipeline orchestrator.

It scans existing pipeline artifacts and generates:
1. HTML DOT files for CFG_CALL_original.dot (subgraph)
2. HTML DOT files for CFG_CALL_original_udf_filtered.dot (UDF-filtered)
3. PNG images for both

Usage:
    python post_process_html_generator.py \
        --artifacts-root /path/to/pipeline_artifacts \
        --dataset dataset_name \
        --jobs 4

Example:
    python post_process_html_generator.py \
        --artifacts-root /home/fadul/GNNTestcases/pipeline_artifacts \
        --dataset secvuleval_cwe-119 \
        --jobs 4
"""

import os
import sys
import argparse
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
from tqdm import tqdm

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import html_dot_generator as hdg
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure html_dot_generator.py is available")
    sys.exit(1)


def find_processable_samples(artifacts_root: Path, dataset_name: str) -> List[Path]:
    """Find all sample directories that have UDF artifacts ready for HTML processing
    
    Args:
        artifacts_root: Root directory of pipeline artifacts
        dataset_name: Dataset name to process
        
    Returns:
        List of sample directory paths ready for HTML processing
    """
    dataset_path = artifacts_root / dataset_name
    if not dataset_path.exists():
        print(f"❌ Dataset path does not exist: {dataset_path}")
        return []
    
    processable_samples = []
    
    # Walk through all splits and samples
    for split_dir in dataset_path.iterdir():
        if split_dir.is_dir():
            for sample_dir in split_dir.iterdir():
                if sample_dir.is_dir():
                    # Check if this sample has the required artifacts
                    subgraph_path = sample_dir / "subgraphs" / "CFG_CALL_original.dot"
                    udf_path = sample_dir / "udf" / "CFG_CALL_original_udf_filtered.dot"
                    
                    if subgraph_path.exists() and udf_path.exists():
                        # Check if HTML already exists (for resume capability)
                        html_dir = sample_dir / "html"
                        existing_files = []
                        if html_dir.exists():
                            existing_files = list(html_dir.glob("*.dot")) + list(html_dir.glob("*.png"))
                        
                        # Only add if HTML doesn't exist or is incomplete
                        if len(existing_files) < 4:  # Expect 2 DOT files + 2 PNG files
                            processable_samples.append(sample_dir)
    
    return sorted(processable_samples)


def process_single_sample_html(sample_dir: Path) -> Dict:
    """Generate HTML visualizations for a single sample
    
    Args:
        sample_dir: Path to sample directory
        
    Returns:
        Result dictionary with processing status
    """
    result = {
        'sample_dir': str(sample_dir),
        'sample_id': sample_dir.name,
        'status': 'unknown',
        'artifacts': {},
        'error_message': ''
    }
    
    try:
        # Create HTML directory
        html_dir = sample_dir / "html"
        html_dir.mkdir(parents=True, exist_ok=True)
        
        # Define source files
        sources = [
            (sample_dir / "subgraphs" / "CFG_CALL_original.dot", ""),
            (sample_dir / "udf" / "CFG_CALL_original_udf_filtered.dot", "_udf_filtered")
        ]
        
        generated_files = []
        
        for source_path, name_suffix in sources:
            if source_path.exists():
                try:
                    # Parse DOT file
                    nodes, edges = hdg.parse_dot_file(str(source_path))
                    base_name = source_path.stem + name_suffix
                    
                    # Generate HTML DOT file
                    html_dot_path = html_dir / f"{base_name}_html.dot"
                    png_path = html_dir / f"{base_name}_html.png"
                    
                    # Create HTML DOT
                    dot_content = hdg.create_html_dot_file(str(source_path), str(html_dot_path), nodes, edges)
                    result['artifacts'][f'html_dot{name_suffix}'] = str(html_dot_path)
                    generated_files.append(str(html_dot_path))
                    
                    # Generate PNG (best effort)
                    try:
                        if hdg.generate_png(dot_content, str(png_path)):
                            result['artifacts'][f'html_png{name_suffix}'] = str(png_path)
                            generated_files.append(str(png_path))
                    except Exception as png_error:
                        # PNG generation is optional, don't fail the whole process
                        result['artifacts'][f'html_png{name_suffix}_error'] = str(png_error)
                        
                except Exception as e:
                    result['error_message'] += f"Failed to process {source_path.name}: {e}; "
        
        # Determine overall status
        if generated_files:
            result['status'] = 'completed'
            result['generated_count'] = len(generated_files)
        else:
            result['status'] = 'failed'
            if not result['error_message']:
                result['error_message'] = 'No files were generated'
                
    except Exception as e:
        result['status'] = 'failed'
        result['error_message'] = str(e)
    
    return result


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Post-Processing HTML Generator - Generate HTML visualizations for processed pipeline artifacts',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  # Generate HTML for a specific dataset
  python post_process_html_generator.py \\
    --artifacts-root /home/fadul/GNNTestcases/pipeline_artifacts \\
    --dataset secvuleval_cwe-119 \\
    --jobs 4

  # Generate HTML for all samples in a dataset with 8 workers
  python post_process_html_generator.py \\
    --artifacts-root /home/fadul/GNNTestcases/pipeline_artifacts \\
    --dataset juliet_cwe121 \\
    --jobs 8
"""
    )
    
    parser.add_argument(
        '--artifacts-root',
        required=True,
        help='Root directory of pipeline artifacts'
    )
    
    parser.add_argument(
        '--dataset',
        required=True,
        help='Dataset name to process (e.g., secvuleval_cwe-119, juliet_cwe121)'
    )
    
    parser.add_argument(
        '--jobs', '-j',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    
    parser.add_argument(
        '--max-samples',
        type=int,
        help='Maximum number of samples to process (for testing)'
    )
    
    args = parser.parse_args()
    
    # Validate paths
    artifacts_root = Path(args.artifacts_root)
    if not artifacts_root.exists():
        print(f"❌ Artifacts root directory does not exist: {artifacts_root}")
        return 1
    
    print("="*80)
    print("🎨 POST-PROCESSING HTML GENERATOR")
    print("="*80)
    print(f"📂 Artifacts Root: {artifacts_root}")
    print(f"📊 Dataset: {args.dataset}")
    print(f"👥 Workers: {args.jobs}")
    if args.max_samples:
        print(f"🔢 Max Samples: {args.max_samples}")
    print("="*80)
    
    # Find processable samples
    print("🔍 Scanning for processable samples...")
    processable_samples = find_processable_samples(artifacts_root, args.dataset)
    
    if not processable_samples:
        print("❌ No processable samples found!")
        print("   Make sure the main pipeline has been run and UDF artifacts exist.")
        return 1
    
    # Apply max samples limit if specified
    if args.max_samples and args.max_samples < len(processable_samples):
        processable_samples = processable_samples[:args.max_samples]
        print(f"🔢 Limited to {args.max_samples} samples")
    
    print(f"✅ Found {len(processable_samples)} samples ready for HTML generation")
    
    # Process samples in parallel
    results = []
    completed = 0
    failed = 0
    
    print(f"\n🎨 Starting HTML generation with {args.jobs} workers...")
    
    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        # Submit all tasks
        future_to_sample = {
            executor.submit(process_single_sample_html, sample_dir): sample_dir
            for sample_dir in processable_samples
        }
        
        # Process results with progress bar
        progress_bar = tqdm(
            total=len(processable_samples),
            desc="Generating HTML",
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
                else:
                    failed += 1
                
                # Update progress bar
                progress_bar.set_postfix({
                    'Completed': completed,
                    'Failed': failed,
                    'Current': result['sample_id'][:20]
                })
                progress_bar.update(1)
                
            except Exception as e:
                print(f"\n❌ Task execution error: {e}")
                failed += 1
                progress_bar.update(1)
        
        progress_bar.close()
    
    # Write summary
    dataset_output_dir = artifacts_root / args.dataset
    summary_path = dataset_output_dir / "HTML_GENERATION_SUMMARY.json"
    
    summary = {
        'dataset': args.dataset,
        'total_samples': len(processable_samples),
        'completed': completed,
        'failed': failed,
        'success_rate': (completed / len(processable_samples) * 100) if processable_samples else 0,
        'results': results
    }
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\n📊 HTML Generation Summary:")
    print(f"   Total samples: {len(processable_samples)}")
    print(f"   ✅ Completed: {completed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📈 Success rate: {summary['success_rate']:.1f}%")
    print(f"   📄 Summary saved: {summary_path}")
    
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())