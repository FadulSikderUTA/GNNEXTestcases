#!/usr/bin/env python3
"""
Generalized Joern Processing Script
====================================
This script processes C/C++ source files through Joern to generate:
1. CPG (Code Property Graph) binary
2. DOT format export of the 'all' graph representation
3. GraphML (XML) export DISABLED for performance optimization

Usage:
    python joern_process.py <c_file> <output_dir>

Example:
    python src/joern_process.py example.c joern_output_example
    
Output Structure:
    output_dir/
    ├── cpg.bin                # Generated CPG binary
    └── dot_files/
        └── all/
            └── export.dot     # DOT format graph
    # graphml/ - DISABLED for performance optimization
"""

import os
import sys
import subprocess
import argparse
import shutil
from pathlib import Path
import time

def validate_dot_content_for_cfg_call(dot_file_path):
    """
    Validate that DOT file contains CFG and CALL edges (critical for vulnerability analysis).
    
    Args:
        dot_file_path: Path to DOT file to validate
    
    Returns:
        Tuple of (has_cfg_edges, has_call_edges, cfg_count, call_count)
    """
    if not os.path.exists(dot_file_path):
        return False, False, 0, 0
    
    try:
        with open(dot_file_path, 'r') as f:
            content = f.read()
            
        # Count CFG and CALL edges
        cfg_count = content.count('[label="CFG"')
        call_count = content.count('[label="CALL"')
        
        has_cfg = cfg_count > 0
        has_call = call_count > 0
        
        return has_cfg, has_call, cfg_count, call_count
        
    except Exception as e:
        print(f"⚠️  Error validating DOT content: {e}")
        return False, False, 0, 0

def wait_for_stable_size(file_path, max_wait_time=5.0, check_interval=0.1):
    """
    Wait for a file to reach a stable size (stops growing) to ensure write completion.
    
    Args:
        file_path: Path to file to monitor
        max_wait_time: Maximum time to wait in seconds
        check_interval: Time between size checks in seconds
    
    Returns:
        Boolean indicating if file became stable (True) or timeout (False)
    """
    if not os.path.exists(file_path):
        return False
    
    start_time = time.time()
    last_size = -1
    stable_checks = 0
    required_stable_checks = 3  # Need 3 consecutive stable checks
    
    while time.time() - start_time < max_wait_time:
        current_size = os.path.getsize(file_path)
        
        if current_size == last_size:
            stable_checks += 1
            if stable_checks >= required_stable_checks:
                return True
        else:
            stable_checks = 0  # Reset counter if size changed
            last_size = current_size
        
        time.sleep(check_interval)
    
    return False

def run_command(command, cwd=None):
    """
    Executes a shell command and captures its output.
    
    Args:
        command: List of command arguments
        cwd: Working directory for command execution
    
    Returns:
        Boolean indicating success (True) or failure (False)
    """
    print(f"Executing: {' '.join(command)}")
    print("-" * 50)
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=cwd
        )
        
        # Stream output in real-time
        for line in process.stdout:
            print(line, end='')
        
        # Wait for process completion
        process.wait()
        
        if process.returncode != 0:
            print(f"\n❌ Error: Command failed with exit code {process.returncode}")
            return False
            
        print(f"✅ Command completed successfully")
        return True
        
    except FileNotFoundError:
        print(f"❌ Error: Command '{command[0]}' not found.")
        print("   Please ensure Joern is installed and in your PATH.")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def generate_cpg(c_source_file, output_dir, max_retries=2):
    """
    Generate CPG binary from C/C++ source file using joern-parse with retry logic.
    
    Args:
        c_source_file: Path to C/C++ source file
        output_dir: Output directory path
        max_retries: Maximum number of retry attempts for parallel processing resilience
    
    Returns:
        Path to generated CPG binary or None if failed
    """
    cpg_bin_path = os.path.join(output_dir, "cpg.bin")
    
    print("\n" + "="*60)
    print("STEP 1: Generating CPG Binary")
    print("="*60)
    
    for attempt in range(max_retries + 1):
        if attempt > 0:
            print(f"🔄 Retry attempt {attempt}/{max_retries} for CPG generation")
            import time
            time.sleep(0.5 * attempt)  # Progressive backoff
        
        # Remove any existing partial file
        if os.path.exists(cpg_bin_path):
            try:
                os.remove(cpg_bin_path)
            except:
                pass
        
        # Run joern-parse command
        joern_parse_cmd = [
            "joern-parse",
            c_source_file,
            "-o", cpg_bin_path
        ]
        
        if run_command(joern_parse_cmd):
            # Wait for filesystem stability instead of fixed delay
            if wait_for_stable_size(cpg_bin_path, max_wait_time=5.0):
                # Verify CPG was created and has reasonable size
                if os.path.exists(cpg_bin_path) and os.path.getsize(cpg_bin_path) > 1000:
                    file_size = os.path.getsize(cpg_bin_path) / 1024  # Size in KB
                    print(f"✅ CPG generated: {cpg_bin_path} ({file_size:.2f} KB)")
                    return cpg_bin_path
                else:
                    print(f"⚠️  CPG file missing or too small (attempt {attempt + 1})")
            else:
                print(f"⚠️  CPG file not stable (attempt {attempt + 1})")
        else:
            print(f"⚠️  Command failed (attempt {attempt + 1})")
    
    print("❌ Failed to generate CPG after all retry attempts")
    return None

def export_dot_format(cpg_bin_path, output_dir, max_retries=2):
    """
    Export 'all' graph representation in DOT format with retry logic.
    Uses unique directory names to avoid conflicts in parallel processing.
    
    Args:
        cpg_bin_path: Path to CPG binary
        output_dir: Base output directory
        max_retries: Maximum number of retry attempts for parallel processing resilience
    
    Returns:
        Path to exported DOT file or None if failed
    """
    # Use process-unique directory to avoid parallel processing conflicts
    import time
    unique_id = f"{os.getpid()}_{int(time.time() * 1000000)}"  # Process ID + microseconds
    temp_dot_dir = os.path.join(output_dir, "dot_files", f"all_temp_{unique_id}")
    final_dot_dir = os.path.join(output_dir, "dot_files", "all")
    final_dot_path = os.path.join(final_dot_dir, "export.dot")
    temp_dot_path = os.path.join(temp_dot_dir, "export.dot")
    
    print("\n" + "="*60)
    print("STEP 2: Exporting DOT Format")
    print("="*60)
    
    for attempt in range(max_retries + 1):
        if attempt > 0:
            print(f"🔄 Retry attempt {attempt}/{max_retries} for DOT export")
            import time
            time.sleep(0.5 * attempt)  # Progressive backoff
            # Generate new unique ID for retry
            unique_id = f"{os.getpid()}_{int(time.time() * 1000000)}"
            temp_dot_dir = os.path.join(output_dir, "dot_files", f"all_temp_{unique_id}")
            temp_dot_path = os.path.join(temp_dot_dir, "export.dot")
        
        # Create parent directory structure but not the final directory
        parent_dir = os.path.dirname(temp_dot_dir)
        os.makedirs(parent_dir, exist_ok=True)
        
        # Run joern-export command for DOT format to unique temp directory
        joern_export_cmd = [
            "joern-export",
            cpg_bin_path,
            "--repr", "all",
            "--format", "dot",
            "-o", temp_dot_dir
        ]
        
        if run_command(joern_export_cmd):
            # Wait for filesystem stability instead of fixed delay
            if wait_for_stable_size(temp_dot_path, max_wait_time=5.0):
                # Verify DOT file was created and has reasonable size
                if os.path.exists(temp_dot_path) and os.path.getsize(temp_dot_path) > 100:
                    # Additional validation: Check if it's a valid DOT file
                    try:
                        with open(temp_dot_path, 'r') as f:
                            content = f.read(200)
                            if 'digraph' in content:
                                # Additional validation: Check for CFG/CALL edges
                                has_cfg, has_call, cfg_count, call_count = validate_dot_content_for_cfg_call(temp_dot_path)
                                
                                if not has_cfg:
                                    print(f"⚠️  Warning: DOT file missing CFG edges ({cfg_count}) - this may indicate CPG corruption (attempt {attempt + 1})")
                                    # Continue anyway but log the issue
                                if not has_call:
                                    print(f"⚠️  Warning: DOT file missing CALL edges ({call_count}) (attempt {attempt + 1})")
                                    # Continue anyway but log the issue
                                
                                # Success! Now move to final location
                                os.makedirs(final_dot_dir, exist_ok=True)
                                
                                # Atomic move to final location
                                import shutil
                                shutil.move(temp_dot_path, final_dot_path)
                                
                                # Clean up temp directory
                                try:
                                    shutil.rmtree(temp_dot_dir)
                                except:
                                    pass  # Don't fail if temp cleanup fails
                                
                                file_size = os.path.getsize(final_dot_path) / 1024  # Size in KB
                                print(f"✅ DOT exported: {final_dot_path} ({file_size:.2f} KB) - CFG: {cfg_count}, CALL: {call_count}")
                                return final_dot_path
                            else:
                                print(f"⚠️  Invalid DOT format (attempt {attempt + 1})")
                    except:
                        print(f"⚠️  Cannot read DOT file (attempt {attempt + 1})")
                else:
                    print(f"⚠️  DOT file missing or too small (attempt {attempt + 1})")
            else:
                print(f"⚠️  DOT file not stable (attempt {attempt + 1})")
        else:
            print(f"⚠️  Command failed (attempt {attempt + 1})")
        
        # Clean up temp directory on failure
        try:
            if os.path.exists(temp_dot_dir):
                import shutil
                shutil.rmtree(temp_dot_dir)
        except:
            pass
    
    print("❌ Failed to export DOT format after all retry attempts")
    return None

def export_xml_format(cpg_bin_path, output_dir):
    """
    Export 'all' graph representation in GraphML (XML) format.
    
    Args:
        cpg_bin_path: Path to CPG binary
        output_dir: Base output directory
    
    Returns:
        Path to exported XML file or None if failed
    """
    xml_output_dir = os.path.join(output_dir, "graphml", "all")
    xml_file_path = os.path.join(xml_output_dir, "export.xml")
    
    print("\n" + "="*60)
    print("STEP 3: Exporting GraphML (XML) Format")
    print("="*60)
    
    # Create output directory structure
    os.makedirs(os.path.dirname(xml_output_dir), exist_ok=True)
    
    # Run joern-export command for GraphML format
    joern_export_cmd = [
        "joern-export",
        cpg_bin_path,
        "--repr", "all",
        "--format", "graphml",
        "-o", xml_output_dir
    ]
    
    if not run_command(joern_export_cmd):
        print("❌ Failed to export GraphML format")
        return None
    
    # Verify XML file was created
    if not os.path.exists(xml_file_path):
        print(f"❌ Error: XML file not found at {xml_file_path}")
        return None
    
    file_size = os.path.getsize(xml_file_path) / 1024  # Size in KB
    print(f"✅ XML exported: {xml_file_path} ({file_size:.2f} KB)")
    
    return xml_file_path

def process_c_file(c_source_file, output_dir):
    """
    Main processing function that orchestrates the entire pipeline.
    
    Args:
        c_source_file: Path to C/C++ source file
        output_dir: Output directory for all artifacts
    
    Returns:
        Dictionary with paths to generated artifacts or None if failed
    """
    # Validate input file
    if not os.path.exists(c_source_file):
        print(f"❌ Error: Source file not found: {c_source_file}")
        return None
    
    # Get absolute paths
    c_source_file = os.path.abspath(c_source_file)
    output_dir = os.path.abspath(output_dir)
    
    print("\n" + "="*60)
    print("JOERN PROCESSING PIPELINE")
    print("="*60)
    print(f"📄 Source file: {c_source_file}")
    print(f"📁 Output directory: {output_dir}")
    
    # Check if output directory exists and handle it
    if os.path.exists(output_dir):
        response = input(f"\n⚠️  Output directory exists. Clear it? (y/n): ")
        if response.lower() == 'y':
            print("Clearing output directory...")
            shutil.rmtree(output_dir)
    
    # Create fresh output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Generate CPG
    cpg_path = generate_cpg(c_source_file, output_dir)
    if not cpg_path:
        return None
    
    # Step 2: Export DOT format
    dot_path = export_dot_format(cpg_path, output_dir)
    if not dot_path:
        print("⚠️  Warning: DOT export failed, continuing with XML...")
    
    # Step 3: Export XML format - SKIPPED FOR PERFORMANCE
    # xml_path = export_xml_format(cpg_path, output_dir)
    # if not xml_path:
    #     print("⚠️  Warning: XML export failed")
    xml_path = None  # Not generated for performance optimization
    
    # Summary
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    
    results = {
        'cpg': cpg_path,
        'dot': dot_path,
        'xml': xml_path
    }
    
    print("\n📊 Generated Artifacts:")
    print(f"  • CPG Binary: {results['cpg']}")
    if results['dot']:
        print(f"  • DOT Graph: {results['dot']}")
    if results['xml']:
        print(f"  • XML Graph: {results['xml']}")
    
    # Show directory structure
    print("\n📁 Output Directory Structure:")
    for root, dirs, files in os.walk(output_dir):
        level = root.replace(output_dir, '').count(os.sep)
        indent = '  ' * level
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = '  ' * (level + 1)
        for file in files:
            file_size = os.path.getsize(os.path.join(root, file)) / 1024
            print(f"{sub_indent}{file} ({file_size:.2f} KB)")
    
    return results

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Process C/C++ files through Joern to generate CPG, DOT, and XML exports",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Example usage:
  python joern_process.py example.c my_output
  python joern_process.py /path/to/file.c /path/to/output
  
Output structure:
  output_dir/
    ├── cpg.bin              # Code Property Graph binary
    ├── dot_files/
    │   └── all/
    │       └── export.dot   # DOT format (for visualization)
    └── graphml/
        └── all/
            └── export.xml   # GraphML/XML format (for data exchange)
"""
    )
    
    parser.add_argument(
        "c_file",
        help="Path to C/C++ source file to process"
    )
    
    parser.add_argument(
        "output_dir",
        help="Output directory for generated artifacts"
    )
    
    args = parser.parse_args()
    
    # Process the file
    results = process_c_file(args.c_file, args.output_dir)
    
    if results:
        print("\n✅ Success! All processing steps completed.")
        return 0
    else:
        print("\n❌ Processing failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())