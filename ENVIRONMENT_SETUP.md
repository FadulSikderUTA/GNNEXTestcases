# Environment Migration Instructions

## Current Machine (Capture Environment)

Run these commands in your current repository to capture the environment state:

```bash
# Already completed - these files are now in the repo:
# env/requirements.lock.txt  (exact Python package versions)
# env/system_versions.txt    (system tool versions)
# env/environment.sh         (environment variables)
```

## New Machine (Recreate Environment)

### Step 1: System Dependencies

```bash
# Ubuntu/Debian
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jdk graphviz curl ca-certificates git

# Verify installations
java -version
javac -version
dot -V
```

### Step 2: Clone Repository

```bash
# Clone your repository
git clone <your-repo-url>
cd GNNTestcases

# Or copy the repository files to the new machine
```

### Step 3: Python Environment

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install exact package versions (for exact replication)
pip install -r env/requirements.lock.txt

# OR install with flexibility (latest compatible versions)
pip install -r requirements.txt
```

### Step 4: Joern Setup

Choose one option:

**Option A: Use setup script (recommended)**
```bash
bash setup_joern_env.sh
# Follow the printed instructions to add exports to your shell profile
```

**Option B: Manual Joern install**
```bash
# Install Joern to home directory
curl -fsSL https://github.com/joernio/joern/releases/latest/download/joern-install.sh -o /tmp/joern-install.sh
chmod +x /tmp/joern-install.sh
bash /tmp/joern-install.sh --install-dir "$HOME/joern-distribution"
```

### Step 5: Environment Variables

```bash
# Source the environment file
source env/environment.sh

# For persistence, add to your shell profile (~/.bashrc or ~/.zshrc)
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export JOERN_HOME=$HOME/joern-distribution' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$JOERN_HOME:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### Step 6: Verification

```bash
# Activate virtual environment
source .venv/bin/activate

# Test all components
java -version && javac -version
dot -V
joern --help
python -c "import pandas, networkx, pydot, graphviz, matplotlib, scipy, pyarrow; print('✓ All Python packages imported successfully')"

# Optional: Run a quick test
python src/stage_one/run_pipeline_orchestrator.py --help
```

## Quick Setup Script

For convenience, you can create this setup script on the new machine:

```bash
#!/bin/bash
# save as: quick_setup.sh

set -e

echo "=== GNNTestcases Environment Setup ==="

# System dependencies
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jdk graphviz curl ca-certificates

# Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r env/requirements.lock.txt

# Joern setup
bash setup_joern_env.sh

echo "=== Setup Complete ==="
echo "Run: source .venv/bin/activate && source env/environment.sh"
echo "Then verify with the commands in Step 6"
```

## Notes

- Java 17 JDK is required for Joern
- Your current setup uses Java 21 runtime with Java 17 compiler, which works fine
- Graphviz is needed for graph visualization
- The .venv directory should NOT be copied - always create fresh virtual environments
- System tool versions are captured in `env/system_versions.txt` for reference