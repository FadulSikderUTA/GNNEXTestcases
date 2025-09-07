# Environment Migration Instructions (No Sudo Access)

## Current Machine (Capture Environment)

```bash
# Already completed - these files are in the repo:
# env/requirements.lock.txt  (exact Python package versions)  
# env/system_versions.txt    (system tool versions)
# env/environment.sh         (environment variables - now with user-space paths)
```

## New Machine (Shared GPU - No Sudo Access)

### Step 1: Check What's Already Available

```bash
# Test if system tools are already installed
java -version    # Check Java availability
javac -version   # Check Java compiler  
dot -V          # Check Graphviz availability
python3 --version # Check Python availability
```

**If all tools are available system-wide, skip to Step 4 (Python Environment)**

### Step 2: User-Space Tool Installation

#### Install Java (if not available system-wide)
```bash
mkdir -p ~/local && cd ~/local
wget https://download.java.net/java/GA/jdk17/0d483333a00540d886896bac774ff48b/35/GPL/openjdk-17_linux-x64_bin.tar.gz
tar -xzf openjdk-17_linux-x64_bin.tar.gz
rm openjdk-17_linux-x64_bin.tar.gz
```

#### Install Graphviz (if not available system-wide)
```bash
# Option A: Using Miniconda/Mambaforge (recommended)
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh"
bash Mambaforge-Linux-x86_64.sh -b -p ~/mambaforge
~/mambaforge/bin/mamba install -c conda-forge graphviz -y

# Option B: Build from source (more complex, skip if Option A works)
# This requires downloading and compiling Graphviz manually
```

### Step 3: Joern Installation (User Directory)

```bash
# Install Joern to user directory
mkdir -p ~/local
curl -fsSL https://github.com/joernio/joern/releases/latest/download/joern-install.sh -o /tmp/joern-install.sh
chmod +x /tmp/joern-install.sh
bash /tmp/joern-install.sh --install-dir "$HOME/local/joern"
```

### Step 4: Python Environment

```bash
# Clone/copy repository
cd /path/to/GNNTestcases

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install exact package versions (for exact replication)
pip install -r env/requirements.lock.txt

# OR install with flexibility (latest compatible versions)
pip install -r requirements.txt
```

### Step 5: Environment Configuration

#### Check what installation method you used:

**If using system-wide Java/Graphviz:**
```bash
cat > ~/gnn_env.sh << 'EOF'
# System-wide tools with user-space Joern
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Adjust path as needed
export JOERN_HOME=$HOME/local/joern
export PATH="$JOERN_HOME:$PATH"
EOF
```

**If using user-space installations:**
```bash
cat > ~/gnn_env.sh << 'EOF'
# User-space installations
export JAVA_HOME=$HOME/local/jdk-17.0.2
export JOERN_HOME=$HOME/local/joern  
export PATH="$HOME/mambaforge/bin:$JAVA_HOME/bin:$JOERN_HOME:$PATH"
EOF
```

### Step 6: Usage

```bash
# Every time you use the environment:
source ~/gnn_env.sh
source .venv/bin/activate

# Verify setup
java -version
javac -version  
dot -V
joern --help
python -c "import pandas,networkx,pyarrow; print('✓ Ready')"
```

### Step 7: Persistent Setup (Optional)

```bash
# Add to your shell profile for automatic loading
echo "source ~/gnn_env.sh" >> ~/.bashrc
echo "cd /path/to/GNNTestcases && source .venv/bin/activate" >> ~/.bashrc
```

## Minimal Sudo Requirements (If Needed)

If the shared GPU is missing basic tools, ask your supervisor to run:

```bash
# Minimal system packages needed
sudo apt-get update
sudo apt-get install -y curl wget build-essential  # Basic tools only

# Only if Java/Graphviz not available and user-space install fails:
sudo apt-get install -y openjdk-17-jdk graphviz
```

## Troubleshooting

### If Java installation fails:
- Try different JDK distribution: Eclipse Temurin, Amazon Corretto
- Check architecture: `uname -m` (use appropriate download)

### If Graphviz installation fails:  
- Try system python package: `pip install pygraphviz` (might work even without system graphviz)
- Ask supervisor for: `sudo apt-get install graphviz-dev libgraphviz-dev`

### If Joern installation fails:
- Download manually from releases page
- Extract to `~/local/joern` and make scripts executable

## Quick Test Script

Save as `test_setup.sh`:
```bash
#!/bin/bash
source ~/gnn_env.sh
source .venv/bin/activate

echo "=== Environment Test ==="
echo "Java: $(java -version 2>&1 | head -n1)"  
echo "Javac: $(javac -version 2>&1)"
echo "Dot: $(dot -V 2>&1 | head -n1)"
echo "Joern: $(joern --help 2>&1 | grep -i version || echo 'Available')"
echo "Python packages:"
python -c "import pandas,networkx,pydot,graphviz,matplotlib,scipy,pyarrow; print('✓ All imported successfully')" 2>/dev/null || echo "✗ Some packages missing"
echo "=== Test Complete ==="
```

This approach minimizes sudo requirements while providing flexible installation options based on what's available on your shared GPU environment.