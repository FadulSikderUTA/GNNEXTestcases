#!/bin/bash
# Environment setup for GNNTestcases project
# Adjust paths based on your installation method

# Option 1: System-wide installation (if Java/Graphviz available)
# export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# export JOERN_HOME=$HOME/local/joern

# Option 2: User-space installation  
export JAVA_HOME=$HOME/local/jdk-17
export JOERN_HOME=$HOME/local/joern
export PATH="$HOME/mambaforge/bin:$JAVA_HOME/bin:$JOERN_HOME:$PATH"

# Check what's available and use accordingly