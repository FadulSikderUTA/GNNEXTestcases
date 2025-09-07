## SecVulEval CWE Subset Preparation

This workspace prepares six CWE-specific subsets from the SecVulEval dataset and creates train/validation/test splits for each.

- Source dataset: [arag0rn/SecVulEval](https://huggingface.co/datasets/arag0rn/SecVulEval)
- The dataset provides a single `train` split with 25,440 rows and includes `cwe_list` (one or more CWE IDs per vulnerable function). Non-vulnerable samples generally do not carry CWE labels.

### Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python data_prep/prepare_cwe_subsets.py \
  --output-dir outputs \
  --cwes CWE-190 CWE-476 CWE-121 CWE-122 CWE-416 CWE-415 \
  --seed 42 --test-ratio 0.1 --val-ratio 0.1
```

Outputs:
- `outputs/cwe_distribution_full.csv`: full CWE frequency table across the dataset
- `outputs/subset_split_summary.csv`: per-CWE counts and split sizes
- `outputs/secvuleval_<cwe-id>/`: on-disk Hugging Face datasets containing `train/validation/test`

### Notes on splitting

The upstream dataset exposes a single `train` split; the script creates random 80/10/10 splits for each CWE-specific subset. If you need project/repository-level de-duplication or group-aware splits to reduce leakage, extend the script to group by `project` or `filepath` before splitting.

### Pushing to the Hub (optional)

You can push subsets to your personal space on the Hugging Face Hub after `huggingface-cli login`:

```bash
python - <<'PY'
from datasets import DatasetDict, load_from_disk
from huggingface_hub import create_repo

repo_name = "your-username/secvuleval-cwe-190"
create_repo(repo_name, exist_ok=True)

dds = DatasetDict.load_from_disk("outputs/secvuleval_cwe-190")
dds.push_to_hub(repo_name)
PY
```

See the dataset card for schema details and context: [arag0rn/SecVulEval](https://huggingface.co/datasets/arag0rn/SecVulEval).


**Joern + Graph Setup**
- Purpose: Enable running Joern-based scripts and graph visualizations (aligns with your previous environment).
- Install deps: `source .venv/bin/activate && pip install -r requirements.txt`
- System deps (Ubuntu/Debian): OpenJDK 17 and Graphviz.
- One-shot helper: `bash setup_joern_env.sh` (installs system deps via apt, adds Python graph libs, and installs Joern under `tools/joern`).
- Exports to add to your shell profile (printed at the end of the script):
  - `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (or your platform’s JDK 17)
  - `export JOERN_HOME=$(pwd)/tools/joern`
  - `export PATH=$PATH:$JOERN_HOME:${JAVA_HOME:+$JAVA_HOME/bin}`
- Verify: `joern --help` and `dot -V` both work.

**Handy Commands**
- Activate venv: `source .venv/bin/activate`
- Use existing Joern install: `export JOERN_HOME=/home/fadul/joern-distribution && export PATH=$PATH:$JOERN_HOME:${JAVA_HOME:+$JAVA_HOME/bin}`
- Or repo-local Joern (if installed): `export JOERN_HOME=$(pwd)/tools/joern && export PATH=$PATH:$JOERN_HOME:${JAVA_HOME:+$JAVA_HOME/bin}`
- Quick checks:
  - `java -version && javac -version`
  - `joern --help | sed -n 's/^Version: //p' | head -n1`
  - `dot -V`
  - `python -c "import networkx, pydot, graphviz, matplotlib, scipy; print('python graph deps ok')"`
- Optional: prefer Java 17 for this shell session:
  - `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH`
