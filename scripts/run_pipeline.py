# run_pipeline.py
import subprocess
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def run(script_name):
    path = os.path.join(SCRIPT_DIR, script_name)
    print("\nRunning:", script_name)
    res = subprocess.run([sys.executable, path], capture_output=False)
    if res.returncode != 0:
        print("Step failed:", script_name)
        sys.exit(res.returncode)

def main():
    os.chdir(SCRIPT_DIR)
    # Optional: generate sample JSONs (remove if you want real extraction)
    run("generate_samples.py")
    # If you have an extract.py that actually calls APIs, run that instead of generate_samples
    # run("extract.py")
    run("transform.py")
    run("load.py")
    run("analysis.py")
    print("\nPipeline finished successfully.")

if __name__ == "__main__":
    main()
