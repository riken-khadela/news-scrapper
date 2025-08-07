import subprocess
import os

def run_sh_in_background():
    script_path = "/home/user1/startups/news-scrapper/run_scrapper.sh" 

    subprocess.Popen(
        ["nohup", "bash", script_path],
        stdout=open(os.devnull, 'w'),
        stderr=open(os.devnull, 'w'),
        preexec_fn=os.setpgrp  # Detach from parent
    )

    print("✅ Shell script launched in background. Python script exiting...")

if __name__ == "__main__":
    run_sh_in_background()
