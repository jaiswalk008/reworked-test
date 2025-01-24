import subprocess
import sys

# Install torch CPU first with specific version
try:
    subprocess.check_call([
        sys.executable, 
        '-m', 
        'pip', 
        'install', 
        'torch==2.0.1',
        '--index-url',
        'https://download.pytorch.org/whl/cpu'
    ])
    print(f"Successfully installed torch CPU")
except subprocess.CalledProcessError as e:
    print(f"Error installing torch: {e}")

# Install flair normally
try:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'flair'])
    print(f"Successfully installed flair")
except subprocess.CalledProcessError as e:
    print(f"Error installing flair: {e}")

# Install parsernaam without dependencies
try:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'parsernaam', '--no-deps'])
    print(f"Successfully installed parsernaam")
except subprocess.CalledProcessError as e:
    print(f"Error installing parsernaam: {e}")

# from huggingface_hub import snapshot_download
# import os

# # Set the model path
# model_path = "/usr/src/app/.sandbox/Llama-3.2-1B-Instruct"

# # Create the directory if it doesn't exist
# os.makedirs(model_path, exist_ok=True)

# # Download the model
# model_id = "meta-llama/Llama-3.2-1B-Instruct"
# try:
#     snapshot_download(
#         repo_id=model_id,
#         local_dir=model_path,
#         token="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"  # You'll need to replace this with your Hugging Face token
#     )
#     print(f"Llama model successfully downloaded to {model_path}")
# except Exception as e:
#     print(f"Error downloading model: {e}")
