import papermill as pm
import multiprocessing
import os
import argparse
import json
from typing import Optional
from pathlib import Path

def find_project_root() -> Optional[Path]:
    current = Path(".").resolve()
    
    while True:
        if (current / ".git").exists():
            return current
        
        if current.parent == current:
            print("WARNING: No .git dir found")
            return current
              
        current = current.parent
        

PROJECT_ROOT = find_project_root()

configs = {
    "SNP": {
        "notebook": "notebooks/stock_up_down_parametrize.ipynb",
        "INDEX" : "SNP",
        "output_label": "SNP"
    },
    
    "GOLD": {
        "notebook": "notebooks/stock_up_down_parametrize.ipynb",
        "INDEX" : "GOLD",
        "output_label": "GOLD"
    },
    
     "SSE": {
         "notebook": "notebooks/stock_up_down_parametrize.ipynb",
         "INDEX" : "SSE",
         "output_label": "SSE"
     },
    
     "HANGSENG": {
         "notebook": "notebooks/stock_up_down_parametrize.ipynb",
         "INDEX" : "HANGSENG",
         "output_label": "HANGSENG"
     },
}

def run_papermill(config):
    '''
    Function to run notebook in parllel using papermill.
    '''
    
    # configuration variables
    config =config['config']
    notebook = str(PROJECT_ROOT/config['notebook'])
    #print("i am here")
    #print(notebook)
    output_label =config['output_label']
    
    # get name of notebook
    file_name = os.path.basename(notebook)
    notebook_name = os.path.splitext(file_name)[0]
    #notebook_name = notebook.split('\')[-1].replace('.ipynb','')
    out_dir = f'{PROJECT_ROOT}\\artifact_dir\\output_notebooks\\{notebook_name}'
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    output_path = f'{out_dir}\\{output_label}.ipynb'
    output_path_backup = output_path.replace('.ipynb','_backup.ipynb')
    #print(notebook_name)
    
    #print(out_dir)
    #print(output_path) 
    #print(output_path_backup)
    # print config to run
    print("-"*50)
    print(config)
    print("-"*50)
    
    # rename existing output file if need to
    if os.path.exists(output_path):
        # remove existing backup file if there is one
        if os.path.exists(output_path_backup):
            os.remove(output_path_backup)
        # rename existing output file
        os.rename(output_path, output_path_backup)
    #print(output_path) 
    #print(output_path_backup)
    # run notebook using papermill
    pm.execute_notebook(
                notebook,
                output_path,
                parameters= dict(INDEX=config["INDEX"])
                )


if __name__ == '__main__':
    for config in configs:

        # passing the config
        config_dict = [{'config': configs[config]}]

        p = multiprocessing.Process(
            target=run_papermill,
            args=(config_dict)
            )
        p.start()