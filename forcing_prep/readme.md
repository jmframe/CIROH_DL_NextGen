## Camels AORC forcing aggregation

# Environment
Create a virtual environment in forcing_prep directory and install the required modules
```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
You may need to make the generate script executable
```sh
chmod +x generate.py
```

# Usage
To run, currently need to edit the list of camels ids in generate.py
Can also adjust the time range (years) variable

Run the process with

```sh
./generate.py "/path/to/git/CIROH_DL_NextGen/forcing_prep/config_aorc.yaml" 
```
or if generate is not executable
```sh
python generate.py "/path/to/git/CIROH_DL_NextGen/forcing_prep/config_aorc.yaml" 
```
