# brainmaps_api_fcn

Functions to set and get data via the Google Brainmaps API, e.g to
* download raw or segmentation image data,
* download surface meshes
* download skeletons created based on the segmentation
* retrieve or modify an agglomeration graph

## API access
Authentication requires a client-secret.json from a Google project, that has the BrainMaps API enabled.

## Installation

This has only been tested using Python 3.7. 

### using conda
Best work in a dedicated environment. Activate and install packages available to conda first.
```
conda install python-snappy networkx numpy requests
```
Then install the remaining requirements using pip
```
pip install -r requirements.txt
```

### using pip

Then install the remaining requirements
```
pip install -r requirements.txt
```

Optional: for faster decompression, e.g. fordownloading subvolumes install python snappy

<details>
  <summary>expand</summary>
  <p>
    Python-snappy requires the snappy c libraries to be installed.snappy.

    Windows:
    download binaries from [C. Gohlke](https://www.lfd.uci.edu/~gohlke/pythonlibs/#python-snappy) and install using [pip](https://pip.pypa.io/en/latest/user_guide/#installing-from-wheels)

    Linux/MacOS:
    ```
    APT: sudo apt-get install libsnappy-dev
    RPM: sudo yum install libsnappy-devel
    Brew: brew install snappy
    ```
  </p>
</details>

## Usage Example
Get segment id at a certain voxel position:
```
from brainmaps_api_fcn.subvolume_request import SubvolumeRequest
svr = SubvolumeRequest(<path_to_client_secret>, volume_id)
arr = svr.get_subvolume([3014, 13292, 89], [1,1,1])
```

Download skeleton of a segment
```
from brainmaps_api_fcn.mesh_request import MeshRequest
mr = MeshRequest(<path_to_client_secret>, volume_id)
segment_id = 55360714
skel_graph = mr.download_skeleton(segment_id)
```

Retrieve connected segments from an agglomeration graph
```
from brainmaps_api_fcn.equivalences_request import EquivalenceRequests
er = EquivalenceRequests(<path_to_client_secret>, volume_id, change_stack_id)
segment_id = 55360714
edges = er.get_equivalence_list(segment_id)
```
