# rmb_pas_infrastructure_transition_pipeline


**Pipeline Discription**: This pipeline hunts through all Pastoral_Infrastructure/Server_Upload subdirectories within the Pastoral_Property directory for shapefiles  and pdf's. 
If any of the following files are located the following checks/ corrections will be made and files will be moved to the For Transition subdirectory in the zzzzz directory.


**Pipeline hunting for**:
 - Points.shp
 - Polylines.shp
 - Polygons.shp
 - .pdf

**Pipeline process**:




## Command Arguments

    - **Output Dir**: String object containing the thath to the temporaty export directory
      Default path: Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs

    - **Pastoral Districts Directory**: String object contining the path to the Pastoral_Districts directory in the Spatial/Working drive.
     Default path: U:\Pastoral_Districts

    - **Remote Desktop** String object containing the workflow command, for the current procedures you should only use remote.
    Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote")
    p.add_argument('-t', '--transition_dir', type=str, help='Directory path for outputs.',
                   default=r"U:\Pastoral_Infrastructure\Transition")

    p.add_argument('-a', '--assets_dir', type=str, help='Directory path containing required shapefile structure.',
                   default=r'E:\DEPWS\code\rangeland_monitoring\rmb_infrastructure_transition_pipeline\assets')

    p.add_argument('-y', '--year', type=str, help='Enter the year (i.e. 2001).')




