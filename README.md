# rmb_pas_infrastructure_transition_pipeline


**Pipeline Discription**: This pipeline hunts through all Pastoral_Infrastructure/Server_Upload subdirectories within the Pastoral_Property directory for shapefiles  and pdf's. 
If any of the following files are located the following checks/ corrections will be made and files will be moved to the For Transition subdirectory in the zzzzz directory.


**Pipeline hunting for**:
 - Points.shp
 - Polylines.shp
 - Polygons.shp
 - .pdf

**Pipeline process**:
Python script: Step1_1_Initiate.py
1. Searches in the For Migration directory for any files awaiting migration to the final dataset.
   - If data is located script ends
   - If No data is located the script continues.  
2. Create a temporary directory (user_YYYYMMDD) within the output_dir command argumnet location.

Python script: step1_2_search_folders.py
1. Searches for shapefiles in the Pastroal_District/Property/Server_Upload/YEAR/ directories for all properties
2. Loops through the Server_Upload subdirectories and files shapefile paths into data type specific lists 
(i.e.  Points, Lines, Paddocks, Poly_Other).
3. Checks that the data types were correctly filed by using Shaply geometry (i.e. Points are Points, and Lines are Lines) and sorts to appropriate lists. Can not correct Polygons and will stop pipline if data is incorrectly filed.
4. Creats a list called delete_files_list to delete original files at completion of the pipeline.
5. Sort input dataframes (Points, Lines etc.) into either the checked_list or faulty_list based on it having only the required column headings. **Note**: Pipeline will correct order when required.
6. Filter out 1 values (True) in the DELETE feature (retain 0 and 2).



## Command Arguments

The following command arguments are required:
- **Output Dir**: String object containing the thath to the temporaty export directory
      Default path: Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs
- **Pastoral Districts Directory**: String object contining the path to the Pastoral_Districts directory in the Spatial/Working drive.
     Default path: U:\Pastoral_Districts
- **Remote Desktop** String object containing the workflow command, for the current procedures you should only use remote.
    Default string: remote
- **Transition Dir**: String object containing the path to the Transition Directory.
    Default path U:\Pastoral_Infrastructure\Transition
- **Assets Dir**:  String object contining the path to all requiresd assets for this pipeline to function.
    Default path: E:\DEPWS\code\rangeland_monitoring\rmb_infrastructure_transition_pipeline\assets
 - **year** Integer object contining the year in which you wish to hunt for (i.e. 2021).




