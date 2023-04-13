# climate-datasets
The purpose is to download the climate data set and upload the Ckan and make it available for the Datahub climate observer.

Prerequisite:
python
Ckan api Key
Ckan resources id's
The scripts will be ran as a cron job once a day in the backend.

installation 
pip3 install requirements.txt

Folders:
Jupiter_file.
contains jupiter notebook of various scripts to:
-convert .nc to csv.
-to list contries.
-population density. ect

Python_file.
contains python script that were converted jupiter notebook. The scripts  of various scripts to:
-convert .nc to csv.
-to list contries.
-population density. ect
in addation, 
- download dataset
-upload the resources to Ckan

end-to-end(Main Script).
contain 3 scripts that will download the climate data and convert to csv file and upload the resources to CKAN.
- Temperature script
- Precipitation script
- Disaster script

1st Step
All 3 does similar job, the script first download the data set from the below:
The .nc files  are stored in <specified_to_store_.nc>folder.
the CSV files are stored in <specified_to_store_.nc> folder.
The .nc files are used to create the CSV files.

Tempetature contains 3 .nc files for Tmax, Tavg and Tmin where each file will produce 2 CSV file for climatology and temperature. The CSV files are stored in .files folder.
The Temperature files are downloaded on https://berkeleyearth.org/data/

download_Tavg="https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/Gridded/Complete_TAVG_LatLong1.nc"
download_Tmin="https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/Gridded/Complete_TMIN_LatLong1.nc"
download_Tmax="https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/Gridded/Complete_TMAX_LatLong1.nc"

Precipitation only have one .nc file that only produce a single CSV file.
The precipitation file is downloaded on http://climexp.knmi.nl/GPCCData/gpcc_10_combined.nc

Disater only have one .nc file that only produce a single CSV file.
The precipitation file is downloaded on https://public.emdat.be/login


**Second step**
The code will convert the .nc file to their respective CSV file

**Third step**
The resources are then uploaded to Ckan and made available for the Datahub climate observer.


