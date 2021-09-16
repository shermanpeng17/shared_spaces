from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import datetime
import zipfile
import shutil

start_date = str(datetime.date.today())

log_file = open('C:\\ETLs\\Shared_Spaces\\Logs\\Update_AGOL' + start_date + '.txt', 'w')

sidewalk_curbside_zip = "C://ETLs//Shared_Spaces//shared_spaces_data.zip"

shared_spaces_merged_zip = 'C://ETLs//Shared_Spaces//for_agol.gdb.zip'

street_closure_zip = "C://ETLs//Shared_Spaces//street_closure.zip"

private_lot_zip = "C://ETLs//Shared_Spaces//private_lot.zip"


def write_to_file_and_print(message):
    log_file.write(str(datetime.datetime.now()) + ': ' + message + '\n')
    log_file.flush()
    print(str(datetime.datetime.now()) + ': ' + message)


gis = GIS("https://sfgov.maps.arcgis.com", "your_username", "your_password")
message = "logged in as " + str(gis.properties.user.username)
write_to_file_and_print(message)

sidewalk_curbside_id = '7b13244930054085bc0eaf4fb83c521d'
shared_spaces_merged_id = '3a9a1b8161c14e7dac913de0f2580d47'
street_closure_id = '94f96b9a49474bed829e914aa4cd7132'
private_lot_id = '2807f1872bfa46278a4c19643ab773f4'

sidewalk_parking_item = gis.content.get(sidewalk_curbside_id)
shared_spaces_merged_item = gis.content.get(shared_spaces_merged_id)
street_closure_item = gis.content.get(street_closure_id)
private_lot_item = gis.content.get(private_lot_id)

try:
    # ------- MAKE WEEKLY COPIES ON MONDAY----------
    if datetime.date.today().weekday() == 0:
        sidewalk_curb_back_up = "C:\\ETLs\\Shared_Spaces\\backup_copies\\sidewalk_curbside_" + start_date + ".zip"
        shared_spaces_merged_backup = "C:\\ETLs\\Shared_Spaces\\backup_copies\\shared_spaces_merged_" + start_date + ".zip"
        street_closure_backup = "C:\\ETLs\\Shared_Spaces\\backup_copies\\street_closure_" + start_date + ".zip"
        private_lot_backup = "C:\\ETLs\\Shared_Spaces\\backup_copies\\private_lot_" + start_date + ".zip"

        shutil.copy(sidewalk_curbside_zip, sidewalk_curb_back_up)
        shutil.copy(shared_spaces_merged_zip, shared_spaces_merged_backup)
        shutil.copy(street_closure_zip, street_closure_backup)
        shutil.copy(private_lot_zip, private_lot_backup)
        message = "made backup copies of shared spaces data"
        write_to_file_and_print(message)
        
    # ------- UPDATE SHARED SPACES FEATURE SERVICE ----------
    sidewalk_curbside_feature_layer = FeatureLayerCollection.fromitem(sidewalk_parking_item)
    sidewalk_curbside_feature_layer.manager.overwrite(sidewalk_curbside_zip)
    message = 'updated sidewalk and parking data on agol!'
    write_to_file_and_print(message)

    private_lot_feature_layer = FeatureLayerCollection.fromitem(private_lot_item)
    private_lot_feature_layer.manager.overwrite(private_lot_zip)
    message = 'updated private lot data on agol!'
    write_to_file_and_print(message)

    street_closure_feature_layer = FeatureLayerCollection.fromitem(street_closure_item)
    street_closure_feature_layer.manager.overwrite(street_closure_zip)
    message = 'updated street closure on agol!'
    write_to_file_and_print(message)


    shared_spaces_merged_feature_layer = FeatureLayerCollection.fromitem(shared_spaces_merged_item)
    shared_spaces_merged_feature_layer.manager.overwrite(shared_spaces_merged_zip)
    message = 'updated shared spaces merged on agol'
    write_to_file_and_print(message)

    # ------- GET COUNT OF UPDATED DATA ----------
    sidewalk_parkinglane_layer = sidewalk_parking_item.layers[0]
    num_of_rows_updated = sidewalk_parkinglane_layer.query(return_count_only=True)
    message = 'updated dataset (sidewalk/parkinglane) now has ' + str(num_of_rows_updated)
    write_to_file_and_print(message) 

    shared_spaces_merged_layer = shared_spaces_merged_item.layers[0]
    num_of_rows_updated = shared_spaces_merged_layer.query(return_count_only=True)
    message = 'updated dataset (shared_spaces) now has ' + str(num_of_rows_updated)
    write_to_file_and_print(message) 

    street_closure_layer = street_closure_item.layers[0]
    num_of_rows_updated = street_closure_layer.query(return_count_only=True)
    message = 'updated dataset (street closure) now has ' + str(num_of_rows_updated)
    write_to_file_and_print(message) 

    private_lot_layer = private_lot_item.layers[0]
    num_of_rows_updated = private_lot_layer.query(return_count_only=True)
    message = 'updated dataset (private lot) now has ' + str(num_of_rows_updated)
    write_to_file_and_print(message) 

except Exception as e:
    print(str(e))
    message = str(e)
    write_to_file_and_print(message)
