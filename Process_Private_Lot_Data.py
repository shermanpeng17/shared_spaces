# This script processes private lot data for the Shared Spaces Program
# Private lot data is downloaded through the AirTable API. 
# The data is then transformed into GIS data to figure out what parcel 
# they're in

import time
import pandas as pd
import arcpy
import os
import re
import zipfile
import requests
import json 
# etl_logger is a custom module created to log etl statues used for
# another purpose
sys.path.append("C:/ETLs/Tools")
import etl_logger

arcpy.env.overwriteOutput = True

start_date = str(datetime.date.today())
log_file = open('C:\\ETLs\\Shared_Spaces\\Logs\\Private_Lot' + start_date + '.txt', 'w')

etl_name = "Shared_Spaces_Private_Lots"
etl_logger.log_etl_has_started(etl_name)

def write_to_file_and_print(message):
    log_file.write(str(time.ctime()) + ': ' + message + '\n')
    log_file.flush()
    print(str(time.ctime()) + ': ' + message)


def remove_old_download_file(file_list_to_delete):
    for file_name in file_list_to_delete:
        shared_space_folder = "C:\\ETLs\\Shared_Spaces"
        for each_file in os.listdir(shared_space_folder):
            # Look for specific works in link
            if file_name in each_file:
                print('Removing download files')
                os.remove(os.path.join(shared_space_folder, each_file))
        message = "removed old private csv data"
        write_to_file_and_print(message)

def process_mapblklot_field_in_csv(blklot_str): 
    """Make new column called processed_blklot in csv"""
    
    try:
        processed_blklot = str(blklot_str)
        items_to_remove = ["/", "\\t", "-"]
        for item in items_to_remove:
            processed_blklot = processed_blklot.replace(item, "")
        correct_blklot_regex = "([\d]{4}[a-zA-Z]?[\d]{3}[a-zA-Z]?)"
        correct_blklot_match_list = re.findall(correct_blklot_regex, processed_blklot)
        
        if (len(correct_blklot_match_list) == 1):
            return correct_blklot_match_list[0]
        elif (len(correct_blklot_match_list) > 1):
            return processed_blklot
        
        if not processed_blklot.isdigit():
            blk_match_pattern = "block[\s]{0,}(#)?[\s]{0,}(\d\d\d\d)"
            lot_match_pattern = "lot[\s]{0,}(#)?[\s]{0,}(\d\d\d)"

            blk_match = re.search(blk_match_pattern, processed_blklot, re.IGNORECASE)
            blk_str = blk_match.group(2)
            lot_match = re.search(lot_match_pattern, processed_blklot, re.IGNORECASE)
            lot_str = lot_match.group(2)
            processed_blklot = blk_str + lot_str
            return processed_blklot
    except Exception as e:
        print("error")
        print(str(e))
        print(blklot_str)

def geoprocess_private_table():
    Processed_Private_Lot_Data_csv = "C:\\ETLs\\Shared_Spaces\\Processed_Private_Lot_Data.csv"
    working_gdb = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb"
    cleaned_private_lot_data_table = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\cleaned_private_lot_data"
    private_lot_data_table = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\private_lot_data"

    arcpy.Delete_management(private_lot_data_table)

    # Process: Table to Table
    arcpy.TableToTable_conversion(Processed_Private_Lot_Data_csv, working_gdb, "private_lot_data", "", "", "")
    # Process: Table to Table (2)
    arcpy.TableToTable_conversion(private_lot_data_table, working_gdb, "cleaned_private_lot_data", "", "", "")

    # Process: Truncate Table
    arcpy.TruncateTable_management(cleaned_private_lot_data_table)
    message = "got copy of private lot fields in cleaned table"
    write_to_file_and_print(message)

    i_cursor = arcpy.da.InsertCursor(cleaned_private_lot_data_table, "*")

    private_lot_table_fields = [f.name for f in arcpy.ListFields(private_lot_data_table)]

    id_field_index = private_lot_table_fields.index("_id")
    cleaned_block_lot_index = private_lot_table_fields.index("data__blockandlot")
    
    with arcpy.da.SearchCursor(private_lot_data_table, "*") as s_cursor:
        for s_row in s_cursor:
            record_id = s_row[id_field_index]
            if record_id is None:
                i_cursor.insertRow(s_row)
            else:
                record_id = record_id.lower()
                if "test" not in record_id:
                    blklot_regex = "([\d]{7}[a-zA-Z]?)"
                    block_lot = s_row[cleaned_block_lot_index]

                    blklot_match_list = re.findall(blklot_regex, block_lot)
                    if len(blklot_match_list) > 1:
                        new_insert_row = s_row[0: -1]
                        first_blklot = blklot_match_list[0]
                        new_insert_row += (first_blklot,)
                        i_cursor.insertRow(new_insert_row)
                    else:
                        i_cursor.insertRow(s_row)
    
    message = "inserted non test records into cleaned private lot table"
    write_to_file_and_print(message)

def zip_folder(folder_path, output_path):
    parent_folder = os.path.dirname(folder_path)  
    contents = os.walk(folder_path)  
    try:  
        zip_file = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED)  
        for root, folders, files in contents:
            for file_name in files:
                absolute_path = os.path.join(root, file_name)  
                relative_path = absolute_path.replace(parent_folder + '\\','')  
                zip_file.write(absolute_path, relative_path)

    except IOError, message:  
        print message  
        sys.exit(1)  
    except OSError, message:  
        print message  
        sys.exit(1)  
    except zipfile.BadZipfile, message:  
        print message  
        sys.exit(1)  
    finally:  
        zip_file.close() 
        print("3")


def modify_supdist_string(sup_dist_num_string):
    """modify supervisor district string format
       1 => "D 01"  and 12 => "D 12" """
    if sup_dist_num_string:
        if len(str(sup_dist_num_string)) == 1:
            return 'D 0' + str(sup_dist_num_string)
        else:
            return 'D ' + str(sup_dist_num_string)
    return sup_dist_num_string

def get_gis_location_for_private_lot():
    # Local variables:
    cleaned_private_lot_data = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\cleaned_private_lot_data"
    gis_db_gisdata_Parcels_Current = "Database Connections\\cpc-postgis-1.sde\\gis_db.gisdata.Parcels_Current"
    gisdata_Parcels_Current_Laye = "gisdata.Parcels_Current_Laye"
    cleaned_private_lot_data = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\cleaned_private_lot_data"
    working_gdb = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb"
    final = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\final"
    gis_db_gisdata_AdministrativeBoundaries_Supervisorial_Districts = "Database Connections\\cpc-postgis-1.sde\\gis_db.gisdata.AdministrativeBoundaries_Supervisorial_Districts"
    final_with_sup_dist = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\final_with_sup_dist"
    QueryTable = "QueryTable"
    private_lot_application_parcels = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\private_lot_application_parcels"

    # -------------------- Do Query Table to do one to many join for private lot data to parcels Current --------------------

    # Process: Feature Class to Feature Class
    arcpy.FeatureClassToFeatureClass_conversion(gis_db_gisdata_Parcels_Current, working_gdb, "parcels_current", "", "", "")

    # Process: Make Query Table
    arcpy.MakeQueryTable_management("C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\cleaned_private_lot_data;C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\parcels_current", QueryTable, "USE_KEY_FIELDS", "", "", "parcels_current.blklot= cleaned_private_lot_data.processed_blklot")

    # Process: Feature Class to Feature Class (2)
    arcpy.FeatureClassToFeatureClass_conversion(QueryTable, working_gdb, "private_lot_application_parcels", "", "", "")

    message = "got private lot gis locations"
    write_to_file_and_print(message)

    # -------------------- Get Supervisor District --------------------
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(private_lot_application_parcels)
    field_mappings.addTable(gis_db_gisdata_AdministrativeBoundaries_Supervisorial_Districts)

    fields_to_keep = [f.name for f in arcpy.ListFields(private_lot_application_parcels)]
    fields_to_keep.append('supervisor')

    for field in field_mappings.fields:
        if field.name not in fields_to_keep:
            field_mappings.removeFieldMap(field_mappings.findFieldMapIndex(field.name))

    arcpy.SpatialJoin_analysis(private_lot_application_parcels, gis_db_gisdata_AdministrativeBoundaries_Supervisorial_Districts, final_with_sup_dist, "JOIN_ONE_TO_ONE", "KEEP_COMMON", field_mappings, "INTERSECT", "", "")

    message = "got supervisor district"
    write_to_file_and_print(message)

    arcpy.AddField_management(final_with_sup_dist, "SUPERVISOR_", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

    private_lot_fc_fields = [f.name for f in arcpy.ListFields(final_with_sup_dist)]
    sup_dist_index = private_lot_fc_fields.index("supervisor")
    modified_sup_dist_index = private_lot_fc_fields.index("SUPERVISOR_")

    with arcpy.da.UpdateCursor(final_with_sup_dist, "*") as u_cursor:
        for u_row in u_cursor:
            curr_sup_dist_val = u_row[sup_dist_index]
            modified_sup_dist = modify_supdist_string(curr_sup_dist_val)
            u_row[modified_sup_dist_index] = modified_sup_dist
            u_cursor.updateRow(u_row)

    message = "updated supervisor district column"
    write_to_file_and_print(message)
 
    arcpy.DeleteField_management(final_with_sup_dist, "supervisor")

    arcpy.AddField_management(final_with_sup_dist, "SUPERVISOR", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

    private_lot_fc_fields = [f.name for f in arcpy.ListFields(final_with_sup_dist)]
    sup_dist_index = private_lot_fc_fields.index("SUPERVISOR")
    modified_sup_dist_index = private_lot_fc_fields.index("SUPERVISOR_")

    with arcpy.da.UpdateCursor(final_with_sup_dist, "*") as u_cursor:
        for u_row in u_cursor:
            curr_sup_dist_val = u_row[modified_sup_dist_index]
            u_row[sup_dist_index] = curr_sup_dist_val
            u_cursor.updateRow(u_row)
    
    arcpy.DeleteField_management(final_with_sup_dist, "SUPERVISOR_")

def determine_final_approval_status():
    private_lot_final = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\final_with_sup_dist"

    arcpy.AddField_management(private_lot_final, "Approval_Status", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
    arcpy.AddField_management(private_lot_final, "Application_Type", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

    approval_mapping = {
        "Closed - Approved": "Approved",
        "Closed - Withdrawn": "Ineligible or Diverted",
        None: "On Hold",
        "On Hold - with applicant": "In Progress (Applicant)",
        "Closed - Disapproved": "Ineligible or Diverted"
    }

    field_names = [f.name for f in arcpy.ListFields(private_lot_final)]
    status_index = field_names.index("Status")
    approval_status_index = field_names.index("Approval_Status")
    application_type_index = field_names.index("Application_Type")

    with arcpy.da.UpdateCursor(private_lot_final, "*") as u_cursor:
        for u_row in u_cursor:
            curr_status_val = u_row[status_index]
            approval_mapped_val = approval_mapping[curr_status_val]
            u_row[approval_status_index] = approval_mapped_val
            u_row[application_type_index] = "Private Lot"

            u_cursor.updateRow(u_row)

    arcpy.DeleteField_management(private_lot_final, "Status")

    message = "Got final approval status based on mapping values"
    write_to_file_and_print(message)

def prep_for_agol_upload():
    """Make copy of private lot FC to staging gdb, 
        Delete sensitive fields,
        add indexes, then zip up,
        add application type field
    """
    for_agol_gdb = "C:\\ETLs\\Shared_Spaces\\private_lot_applications.gdb"
    agol_zipfile = "C:\\ETLs\\Shared_Spaces\\private_lot.zip"
    private_lot_final = "C:\\ETLs\\Shared_Spaces\\private_lot\\working.gdb\\final_with_sup_dist"

    determine_final_approval_status()
    arcpy.DeleteField_management(private_lot_final, "data__identity;data__hispanic;Cultural_affiliation_or_nationality;Your_preferred_language;data__businessownership;Join_Count;TARGET_FID;Field1;data__email;data__phone;Check_any_that_apply_to_you_;data__propertyowner_email;OBJECTID;asr_secured_roll")
    message = "deleted sensitive fields"
    write_to_file_and_print(message)

    # Process: Feature Class to Feature Class
    arcpy.FeatureClassToFeatureClass_conversion(private_lot_final, for_agol_gdb, "private_lot_applications", "", "", "")
    message = 'moved to agol gdb'
    write_to_file_and_print(message)

    zip_folder(for_agol_gdb, agol_zipfile)
    message = 'produced zip file for private lot data'
    write_to_file_and_print(message)


def construct_df_from_json(json):
    columns = ['_id', 'data__name', 'data__email', 'data__phone', 'data__businessname', 'data__businessaddress1', 
     'data__BAN', 'data__businessHours', 'data__ownerrname', 'data__identity', 'data__hispanic', 
     'Cultural_affiliation_or_nationality', 'Check_any_that_apply_to_you_', 'Your_preferred_language', 'data__upload1_photos', 
     'data__upload2_permission', 'data__address_same', 'data__outdoor__address', 'data__blockandlot', 
     'data__businesstype', 'data__businesstype_other', 'data__propertyowner_email', 'data__propertyowner_phone', 
     'data__describe_space_use', 'data__outdoorspace__use', 'data__businessownership', 'data__insurance', 
     'Status', 'closed_date', 'planner_assigned', 'filed_at', 'autonumber_id', 'PLN_SSP_permit']
    records = json["records"]
    private_lot_df = pd.DataFrame(columns=columns)
    for record in records:
        fields = record["fields"]

        obj = {}
        for column in columns:
            if column in fields:
                value_to_insert = fields[column]
                if isinstance(value_to_insert, bool) == False and isinstance(value_to_insert, list) == False and isinstance(value_to_insert, int) == False:
                    value_to_insert = value_to_insert.encode('utf8') 
                if column == "PLN_SSP_permit" or column == "data__upload1_photos" or column == "data__upload2_permission":
                    url = value_to_insert[0]["url"]
                    obj[column] = url
                else:
                    val = value_to_insert
                    if isinstance(value_to_insert, list):
                        concat_str = ", ".join(value_to_insert)
                        val = concat_str
                    obj[column] = val
            else:
                obj[column] = None

        private_lot_df = private_lot_df.append(obj, ignore_index=True)
    message = "got dataframe from private lot json"
    write_to_file_and_print(message)
    return private_lot_df


def get_private_lot_data():
    token = ENTER_TOKEN_HERE
    private_lot_api_url = "https://api.airtable.com/v0/appeWqTbOH1KqbJCc/Private%20space%20applications"
    headers={
        'Authorization': "Bearer " + token,
        'Accept': 'application/json'
    }
    response = requests.get(private_lot_api_url, headers=headers)
    json_data = json.loads(response.text)
    message = "got private lot json data"
    write_to_file_and_print(message)
    return json_data

try:
    shared_space_folder = "C:\\ETLs\\Shared_Spaces"
    private_lot_csv_file = "Private space applications-Grid view.csv"
    processed_private_lot_csv = "Processed_Private_Lot_Data.csv"
    files_to_delete = [private_lot_csv_file, processed_private_lot_csv]

    remove_old_download_file(files_to_delete)
    private_lot_json = get_private_lot_data()
    private_lot_df = construct_df_from_json(private_lot_json)
    private_lot_df.to_csv("C:\\ETLs\\Shared_Spaces\\Private space applications-Grid view.csv", index=False)

    # use pandas to process csv file (mainly blklot field)
    df = pd.read_csv(os.path.join(shared_space_folder, private_lot_csv_file))
    df["processed_blklot"] = df["data__blockandlot"].apply(process_mapblklot_field_in_csv)
    df["processed_blklot"] = df["processed_blklot"].astype("str")
    df.to_csv(os.path.join(shared_space_folder, processed_private_lot_csv))
    message = "got processed private lot csv file"
    write_to_file_and_print(message)

    geoprocess_private_table()
    get_gis_location_for_private_lot()

    prep_for_agol_upload()

    message = "finished"
    write_to_file_and_print(message)
    etl_logger.log_etl_has_completed(etl_name, True)

except Exception, e:
    etl_logger.log_etl_has_completed(etl_name, False)
    print("error")
    print(str(e))
    write_to_file_and_print(str(e))






