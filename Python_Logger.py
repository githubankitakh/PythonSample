#!/usr/bin/python3
##DIA-2657 Created by Mehul Batra##
##DIA-2657 Updated by Praveen Kumar KS##
import json
import snowflake.connector as sf    
import sys
import time
import logging
import argparse


logger = logging.getLogger('Snowflake Return Analysis')
logger.setLevel(logging.DEBUG)
# Here we define our formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logs_handler_stdr = logging.StreamHandler()
#logs_handler_stdr.setLevel(logging.DEBUG)
logs_handler_stdr.setFormatter(formatter)
logger.addHandler(logs_handler_stdr)
MAX_RETRY_ATTEMPTS=3
my_parser = argparse.ArgumentParser()
my_parser.add_argument('--Snowuser', action='store', type=str, required=True)
my_parser.add_argument('--Snowpass', action='store', type=str, required=True)
my_parser.add_argument('--Snowaccount', action='store', type=str, required=True)
my_parser.add_argument('--Snowdatabase', action='store', type=str, required=True)
my_parser.add_argument('--Snowschema', action='store', type=str, required=True)
my_parser.add_argument('--Snowwarehouse', action='store', type=str, required=True)
my_parser.add_argument('--Snowrole', action='store', type=str, required=True)

args = my_parser.parse_args()
"""Snowflake"""

for i in range(0,MAX_RETRY_ATTEMPTS):
    try:
        logs = {'Info': 'Establishing connection with Snowflake'}
        logger.info(json.dumps(logs))
        connection=sf.connect(user=args.Snowuser,password=args.Snowpass,account=args.Snowaccount,database=args.Snowdatabase,schema=args.Snowschema,warehouse=args.Snowwarehouse,role=args.Snowrole)
        SNOW_CURSOR = connection.cursor()
        logs = {'Info': 'Established connection with Snowflake'}
        logger.info(json.dumps(logs))
        break
    except Exception as e:
        logs = {'Warning': "Exception occured " + str(e)}
        logger.warning(json.dumps(logs))
        logs = {'Info': "Retrying to attempt the connection with Snowflake"}
        logger.info(json.dumps(logs))
        time.sleep(5)
        if i >= MAX_RETRY_ATTEMPTS - 1:
            logs = {'Error': "Error occurred in connecting to Snowflake " + str(e)}
            logger.error(json.dumps(logs))
            sys.exit(-1)

insert_ips='''INSERT  into "FDR_DWH_DB_PROD"."ANALYTICS"."RETURNANALYSIS"
SELECT DISTINCT P.PACKAGEKEY,P.PRIMARYBARCODE, P.SRCPACKAGEID AS COREPACKAGEID,TO_DATE(CAST(p.Scandatekey as VARCHAR) , 'YYYYMMDD') INDUCTIONDATE , OD.ORGNAME, P.CONTAINERSTATUSKEY ,
IFNULL( P. BILLCYCLEDATEKEY,0) BILLEDCYCLE, IFNULL( P.INVOICEAUDITKEY,0) INVOICEAUDITKEY,  TO_DATE(CAST (IFNULL( P.DELIVERYDATEKEY,'19500101') as VARCHAR), 'YYYYMMDD') DWDELIVERYDATE, 0 AS REVPACKAGERATING,
'DWH' AS DELIVERYFLAG, 0 AS FTCONTAINERID, 'NULL' AS BOL, 'NULL' AS BOLCREATEDATE ,  'NULL'as SHIPTOFACILITY, 'NULL' AS COREDELIVERYDATE, CASE WHEN P.BILLCYCLEDATEKEY IS NOT NULL THEN 'BILLED' ELSE 'UNBILLED' END AS CATEGORY,
CASE WHEN  P.DELIVERYDATEKEY IS NOT NULL THEN 'DELIVERED' ELSE 'UNDELIVERED' END AS SUBCATEGORY1,'NULL' AS SUBCATEGORY2,'NULL' AS SUBCATEGORY3,'NULL' AS SUBCATEGORY4,'2020-01-03' AS SUBCATEGORY5,'LOADED' AS SUBCATEGORY6
FROM  "FDR_NGSDW_DB_PROD"."FDRDW"."PACKAGE" P
LEFT JOIN "FDR_NGSDW_DB_PROD"."FDRDW"."ORGDIM" OD
ON OD.ORGKEY=P.MERCHANTORGKEY
WHERE INDUCTIONDATE >= '2020-04-24';'''

update_rating_info='''UPDATE "FDR_DWH_DB_PROD"."ANALYTICS"."RETURNANALYSIS" RBA
SET REVPACKAGERATING =IFNULL(RP.REVPACKAGERATINGKEY,0)
    FROM ( SELECT P.PACKAGEKEY ,REVPACKAGERATINGKEY FROM  "FDR_NGSDW_DB_PROD"."FDRDW"."PACKAGE" P 
           LEFT JOIN  "FDR_NGSDW_DB_PROD"."FDRDW"."REVPACKAGERATING" RPR
ON RPR.PACKAGEKEY=P.PACKAGEKEY
         WHERE P.SCANDATEKEY >=20200424) RP
       WHERE RP.PACKAGEKEY=RBA.PACKAGEKEY;'''

update_llt='''UPDATE "FDR_DWH_DB_PROD"."ANALYTICS"."RETURNANALYSIS" RBA
SET DELIVERYFLAG ='LLT',
    DWDELIVERYDATE = TEMP.EVENTSTDDATETIME,
    SUBCATEGORY1='DELIVERED',
    rba.SUBCATEGORY5= DELVCREATEDATE
    FROM ( SELECT P.PACKAGEKEY , CAST( RTE. EVENTSTDDATETIME as DATE) EVENTSTDDATETIME ,IFNULL(CAST (RTE.CREATEDATE AS DATE),'2019-04-24' ) DELVCREATEDATE FROM  "FDR_NGSDW_DB_PROD"."FDRDW"."PACKAGE" P 
           LEFT JOIN "FDR_DWH_DB_PROD"."RETURNS"."REVTRACKING" RT
ON RT.PACKAGEKEY=P.PACKAGEKEY
JOIN "FDR_DWH_DB_PROD"."RETURNS"."REVTRACKINGEVENT"  RTE
on RT.REVTRACKINGID=RTE.REVTRACKINGID
And RTE.TRACKINGEVENTKEY=13  
         WHERE P.SCANDATEKEY>=20200424
         AND  P.DELIVERYDATEKEY IS NULL
        ) TEMP
       WHERE TEMP.PACKAGEKEY=RBA.PACKAGEKEY;'''


update_undelivered_bucket='''UPDATE   "FDR_DWH_DB_PROD"."ANALYTICS"."RETURNANALYSIS" rba
SET rba.FTCONTAINERID = UD.FTCONTAINERID,
    rba.BOL=UD.BOL,
    rba.BOLCREATEDATE= UD.BOLCREATEDATE ,
    rba.COREDELIVERYDATE=  UD.DELIVERYDATE ,
    rba.SHIPTOFACILITYNAME=UD.SHIPTOFACILITYNAME,
    rba.SUBCATEGORY6 = CASE WHEN UD.CONTSTATUS ='Container Opened' THEN 'OPENED' WHEN UD.CONTSTATUS ='Container Loaded' THEN 'LOADED' WHEN UD.CONTSTATUS ='Container Staged' THEN 'STAGED' ELSE  'NEW' END 
FROM (SELECT FTCONTAINERID, BOL, BOLCREATEDATE, DELIVERYDATE, SHIPTOFACILITYNAME, PACKAGEKEY,CONTSTATUS FROM "FDR_DWH_DB_PROD"."DEV"."UNDELIVERBUCKET" ) UD
WHERE UD.PACKAGEKEY= RBA.PACKAGEKEY ;'''



def update_table():
    try :
        logs = {'Info': "Upserting Return Analysis Table (Snowflake)"}
        logger.info(json.dumps(logs))

        SNOW_CURSOR.execute('''delete from FDR_DWH_DB_PROD.ANALYTICS.RETURNANALYSIS;''')

        SNOW_CURSOR.execute(insert_ips)
        InsertStatus = SNOW_CURSOR.execute(''' SELECT  "number of rows inserted" FROM  TABLE (RESULT_SCAN(LAST_QUERY_ID()))''').fetchone()
        logs = {'Info': "No. of Records Inserted " + str(InsertStatus[0])}
        logger.info(json.dumps(logs))
        SNOW_CURSOR.execute(update_rating_info)
        SNOW_CURSOR.execute(update_llt)
        SNOW_CURSOR.execute(update_undelivered_bucket)
        logs = {'Info': "Return Analysis Table Upserted (Snowflake)"}
        logger.info(json.dumps(logs))
    except Exception as e:
        logs = {'Warning': "Exception occured while Upserting Return Analysis Table " + str(e)}
        logger.warning(json.dumps(logs))
        SNOW_CURSOR.close()
        connection.close()
    return None
if __name__ == "__main__":
     update_table()
     SNOW_CURSOR.close()
     connection.close()
