import settings
import MySQLdb
import datetime
from datetime import date, timedelta
import sys
import os
import zipfile
from ftplib import FTP, error_reply
import logging


def db_connect(host=settings.DB_SERVER, user=settings.DB_USR, passwd=settings.DB_PWB, db=settings.DB_DB):
    """
    Connects to the AWS database, default parameters supplied for normal connection
    """
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_station_aws_id()\n" + str(e))
        sys.exit(1)

    return conn


def db_disconnect(conn):
    conn.close()


def get_observation_member(conn, aws_id, member, wdtf_id, in_date):
    """
    Add each station's values to the wdtf_file

    :param: live DB connection
    :param aws_id: string
    :param member: DB column
    :param wdtf_id: string
    :param in_date: date
    :return: XML - observationMember
    """
    logging.debug("get_hydrocollection " + aws_id + " " + member + " " + wdtf_id + " " + in_date.strftime("%Y-%m-%d"))
          
    dat_str = in_date.strftime("%Y-%m-%d")
    if member == 'rain_total':  # rainguages only
        sql = """
            SELECT
              CONCAT(DATE_FORMAT(stamp,'%Y-%m-%dT%H:%i:%s'),'+09:30') AS stamp,
              """" + member + """"
            FROM tbl_data_days
            WHERE
              stamp BETWEEN '""" + dat_str + """ 00:00:00'
              AND '""" + dat_str + """ 11:59:00'
              AND aws_id = '""" + aws_id + """'
            ORDER BY aws_id, stamp;
            """
    else:
        sql = """
            SELECT
                #CONCAT(DATE_FORMAT(stamp,'%Y-%m-%dT%H:%i:%s'),'+09:30') AS stamp,
                CONCAT(DATE_FORMAT(stamp - INTERVAL 9 HOUR - INTERVAL 30 MINUTE, '%Y-%m-%dT%H:%i:%s'),'+09:30') AS stamp,
                """ + member + """
            FROM tbl_data_minutes
            WHERE stamp BETWEEN '""" + dat_str + """ 00:00:00'
                AND '""" + dat_str + """ 11:59:00'
                AND aws_id = '""" + aws_id + """'
            ORDER BY aws_id, stamp;
            """

    logging.debug(sql)

    if member == 'rain_total':  # rainguages only
        gml_id = "TS_rain"
        feature = 'Rainfall_mm'
        interpol = 'PrecTot'
        units = 'mm'
    elif member == 'rain':
        gml_id = "TS_rain"
        feature = 'Rainfall_mm'
        interpol = 'InstTot'
        units = 'mm'
    elif member == 'Wavg':
        gml_id = "TS_Wavg"
        feature = 'WindSpeed_ms'
        interpol = 'InstVal'
        units = 'm/s'
    elif member == 'gsr':
        gml_id = "TS_gsr"
        feature = 'GlobalSolarIrradianceAverage_Wm2'
        interpol = 'PrecVal'
        units = 'W/m2'
    elif member == 'airT':
        gml_id = "TS_airT"
        feature = 'DryAirTemperature_DegC'
        interpol = 'InstVal'
        units = 'Cel'
    elif member == 'rh':
        gml_id = "TS_rh"
        feature = 'RelativeHumidity_Perc'
        interpol = 'InstVal'
        units = '%'

    #elif member == 'dp':
    #          gml_id = "TS_dp"
    #          feature = 'DewPoint_DegC'
    #          interpol = 'InstVal'
    #          units = 'Cel'


    wdtf_obsMember = '''
    <wdtf:observationMember>
        <wdtf:TimeSeriesObservation gml:id="''' + gml_id + '''">
            <gml:description>Weatherstation data</gml:description>
            <gml:name codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/feature/TimeSeriesObservation/''' + wdtf_id + '''/">1</gml:name>
            <om:procedure xlink:href="urn:ogc:def:nil:OGC::unknown"/>
            <om:observedProperty xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/property//bom/''' + feature + '''"/>
            <om:featureOfInterest xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/feature/SamplingPoint/''' + wdtf_id + '''/''' + aws_id + '''/1"/>
            <wdtf:metadata>
                <wdtf:TimeSeriesObservationMetadata>
                    <wdtf:relatedTransaction xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/definition/sync/bom/DataDefined"/>
                    <wdtf:siteId>''' + aws_id + '''</wdtf:siteId>
                    <wdtf:relativeLocationId>1</wdtf:relativeLocationId>
                    <wdtf:relativeSensorId>''' + aws_id + '''_aws</wdtf:relativeSensorId>
                    <wdtf:status>validated</wdtf:status>
                </wdtf:TimeSeriesObservationMetadata>
            </wdtf:metadata>
            <wdtf:result>
                <wdtf:TimeSeries>
                    <wdtf:defaultInterpolationType>''' + interpol + '''</wdtf:defaultInterpolationType>
                    <wdtf:defaultUnitsOfMeasure>''' + units + '''</wdtf:defaultUnitsOfMeasure>
                    <wdtf:defaultQuality>quality-A</wdtf:defaultQuality>'''

    try:
        if conn is None:
            conn = db_connect()

        #cursor = conn.cursor (MySQLdb.cursors.DictCursor) -- for named columns
        cursor = conn.cursor()
        cursor.connection.autocommit(True)
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
                wdtf_obsMember += "                     <wdtf:timeValuePair time=\"%s\">%s</wdtf:timeValuePair>\n" % (row[0], row[1])

        cursor.close()
        conn.commit()
        conn.close()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        logging.error("failed to connect to DB in get_districts()\n" + str(e))
        sys.exit(1)
    finally:
        cursor.close()
        conn.commit()
        #conn.close()

    wdtf_obsMember += '''
                </wdtf:TimeSeries>
            </wdtf:result>
        </wdtf:TimeSeriesObservation>
    </wdtf:observationMember>'''

    return wdtf_obsMember


def get_hydrocollection(conn, aws_id, wdtf_id, in_date):
    """
    Get the full WDTF for a particular station. Calls get_observation_member(aws_id, member, wdtf_id)

    :param aws_id: string
    :param wdtf_id: string
    :param in_date: date
    :return: XML - HydroCollection
    """
    logging.debug("get_hydrocollection " + aws_id + " " + wdtf_id)

    t = datetime.datetime.utcnow()

    hydrocollection = '''<?xml version="1.0"?>
    <wdtf:HydroCollection
        xmlns:sa="http://www.opengis.net/sampling/1.0/sf1"
        xmlns:om="http://www.opengis.net/om/1.0/sf1"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xmlns:gml="http://www.opengis.net/gml"
        xmlns:wdtf="http://www.bom.gov.au/std/water/xml/wdtf/1.0"
        xmlns:ahgf="http://www.bom.gov.au/std/water/xml/ahgf/0.2"
        xsi:schemaLocation="http://www.opengis.net/sampling/1.0/sf1 ../sampling/sampling.xsd
        http://www.bom.gov.au/std/water/xml/wdtf/1.0 ../wdtf/water.xsd
        http://www.bom.gov.au/std/water/xml/ahgf/0.2 ../ahgf/waterFeatures.xsd"
        gml:id="timeseries_m">
    <gml:description>This document encodes timeseries data from the SANRM's Automatic Weatherstation Network.</gml:description>
    <gml:name codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/feature/HydroCollection/''' + wdtf_id + '''/">wdtf_sanrm</gml:name>

    <wdtf:metadata>
        <wdtf:DocumentInfo>
            <wdtf:version>wdtf-package-v1.0</wdtf:version>
            <wdtf:dataOwner codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/party/person/bom/">''' + wdtf_id + '''</wdtf:dataOwner>
            <wdtf:dataProvider codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/party/person/bom/">''' + wdtf_id + '''</wdtf:dataProvider>
            <wdtf:generationDate>''' + t.strftime("%Y-%m-%dT%H:%M:%S") + '''+09:30</wdtf:generationDate>
            <wdtf:generationSystem>KurrawongIC_WDTF</wdtf:generationSystem>
        </wdtf:DocumentInfo>
    </wdtf:metadata>
    '''
    # separate format for rainguages
    if aws_id[:4] == 'TBRG':
        #used to use daily total for TBRG, now using 15min data
        #hydrocollection += get_observation_member(aws_id, 'rain_total', wdtf_id, in_date)
        hydrocollection += get_observation_member(conn, aws_id, 'rain', wdtf_id, in_date)
    else:  # aws
        logging.debug("get_hydrocollection before get_observation_member for rain")
        hydrocollection += get_observation_member(conn, aws_id, 'rain', wdtf_id, in_date)
        logging.debug("get_hydrocollection after get_observation_member for rain")
        hydrocollection += get_observation_member(conn, aws_id, 'Wavg', wdtf_id, in_date)
        hydrocollection += get_observation_member(conn, aws_id, 'gsr', wdtf_id, in_date)
        hydrocollection += get_observation_member(conn, aws_id, 'airT', wdtf_id, in_date)
        hydrocollection += get_observation_member(conn, aws_id, 'rh', wdtf_id, in_date)
        #hydrocollection += get_observation_member(conn, aws_id, 'dp', wdtf_id, in_date)

    hydrocollection += "</wdtf:HydroCollection>"

    return hydrocollection


def make_wdtf_zip_file(conn, owner, in_date):
    """
    Make a WDTF XML file for each station for a particular owner with status 'on' and returns them as zip file
    calls get_hydrocollection(owner, wdtf_id)

    :param owner: string
    :param in_date: date
    :return: a zip file of XML docs named according to the BoM's naming convention
    """
    logging.debug("make_wdtf_zip_file " + owner)

    sql = "SELECT aws_id, wdtf_id FROM tbl_stations INNER JOIN tbl_owners ON tbl_stations.owner = tbl_owners.owner_id WHERE owner = '" + owner + "' AND status = 'on';"

    t = datetime.datetime.now()

    if conn is None:
        conn = db_connect()

    cursor = conn.cursor()
    cursor.connection.autocommit(True)
    cursor.execute(sql)
    rows = cursor.fetchall()
    wdtf_file_names = []
    wdtf_file_data = []
    wdtf_id = ''
    #make double array of file names & file data
    for row in rows:
        wdtf_id = row[1]
        #wdtf.w00208.20111225H0000.RMPW12-ctsd.xml
        wdtf_file_names.append("wdtf." + wdtf_id + "." + t.strftime("%Y%m%d%H0000") + "." + row[0] + "-ctsd.xml")
        wdtf_file_data.append(conn, get_hydrocollection(row[0], row[1], in_date))

    cursor.close()
    conn.close()

    #make the zipfile from file names & file data
    #w00208.20111225093000.zip
    zfilename = wdtf_id + "." + in_date.strftime("%Y%m%d") + "093000.zip"#fixed at 9:30am
    logging.debug("zip file name " + zfilename)
    zout = zipfile.ZipFile(os.getcwd() + "/" + zfilename, "w", zipfile.ZIP_DEFLATED)

    for i in range(len(wdtf_file_names)):
        zout.writestr(wdtf_file_names[i],wdtf_file_data[i])
    zout.close()

    #we have created a zipfile on disk so return the file name
    return zfilename


def make_wdtf_zip_file_for_station_and_date(conn, owner, aws_id, in_date):
    """
    Make a WDTF XML file for each station for a particular owner with status 'on' and returns them as zip file
    Calls get_hydrocollection(owner, wdtf_id)

    :param owner:
    :param aws_id:
    :param in_date:
    :return:
    """
    #logging.debug("make_wdtf_zip_file_for_station_and_date " + owner + " " + aws_id + " " + in_date.strftime("%Y%m%d%H0000"))
    logging.debug("make_wdtf_zip_file_for_station_and_date " + owner + " " + aws_id)
    #get the WDTF ID for this owner
    sql = "SELECT wdtf_id FROM tbl_owners WHERE owner_id = '" + owner + "';"

    if conn is None:
        conn = db_connect()

    #t = datetime.datetime.now()
    t = in_date

    cursor = conn.cursor()
    cursor.connection.autocommit(True)
    cursor.execute(sql)
    rows = cursor.fetchall()
    wdtf_file_names = []
    wdtf_file_data = []
    wdtf_id = ''
    #make double array of file names & file data
    for row in rows:
        wdtf_id = row[0]
        print wdtf_id
        #wdtf.w00208.20111225H0000.RMPW12-ctsd.xml
        wdtf_file_names.append("wdtf." + wdtf_id + "." + t.strftime("%Y%m%d%H0000") + "." + aws_id + "-ctsd.xml")
        #get_hydrocollection(aws_id, wdtf_id)
        wdtf_file_data.append(get_hydrocollection(conn, aws_id, row[0], in_date))

    cursor.close()
    conn.close()

    #make the zipfile from file names & file data
    #w00208.20111225093000.zip
    zfilename = wdtf_id + "." + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".zip"#now
    zout = zipfile.ZipFile(settings.APPLICATION_DIR + zfilename, "w", zipfile.ZIP_DEFLATED)

    for i in range(len(wdtf_file_names)):
        zout.writestr(wdtf_file_names[i], wdtf_file_data[i])
    zout.close()

    #we have created a zipfile on disk so return the file name
    return zfilename


def send_wdtf_zipfile(conn, owner, in_date):
    """
    Send the zipped WDTF file collection to the BoM by FTP using owner's FTP WDTF details
    Calls make_wdtf_zip_file(owner)

    :param owner: string
    :param in_date: date string
    :return: True/False if successful
    """
    logging.debug("send_wdtf_zipfile " + owner)
    #get the owner's WDTF details
    sql = "SELECT wdtf_server,wdtf_id,wdtf_password FROM tbl_owners WHERE owner_id = '" + owner + "';"

    if conn is None:
        conn = db_connect()

    cursor = conn.cursor()
    cursor.connection.autocommit(True)
    cursor.execute (sql)
    rows = cursor.fetchall()

    svr = ''
    usr = ''
    pwd = ''

    for row in rows:
        svr = row[0]
        usr = row[1]
        pwd = row[2]

    cursor.close()

    #dummy FPT details for testing
    '''
    svr = 'kurrawong.net'
    usr = 'wdtf'
    pwd = 'wdtfwdtf'
    '''

    #get the zip file
    zipfile = make_wdtf_zip_file(owner, in_date)

    #send the zip file
    try:
        ftp = FTP(svr)
        ftp.set_debuglevel(0)
        ftp.login(usr, pwd)
        ftp.cwd('/register/' + usr + '/incoming/data')
        ftp.storbinary("STOR " + zipfile,open(zipfile,'rb') )
        ftp.quit()
    except error_reply:
        t = datetime.datetime.now()
        f = open("scheduled_export.log",'w')
        f.write(t.strftime("%Y-%m-%d %H%i%s") + " ERROR for send_wdtf_zipfile: " + str(error_reply))
        f.close()

    #delete zip file on disc
    os.remove(zipfile)


if __name__ == "__main__":
    logging.basicConfig(
        filename=settings.LOG_FILE,
        level=settings.LOG_LEVEL,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(asctime)s %(levelname)s %(message)s')
    #expected call: 
    #param: stations' owner
    #methods:
    #send_wdtf_zipfile(owner)
    #--> make_wdtf_zipfile()
    #----> get_hydrocollection()
    #------> get_observation_member()
    #return: zipfile

    conn = db_connect()

    ## manual, make zip file for one station
    # python /home/ftp/wdtf/generator_wdtf.py station SAMDB RMPW12 2013-08-27
    if sys.argv[1] == "station":
        owner = sys.argv[2]
        aws_id = sys.argv[3]
        date_string = sys.argv[4]
        in_date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
        zf = make_wdtf_zip_file_for_station_and_date(conn, owner, aws_id, in_date)
        logging.info("manual station " + owner + " : " + aws_id + " : " + date_string + " : file " + zf)
    elif sys.argv[1] == "owner":
        # python /home/ftp/wdtf/generator_wdtf.py owner SAMDB 2013-08-27
        owner = sys.argv[2]
        date_string = sys.argv[3]
        in_date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
        zf = make_wdtf_zip_file(conn, owner, in_date)
        logging.info("manual owner " + owner + " : " + date_string + " : file " + zf)
    else:          
        #automated, daily run from crontab
        # python /home/ftp/wdtf/generator_wdtf.py SAMDB
        # python /home/ftp/wdtf/generator_wdtf.py SENRM
        # python /home/ftp/wdtf/generator_wdtf.py LMW
        # python /home/ftp/wdtf/generator_wdtf.py AWNRM
        owner = sys.argv[1]
        in_date = (date.today() - timedelta(1))
        logging.info("cron " + owner + " : " + in_date.strftime('%Y-%m-%d'))
        send_wdtf_zipfile(owner, in_date)
