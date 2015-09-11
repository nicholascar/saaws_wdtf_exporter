import generator_wdtf
import logging
import datetime
import sys
import cStringIO
from ftplib import FTP, error_reply


def get_minutes_data(conn, aws_id):
    """
    Get yesterday's 15min data for a station for CSV maker

    :param conn: a live DB connection
    :param aws_id: string
    :return: CSV data
    """
    """
        colums:
        DfW ID
        Date
        Time
        Ave AirTemp (AWS) (degC)
        Ave AppTemp (degC)
        Ave DewPoint (degC)
        Ave Humidity (AWS) (%)
        Ave DeltaT (degC)
        Ave Soil Temperature (degC)
        Ave GSR (W/m^2)
        Min WndSpd (m/s)
        Ave WndSpd (m/s)
        Max WndSpd (m/s)
        Ave WndDir (deg)
        Total Rain (mm)
        Ave LeafWet (% Wet)
        Ave AirTemp (Canopy) (degC)
        Ave Humidity (Canopy) (%)
    """

    sql = """
        SELECT COALESCE(tbl_stations.dfw_id, tbl_stations.aws_id), tbl_data_minutes.*
        FROM tbl_data_minutes
        INNER JOIN tbl_stations
        ON tbl_data_minutes.aws_id = tbl_stations.aws_id
        WHERE
            tbl_data_minutes.aws_id = '""" + aws_id + """'
            AND DATE(stamp) = CURDATE() - INTERVAL 1 DAY GROUP BY stamp
        ORDER BY stamp;"""

    if conn is None:
        conn = generator_wdtf.db_connect()

    #cursor = conn.cursor (MySQLdb.cursors.DictCursor) -- for named columns
    cursor = conn.cursor()
    cursor.connection.autocommit(True)
    cursor.execute(sql)
    rows = cursor.fetchall()

    csv_string = ""

    for row in rows:
        date_time = str(row[3])
        row2 = []

        #turn None into 0
        for col in row:
            if col == None:
                row2.append(",")
            else:
                row2.append(str(col)+",")

        row_string = str(row2[0])+date_time[0:10]+","+date_time[11:]+","+str(row2[4])+str(row2[5])+str(row2[6])+str(row2[7])+str(row2[8])+str(row2[9])+str(row2[10])+str(row2[11])+str(row2[12])+str(row2[13])+str(row2[14])+str(row2[15])+str(row2[16])+str(row2[17])+str(row2[18])
        csv_string += row_string.strip(',') + "\r\n"

    cursor.close()
    conn.commit()
    conn.close()

    return csv_string


def make_csv_file(conn, owner):
    """
    Writes the 15min data for each station with status 'on' for DfW to a single CSV file (SAMDBNRM_YYYYMMDD.CSV)
    Calls get_15min_data()

    :param conn: a live DB connection
    :param owner: string
    :return: a CSV file string
    """
    sql = "SELECT aws_id FROM tbl_stations WHERE owner = '" + owner + "' AND status = 'on';"

    if conn is None:
        conn = generator_wdtf.db_connect()

    cursor = conn.cursor()
    cursor.connection.autocommit(True)
    cursor.execute(sql)
    rows = cursor.fetchall()

    single_csv_file = "DfW ID,Date,Time,Ave AirTemp (AWS) (degC),Ave AppTemp (degC),Ave DewPoint (degC),Ave Humidity (AWS) (%),Ave DeltaT (degC),Ave Soil Temperature (degC),Ave GSR (W/m^2),Min WndSpd (m/s),Ave WndSpd (m/s),Max WndSpd (m/s),Ave WndDir (deg),Total Rain (mm),Ave LeafWet (% Wet),Ave AirTemp (Canopy) (degC),Ave Humidity (Canopy) (%)\r\n"

    for row in rows:
        single_csv_file += get_minutes_data(conn, row[0])

    cursor.close()
    conn.close()

    return single_csv_file


def send_csv_to_dfw(conn, owner):
    """
    Sends the CSV file via FTP somewhere

    :param conn: live db connection
    :param owner: string
    :return: True if ok
    """
    svr = 'e-nrims.dwlbc.sa.gov.au'
    usr = 'MEATelem'
    pwd = 'meatelem01'

    t = datetime.datetime.now()
    try:
        ftp = FTP(svr)
        ftp.set_debuglevel(0)
        ftp.login(usr, pwd)
        single_csv_file = cStringIO.StringIO(make_csv_file(conn, owner))
        ftp.storbinary("STOR " + owner + "_" + t.strftime("%Y%m%d") + ".csv",single_csv_file)
        single_csv_file.close()
        ftp.quit()

        return True
    except Exception, e:
        logging.error("failed in send_csv_to_dfw()\n" + str(e))

        return [False, error_reply.message]

#expected call: 

#param: stations' owner

#send_csv_to_dfw(owner)
#--> make_csv_file()
#----> get_15min_data()

