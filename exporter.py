import sys
import datetime
import logging
import settings
import generator_wdtf


if __name__ == "__main__":
    logging.basicConfig(
        filename=settings.LOG_FILE,
        level=settings.LOG_LEVEL,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(asctime)s %(levelname)s %(message)s')

    conn = generator_wdtf.db_connect()

    '''
    owner = sys.argv[2]
    aws_id = sys.argv[3]
    date_string = sys.argv[4]
    in_date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    zf = generator_wdtf.make_wdtf_zip_file_for_station_and_date(conn, owner, aws_id, in_date)
    logging.info("manual station " + owner + " : " + aws_id + " : " + date_string + " : file " + zf)
    '''

    import generator_dfw_csv
    print generator_dfw_csv.make_csv_file(conn, 'SAMDB')