from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib
import time
import logging
import traceback

import psycopg2
import psycopg2.extras
import pymysql as MySQLdb


def get_logger(name, file_path='./deposit_table_migration.log'):
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)

	# create a file handler
	handler = logging.FileHandler(file_path)
	handler.setLevel(logging.DEBUG)

	# create a logging format
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)

	# add the handlers to the logger
	logger.addHandler(handler)
	return logger


logger = get_logger("[DEPOSIT TABLE MIGRATION]")

# Credentials for Collection Service
host1 = "localhost"
user1 = "cf_collections"
password1 = "hDKtcVazrH56wudfdEFa"
database1 = "collections"
port1 = 12350

# Credentials for Web2py Service
host2 = "localhost"
user2 = "root"
password2 = "C4p1t4lfl04TDB"
database2 = "CF"
port2 = 12360

BATCH_SIZE = 10000
india_timezone = ZoneInfo("Asia/Kolkata")


def db1_conn():
	db1 = psycopg2.connect(host=host1, user=user1, password=password1, database=database1, port=port1)
	cursor1 = db1.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	return db1, cursor1


def db2_conn():
	db2 = MySQLdb.connect(host=host2, user=user2, password=password2, database=database2, port=port2)
	cursor2 = db2.cursor(MySQLdb.cursors.DictCursor)
	return db2, cursor2


def mig():
	logger_print = "[deposit table]"
	select_query = "select * from deposit"
	db2, cursor2 = db2_conn()
	try:
		cursor2.execute(select_query)
	except Exception:
		exception = traceback.format_exc().replace("\n", "\t")
		logger.critical("{} Error Occurred while fetching data from query: {}, Error: {}".format(
			logger_print, select_query, exception))

	results = cursor2.fetchmany(size=BATCH_SIZE)
	while results is not None and len(results) > 0:
		last_id = results[-1]["id"]
		insert_data = [{
			"deposit_id": result["deposit_id"],
			"status": int(result["status"]) if result["status"] is not None else None,
			"status_description": result["status_desc"],
			"is_cheque_different": result["is_cheque_different"],
			"provided_cheque_number": result["provided_cheque_no"],
			"transaction_number": result["txn_number"],
			"created_at": result["ts_added"].replace(tzinfo=india_timezone),
			"updated_at": result["ts_added"].replace(tzinfo=india_timezone),
		} for result in results]

		try:
			db1, cursor1 = db1_conn()
			psycopg2.extras.execute_batch(cursor1, """
			INSERT INTO deposit 
			(deposit_id, status, status_description, is_cheque_different, provided_cheque_number, transaction_number,
			created_at, updated_at) 
			VALUES (
			%(deposit_id)s, %(status)s, %(status_description)s, %(is_cheque_different)s,
			%(provided_cheque_number)s, %(transaction_number)s, %(created_at)s, %(updated_at)s
			);
			""", insert_data)
			db1.commit()

			# Checksum in batches
			checksum(cursor1, insert_data, last_id, logger_print, results)

			cursor1.close()
			db1.close()
		except Exception:
			exception = traceback.format_exc().replace("\n", "\t")
			logger.critical("{} Error Occurred while fetching inserting data. Error: {}".format(
				logger_print, exception))

		logger.info("{} Data inserted till id: {}".format(logger_print, last_id))
		results = cursor2.fetchmany(size=BATCH_SIZE)

	return


def checksum(cursor1, insert_data, last_id, logger_print, results):
	select_query2 = "select deposit_id, status, status_description, is_cheque_different, " \
					"provided_cheque_number, transaction_number, created_at, updated_at from deposit " \
					"order by id desc limit %s" % BATCH_SIZE
	cursor1.execute(select_query2)
	new_results = cursor1.fetchall()
	new_results.reverse()
	final_data = [{
		"deposit_id": result["deposit_id"],
		"status": result["status"],
		"status_description": result["status_description"],
		"is_cheque_different": result["is_cheque_different"],
		"provided_cheque_number": result["provided_cheque_number"],
		"transaction_number": result["transaction_number"],
		"created_at": result["created_at"].replace(tzinfo=india_timezone),
		"updated_at": result["updated_at"].replace(tzinfo=india_timezone),
	} for result in new_results]

	postgres_md5_digest = hashlib.md5(str(final_data).encode("utf-8")).hexdigest()
	mysql_md5_digest = hashlib.md5(str(insert_data).encode("utf-8")).hexdigest()
	if postgres_md5_digest != mysql_md5_digest:
		logger.critical("{} Data didn't match from id: to id: ".format(logger_print, results[0]["id"], last_id))


start_time = time.time()
print("start_time :%s" % start_time)
mig()
print("end_time :%s" % (time.time()))
print("Total time taken :%s" % (time.time() - start_time))
logger.info("Total time taken :%s" % (time.time() - start_time))