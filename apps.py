#!/usr/bin/env python
# -*- coding: utf-8 -*-

#import pymysql as db
import time
import Tkinter
import tkMessageBox
import subprocess
import shlex
import logging
import psycopg2 as db
from datetime import datetime
import os
import sys
import threading

while True:
	host = "localhost"
	user = "postgres"
	password = "root"
	dbname = "stta"

	conn = db.connect(host=host, user=user, password=password, database=dbname)
	cur = conn.cursor()


	sum1 = []
	sizeA = []
	use_db = db.connect(host=host, database=dbname, user=user, password=password)
	cur_db = use_db.cursor()
	show_tables_db = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
	cur_db.execute(show_tables_db)
	ex = cur_db.fetchall()
	for i in ex:
		tabel_db = i[0]
		checksum_db = "select md5(string_agg("+tabel_db+"::text, '' ORDER BY "+tabel_db+")) from "+tabel_db
		cur_db.execute(checksum_db)
		cek_db = cur_db.fetchall()
		for element in cek_db:
			hash_db = element[0]
			sum1.append(hash_db)
		size_db = "SELECT pg_size_pretty(pg_total_relation_size('"+tabel_db+"'));"
		cur_db.execute(size_db)
		hasil_db = cur_db.fetchall()
		for size in hasil_db:
			sizeA.append(size)
	select_tabel_db = 'select * from d_absensi_mhs'
	cur_db.execute(select_tabel_db)
	select_a = cur_db.fetchall()

	sum2 = []
	sizeB = []
	use_db_test = db.connect(host=host, database="dbstta", user=user, password=password)
	cur_test = use_db_test.cursor()
	show_db_test = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
	cur_test.execute(show_db_test)
	xy = cur_test.fetchall()
	for j in xy:
		tabel_db_test = j[0]
		checksum_db_test = "select md5(string_agg("+tabel_db_test+"::text, '' ORDER BY "+tabel_db_test+")) from "+tabel_db_test
		cur_test.execute(checksum_db_test)
		cek_db_test = cur_test.fetchall()
		for element in cek_db_test:
			hash_db_test = element[0]
			sum2.append(hash_db_test)

		size_db_test = "SELECT pg_size_pretty(pg_total_relation_size('d_absensi_mhs'));"
		cur_test.execute(size_db_test)
		hasil_db_test = cur_test.fetchall()
		for size in hasil_db_test:
			sizeB.append(size)	
	select_tabel_db_test = 'select * from d_absensi_mhs'
	cur_test.execute(select_tabel_db_test)
	select_b = cur_test.fetchall()
	
#	def pesanDialog():
#		pesan="ada percobaan untuk melakukan peruban secara tidak wajar\n\
#sistem akan mencoba mengembalikan data yang berubah secara cepat\n\n\
#mohon tunggu sebentar"
#		os.system('curl -s -d "chat_id=258037352&disable_web_page_preview=1&text={}"'.format(pesan)+' https://api.telegram.org/bot667835835:AAF6fadMXy9Q-pGd-YGJSCOhzkrYeqXxTIA/sendMessage >/dev/null')
		
	a=datetime.now()
	jam=a.hour
	menit=a.minute
	detik=a.second
	awal = time.time()
	if (set(sum1)==set(sum2)):
		print "OK"

	else:
		#print "berubah"
#		threading.Thread(target=pesanDialog()).start()
		penyembuhan = "UPDATE d_absensi_mhs data1 SET status_hadir=data2.status_hadir from dblink('dbname=dbstta', 'SELECT * FROM d_absensi_mhs') AS data2(kd_krs CHARACTER VARYING, kd_kelas CHARACTER VARYING, kuliah_ke NUMERIC, nim CHARACTER VARYING, tgl_kul CHARACTER VARYING, status_hadir CHARACTER VARYING, catatan CHARACTER VARYING, keterangan CHARACTER VARYING) WHERE data1.kd_krs=data2.kd_krs and data1.kd_kelas=data2.kd_kelas and data1.kuliah_ke=data2.kuliah_ke AND data1.status_hadir IS DISTINCT FROM data2.status_hadir"
		cur.execute(penyembuhan)
		conn.commit()
		akhir = time.time()
		waktu = akhir - awal
		b=datetime.now()
		jam1=b.hour
		menit1=b.minute
		detik1=b.second
		print "waktu proses : %s \n" %(waktu)
		ukuran = set(sizeB)
		ukuran2 = set(sizeA)
		hash1 = set(sum1)
		hash2 = set(sum2)
		log ="waktu proses : {}\n\
ukuran sebelum perubahan : {}\n\
ukuran saat perubahan : {}\n\
ukuran sesudah perubahan : {}\n\
hash sebelum beprubahan : {}\n\
hash saat perubahan : {}\n\
hash sesudah perubahan : {}\n\
waktu awal proses : {}:{}:{}\n\
waktu akhir proses : {}:{}:{}\n\
---\n".format(waktu, ukuran, ukuran2, ukuran, hash2, hash1, hash2, jam, menit, detik, jam1, menit1, detik1)
		logging.basicConfig(filename='/var/log/stta.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
		logging.warning(log)
		def pesanDialogsukses():
			nimar = []
			hasil1 = set (select_a)
			hasil2 = set (select_b)
			hasilproeses = hasil1.difference(hasil2)
			for i in hasilproeses:
				nim = i[3]
				nimar.append(nim)
			nimrubah = set (nimar)
			pesanhasil="ada percobaan untuk melakukan perubahan secara tidak wajar\n\
sistem akan mengembalikan data yang berubah secara cepat\n\
data yang telah diubah nim = {},\n\n\n\
sistem keamanan database\n\
---arifin---".format(nimrubah)
			os.system('curl -s -d "chat_id=258037352&disable_web_page_preview=1&text={}"'.format(pesanhasil)+' https://api.telegram.org/bot667835835:AAF6fadMXy9Q-pGd-YGJSCOhzkrYeqXxTIA/sendMessage >/dev/null')
		threading.Thread(target=pesanDialogsukses()).start()
	time.sleep(5)