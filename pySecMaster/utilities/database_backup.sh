#!/usr/bin/env bash

# NOTE: To automate this with cron, you must set database passwords in .pgpass
#    sudo nano ~/.pgpass
#        localhost:5432:postgres:postgres:mypass
#        localhost:5432:mydbname:postgres:mypass
#    chmod 600 ~/.pgpass

# On linux, you must enable this script to be executable
#    chmod u+x ~/Programming/Code/database_backup.sh

cur_time=$(date "+%Y%m%dT%H%M%S")
cur_database="pysecmaster"

#database_dump_location="O:/Programming/Databases/pySecMaster/postgres_dumps"
database_dump_location="/mnt/backups/Programming/Databases/pySecMaster/postgres_dumps"

echo "Starting the Postgres database dump for ${cur_database}"


# Local database dumps:
#    Dump the database that is on the local machine, saving it to a local folder

# Windows Backup
#"C:\Program Files\PostgreSQL\9.5\bin\pg_dump" -Fc -U postgres -h localhost $cur_database > ${database_dump_location}/${cur_database}_${cur_time}.dump
#"C:\Program Files\PostgreSQL\9.5\bin\pg_dumpall" -g -U postgres -h localhost > ${database_dump_location}/postgres_misc_${cur_time}.dump

# Linux Backup
pg_dump -Fc -U postgres -h localhost $cur_database > ${database_dump_location}/${cur_database}_${cur_time}.dump
pg_dumpall -g -U postgres -h localhost > ${database_dump_location}/postgres_misc_${cur_time}.dump


# Remote database dumps:
#    SSH into the database server to run pg_dump, saving it to the local machine

#remote_user="xxxxx"
#remote_ip="xxx.xxx.xxx.xxx"

#ssh -o "Compression=no" ${remote_user}@${remote_ip} "pg_dump -Fc -U postgres -h localhost ${cur_database}" > ${database_dump_location}/${cur_database}_${cur_time}.dump
#ssh -o "Compression=no" ${remote_user}@${remote_ip} "pg_dumpall -g -U postgres -h localhost" > ${database_dump_location}/postgres_misc_${cur_time}.dump


# Find and delete ALL files that are older than days_to_retain period

days_to_retain=60

find ${database_dump_location} -type f -mtime +${days_to_retain} -delete
