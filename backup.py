#!/usr/bin/python3
#
# MIT License
#
# Copyright (c) 2019 Adam Dodd
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from datetime import datetime, date, timedelta
from enum import IntEnum
import hashlib
import json
import os
import os.path
import sqlite3
import sys
import tarfile
import time


sqlite3_create_tables = """
    DROP TABLE IF EXISTS `meta`;
    CREATE TABLE IF NOT EXISTS `meta` (
        `name`                          TEXT NOT NULL,
        `out_dir`                       TEXT NOT NULL,
        `hash_excluded_files_max_size`  INTEGER NOT NULL,
        `start_time`                    INTEGER NOT NULL,
        `end_time`                      INTEGER NOT NULL,
        `files_allowed`                 INTEGER NOT NULL,
        `files_allowed_bytes`           INTEGER NOT NULL,
        `files_excluded`                INTEGER NOT NULL,
        `files_excluded_bytes`          INTEGER NOT NULL,
        `files_errored`                 INTEGER NOT NULL,
        `files_hashed`                  INTEGER NOT NULL,
        `sql_file_inserts`              INTEGER NOT NULL,
        `sql_file_insert_time`          REAL NOT NULL,
        `tar_add_time`                  REAL NOT NULL,
        `hash_time`                     REAL NOT NULL
    );
    DROP TABLE IF EXISTS `in_dir`;
    CREATE TABLE IF NOT EXISTS `in_dir` (
        `dir` TEXT NOT NULL UNIQUE,
        PRIMARY KEY(`dir`)
    );
    DROP TABLE IF EXISTS `file`;
    CREATE TABLE IF NOT EXISTS `file` (
        `id`            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        `path`          TEXT NOT NULL UNIQUE,
        `size`          INTEGER,
        `date_accessed` INTEGER,
        `date_modified` INTEGER,
        `hash`          TEXT,
        `error_code`    INTEGER NOT NULL
    );
    DROP TABLE IF EXISTS `error_codes`;
    CREATE TABLE IF NOT EXISTS `error_codes` (
        `error_code`   INTEGER NOT NULL UNIQUE,
        `error_string` TEXT NOT NULL,
        PRIMARY KEY(`error_code`)
    );
    DROP TABLE IF EXISTS `excluded_exts`;
    CREATE TABLE IF NOT EXISTS `excluded_exts` (
        `ext` TEXT NOT NULL UNIQUE,
        PRIMARY KEY(`ext`)
    );
    CREATE UNIQUE INDEX `ind_file_path` ON `file` (
        `path` ASC
    );
    CREATE UNIQUE INDEX `ind_error_codes_error_code` ON `error_codes` (
        `error_code` ASC
    );
    CREATE INDEX `ind_file_hash` ON `file` (
        `hash` ASC
    );
    PRAGMA synchronous = OFF;
"""

sqlite3_insert_in_dir = """
    INSERT INTO `in_dir` (
        `dir`
    ) VALUES (?)
"""

sqlite3_insert_meta = """
    INSERT INTO `meta` (
        `name`, `out_dir`, `hash_excluded_files_max_size`,
        `start_time`, `end_time`, `files_allowed`, `files_allowed_bytes`,
        `files_excluded`, `files_excluded_bytes`, `files_errored`,
        `files_hashed`, `sql_file_inserts`, `sql_file_insert_time`,
        `tar_add_time`, `hash_time`
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

sqlite3_insert_file = """
    INSERT INTO `file` (
        `path`, `size`, `date_accessed`, `date_modified`, `hash`, `error_code`
    ) VALUES (?, ?, ?, ?, ?, ?)
"""

sqlite3_insert_error_code = """
    INSERT INTO `error_codes` (
        `error_code`, `error_string`
    ) VALUES (?, ?)
"""

sqlite3_insert_excluded_ext = """
    INSERT INTO `excluded_exts` (
        `ext`
    ) VALUES (?)
"""


class ErrorCodes(IntEnum):
    SUCCESS = 0
    EXCL_EXT = 1
    IO_ERROR = 2
    TAR_ERROR = 3


error_codes = {
    ErrorCodes.SUCCESS:     "Success",
    ErrorCodes.EXCL_EXT:    "Excluded extension",
    ErrorCodes.IO_ERROR:    "IOError",
    ErrorCodes.TAR_ERROR:   "TarError"
}


def hash_file(file_path):
    h = hashlib.sha256()

    f = open(file_path, "rb")
    h.update(f.read())
    f.close()

    return h.hexdigest()


def datetime_to_unix(dt):
    return int(time.mktime(dt.timetuple()))


class DbAgent:
    def __init__(self, backup_name, out_dir, start_time):
        self.backup_name = backup_name
        self.out_dir = out_dir
        self.start_time = start_time

        sqlite3.register_adapter(datetime, datetime_to_unix)

        self.db_name = self.backup_name + "-" + self.start_time + ".sqlite3"
        self.full_path = os.path.join(self.out_dir, self.db_name)

        print("Creating database '{}'".format(self.full_path))
        self.conn = sqlite3.connect(self.full_path)
        self.conn.isolation_level = None

        self.cur = self.conn.cursor()
        self.cur.executescript(sqlite3_create_tables)


    #def __del__(self):
    #    self.close()


    def get_cursor(self):
        return self.cur


    def close(self):
        self.cur.close()
        self.conn.close()


def validate_dir(d, perms):
    if not os.path.isdir(d):
        print("Not a directory: '{}'".format(d))
        sys.exit(1)
    elif not os.access(d, perms):
        print("Bad permissions for directory: '{}'".format(d))
        sys.exit(1)
    else:
        print("Validated directory '{}'".format(d))


def validate_dirs(dirs):
    for d in dirs:
        validate_dir(d, os.R_OK | os.X_OK)


def insert_error_codes(db):
    cur = db.get_cursor()
    data = [(int(key), error_codes[key]) for key in error_codes]
    cur.executemany(sqlite3_insert_error_code, data)


def insert_meta(
        db, backup_name, in_dirs, out_dir, excluded_exts, hefms,
        start_time, end_time, files_allowed, files_allowed_bytes,
        files_excluded, files_excluded_bytes, files_errored, files_hashed,
        sql_file_inserts, sql_file_insert_time, tar_add_time, hash_time):
    cur = db.get_cursor()

    in_dirs_tupled = [(in_dir,) for in_dir in in_dirs]
    cur.executemany(sqlite3_insert_in_dir, in_dirs_tupled)

    excluded_exts_tupled = [(ext,) for ext in excluded_exts]
    cur.executemany(sqlite3_insert_excluded_ext, excluded_exts_tupled)

    cur.execute(
            sqlite3_insert_meta,
            (backup_name, out_dir, hefms, start_time, end_time, files_allowed,
                files_allowed_bytes, files_excluded, files_excluded_bytes,
                files_errored, files_hashed, sql_file_inserts,
                sql_file_insert_time, tar_add_time, hash_time))


def create_tar(backup_name, out_dir, start_time):
    tar_name = backup_name + "-" + start_time + ".tar.bz2"
    full_path = os.path.join(out_dir, tar_name)

    print("Creating archive '{}'".format(full_path))
    tar = tarfile.open(full_path, "w:bz2")

    return tar


def do_backup(db, tar, backup_name, in_dirs, out_dir, excluded_exts, hefms):
    files_allowed = 0
    files_allowed_bytes = 0
    files_excluded = 0
    files_excluded_bytes = 0
    files_errored = 0
    files_hashed = 0
    sql_file_inserts = 0
    sql_file_insert_time = 0.0
    tar_add_time = 0.0
    hash_time = 0.0

    cur = db.get_cursor()

    for in_dir in in_dirs:
        for root, dirs, files in os.walk(in_dir):
            for name in files:
                file_status = ErrorCodes.SUCCESS
                file_stat = None
                can_hash_file = False
                hash_code = None
                full_path = os.path.join(root, name)
                file_ext = os.path.splitext(name)[1][1:]

                if file_ext in excluded_exts:
                    file_status = ErrorCodes.EXCL_EXT

                try:
                    file_stat = os.stat(full_path)

                    if (file_status == ErrorCodes.SUCCESS) or \
                        (file_stat.st_size <= hefms):
                        can_hash_file = True
                except IOError as e:
                    file_status = ErrorCodes.IO_ERROR
                    print(("IOError\n" +
                            "> file     : '{}'\n" +
                            "> errno    : {}\n" +
                            "> args     : {}\n" +
                            "> message  : {}\n" +
                            "> strerror : {}").format(
                                full_path, e.errno, e.args,
                                e.message, e.strerror))

                if can_hash_file:
                    perf_counter_before = time.perf_counter()
                    hash_code = hash_file(full_path)
                    perf_counter_after = time.perf_counter()

                    files_hashed += 1
                    hash_time += \
                            perf_counter_after - perf_counter_before

                if file_status == ErrorCodes.SUCCESS:
                    try:
                        perf_counter_before = time.perf_counter()
                        tar.add(full_path)
                        perf_counter_after = time.perf_counter()

                        files_allowed += 1
                        files_allowed_bytes += file_stat.st_size
                        tar_add_time += \
                                perf_counter_after - perf_counter_before
                    except tarfile.TarError as e:
                        file_status = ErrorCodes.TAR_ERROR
                        print(("TarError\n" +
                                "> file     : '{}'\n" +
                                "> args     : {}\n" +
                                "> message  : {}").format(
                                    full_path, e.args, e.message))

                if file_status == ErrorCodes.EXCL_EXT:
                    files_excluded += 1
                    files_excluded_bytes += file_stat.st_size
                elif file_status != ErrorCodes.SUCCESS:
                    files_errored += 1

                if file_stat != None:
                    size = file_stat.st_size
                    date_accessed = int(file_stat.st_atime)
                    date_modified = int(file_stat.st_mtime)
                else:
                    size = None
                    date_accessed = None
                    date_modified = None

                try:
                    perf_counter_before = time.perf_counter()
                    cur.execute(
                            sqlite3_insert_file,
                            (full_path, size, date_accessed, date_modified,
                                hash_code, file_status))
                    perf_counter_after = time.perf_counter()

                    sql_file_inserts += 1
                    sql_file_insert_time += \
                            perf_counter_after - perf_counter_before


                except sqlite3.IntegrityError as e:
                    print(("IntegrityError\n" +
                            "> file      : '{}'\n" +
                            "> args      : {}").format(
                                full_path, e.args))

    return files_allowed, files_allowed_bytes, files_excluded, \
            files_excluded_bytes, files_errored, files_hashed, \
            sql_file_inserts, sql_file_insert_time, tar_add_time, hash_time


def main(config):
    start_time = datetime.now()
    start_time_str = start_time.strftime("%y%m%d-%H%M%S")
    print("Started at  {}".format(start_time_str))

    j = json.loads(open(config).read())

    backup_name = j["name"]
    in_dirs = j["inDirs"]
    out_dir = j["outDir"]
    excluded_exts = j["excludedExts"]
    hash_excluded_files_max_size = j["hashExcludedFilesMaxSize"]

    validate_dirs(in_dirs)
    validate_dir(out_dir, os.R_OK | os.W_OK | os.X_OK)

    tar = create_tar(backup_name, out_dir, start_time_str)
    db = DbAgent(backup_name, out_dir, start_time_str)

    insert_error_codes(db)

    files_allowed, files_allowed_bytes, files_excluded, \
        files_excluded_bytes, files_errored, files_hashed, \
        sql_file_inserts, sql_file_insert_time, tar_add_time, \
        hash_time = do_backup(
                        db, tar, backup_name, in_dirs, out_dir,
                        excluded_exts, hash_excluded_files_max_size) 

    tar.close()

    print("Allowed files            : {}".format(files_allowed))
    print("Allowed files size       : {} ({} MB)".format(
            files_allowed_bytes, files_allowed_bytes / 1000000))
    print("Excluded files           : {}".format(files_excluded))
    print("Excluded files size      : {} ({} MB)".format(
            files_excluded_bytes, files_excluded_bytes / 1000000))
    print("Errored files            : {}".format(files_errored))
    print("Hashed files             : {}".format(files_hashed))
    print("SQL `file` inserts       : {}".format(sql_file_inserts))
    print("SQL `file insert time    : {}".format(sql_file_insert_time))
    print("Tar add time             : {}".format(tar_add_time))
    print("Hash time                : {}".format(hash_time))

    if sql_file_inserts > 0:
        print("Avg time per file insert : {}".format(
                sql_file_insert_time / float(sql_file_inserts)))

    if files_allowed > 0:
        print("Avg time per tar add     : {}".format(
                tar_add_time / float(files_allowed)))

    if files_hashed > 0:
        print("Avg time per hash        : {}".format(
                hash_time / float(files_hashed)))

    end_time = datetime.now()

    print("Finished at {}".format(end_time.strftime("%y%m%d-%H%M%S")))

    insert_meta(
            db, backup_name, in_dirs, out_dir, excluded_exts,
            hash_excluded_files_max_size, datetime_to_unix(start_time),
            datetime_to_unix(end_time), files_allowed, files_allowed_bytes,
            files_excluded, files_excluded_bytes, files_errored, files_hashed,
            sql_file_inserts, sql_file_insert_time, tar_add_time, hash_time)

    db.close()


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: {} <JSON config>".format(sys.argv[0]))
        sys.exit(1)
