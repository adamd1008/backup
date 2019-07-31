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
        `name`                         TEXT NOT NULL,
        `out_dir`                      TEXT NOT NULL,
        `excluded_exts`                TEXT NOT NULL,
        `hash_excluded_files_max_size` INTEGER NOT NULL
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
        `size`          INTEGER NOT NULL,
        `date_accessed` INTEGER NOT NULL,
        `date_modified` INTEGER NOT NULL,
        `hash`          TEXT NOT NULL,
        `error_code`    INTEGER NOT NULL
    );
    DROP TABLE IF EXISTS `error_codes`;
    CREATE TABLE IF NOT EXISTS `error_codes` (
        `error_code`   INTEGER NOT NULL UNIQUE,
        `error_string` TEXT NOT NULL,
        PRIMARY KEY(`error_code`)
    );
"""

sqlite3_insert_in_dir = "INSERT INTO `in_dir` (`dir`) VALUES (?)"
sqlite3_insert_meta = """
    INSERT INTO `meta` (
        `name`, `out_dir`, `excluded_exts`, `hash_excluded_files_max_size`
        ) VALUES (?, ?, ?, ?)
"""
sqlite3_insert_file = """
    INSERT INTO `file` (
        `path`, `size`, `date_accessed`, `date_modified`, `hash`, `error_code`
        ) VALUES (?, ?, ?, ?, ?, ?)
"""
sqlite3_insert_error_code = """
    INSERT INTO `error_codes` (`error_code`, `error_string`) VALUES (?, ?)
"""

def hashFile(filePath):
   sha1 = hashlib.sha1()
   
   f = open(filePath, 'rb')
   sha1.update(f.read())
   f.close()
   
   return sha1.hexdigest()


def dt_adapter(dt):
    return int(time.mktime(dt.timetuple()))

class DbAgent:
    def __init__(self, backup_name, out_dir, start_time):
        self.backup_name = backup_name
        self.out_dir = out_dir
        self.start_time = start_time

        self.db_name = self.backup_name + "-" + self.start_time + ".sqlite3"
        self.full_path = os.path.join(self.out_dir, self.db_name)
        print("Creating database '{}'".format(self.full_path))

        sqlite3.register_adapter(datetime, dt_adapter)
        self.conn = sqlite3.connect(self.full_path)
        self.conn.isolation_level = None

        self.cur = self.conn.cursor()
        self.cur.executescript(sqlite3_create_tables)
        self.cur.close()

    #def __del__(self):
    #    self.close()

    def close(self):
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


def main(config):
    time_now = datetime.now().strftime("%y%m%d-%H%M%S")

    j = json.loads(open(config).read())
    print("j = {}".format(j))

    backup_name = j["name"]
    in_dirs = j["inDirs"]
    out_dir = j["outDir"]
    excluded_exts = j["excludedExts"]
    hash_excluded_files_max_size = j["hashExcludedFilesMaxSize"]

    validate_dirs(in_dirs)
    validate_dir(out_dir, os.R_OK | os.W_OK | os.X_OK)

    db = DbAgent(backup_name, out_dir, time_now)
    db.close()



#conn = MySQLdb.connect(user = "kda", passwd = "4dvance",
#                       host = "127.0.0.1", db = "kda")
#conn.autocommit(True)
#cur = conn.cursor()
#
#cur.execute("SELECT `name`, `value` FROM `backupcfg`")
#
#for row in cur:
#   if row[0] == "backup_dir":
#      backupDirs.append(row[1])
#   elif row[0] == "tar_dir":
#      tarDir = row[1]
#   else:
#      print "Warning: invalid cfg name", row[0]
#
#cur.execute("SELECT `name` FROM `backupextforbidden`")
#
#for row in cur:
#   exclude.append(row[0])
#
#for x in backupDirs:
#   if not os.path.isdir(x):
#      print "Directory", x, "does not exist! Exiting..."
#      sys.exit(1)
#
#if not os.path.isdir(tarDir):
#   print "Tar directory", tarDir, "does not exist! Exiting..."
#   sys.exit(1)
#
#print "exclude:", exclude
#print "backupDirs:", backupDirs
#print "tarDir:", tarDir
#
#timeStr = datetime.now().strftime("%y%m%d-%H%M%S")
#timeStrDB = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#tarFile = "backup-%s.tar.bz2" % timeStr
#tarName = os.path.join(tarDir, tarFile)
#
#print "tarName:", tarName
#
#tar = tarfile.open(tarName, "w:bz2")
#
#query = "SELECT MAX(id) + 1 as max FROM backupinstance"
#cur.execute(query)
#backupInstance = int(cur.fetchone()[0])
#
#print "Next backup instance:", backupInstance
#
#query = "INSERT INTO backupinstance (`id`, `start`) VALUES (%d, '%s')" \
#        % (backupInstance, timeStrDB)
#
#print "Started at", timeStrDB
#
#cur.execute(query)
#
#print "###################################################" \
#      "#############################"
#
## 0 = yes, 1 = no (excluded extension), 3 = IOError, 4 = TarError
#
#totalAllowed = 0
#totalAllowedSize = 0
#totalDisallowed = 0
#totalDisallowedSize = 0
#
#for backupDir in backupDirs:
#   for root, dirs, files in os.walk(backupDir):
#      for name in files:
#         fileStatus = 0
#         
#         for x in exclude:
#            if str.lower(name[-len(x):]) == x:
#               fileStatus = 1
#               totalDisallowed += 1
#         
#         try:
#            fullPath = os.path.join(root, name)
#            fileStat = os.stat(fullPath)
#         except IOError as e:
#            print "IOError"
#            print "errno:", e.errno
#            print "args:", e.args
#            print "message:", e.message
#            print "strerror:", e.strerror
#            sys.exit(2)
#         
#         if fileStatus == 0:
#            totalAllowed += 1
#            totalAllowedSize += fileStat.st_size
#            
#            fileHash = hashFile(fullPath)
#            
#            try:
#               tar.add(fullPath)
#               
#               query = "INSERT INTO backupfile (`backupInstanceID`, `path`, " \
#                       "`size`, `dateAccessed`, `dateModified`, " \
#                       "`sha1`, `errno`) VALUES (%s, %s, %s, " \
#                       "FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), %s, 0)"
#               
#               cur.execute(query, (backupInstance,
#                           conn.escape_string(fullPath), fileStat.st_size,
#                           fileStat.st_atime, fileStat.st_mtime, fileHash))
#            except tarfile.TarError as e:
#               print "IOError"
#               print "args:", e.args
#               print "message:", e.message
#               sys.exit(3)
#         else:
#            totalDisallowed += 1
#            totalDisallowedSize += fileStat.st_size
#            
#            query = "INSERT INTO backupfile (`backupInstanceID`, `path`, " \
#                    "`size`, `dateAccessed`, `dateModified`, `errno`) " \
#                    "VALUES (%s, %s, %s, FROM_UNIXTIME(%s), " \
#                    "FROM_UNIXTIME(%s), 1)"
#            
#            cur.execute(query, (backupInstance,
#                        conn.escape_string(fullPath), fileStat.st_size,
#                        fileStat.st_atime, fileStat.st_mtime))
#         
#         #print fileStatus, fullPath
#
#query = "UPDATE backupinstance SET `end` = %s WHERE `id` = %s"
#
#timeStrDB = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#
#cur.execute(query, (timeStrDB, backupInstance))
#
#print "###################################################" \
#      "#############################"
#
#print "Finished at", timeStrDB
#print "Allowed files", totalAllowed, totalAllowedSize
#print "Disallowed files", totalDisallowed, totalDisallowedSize
#
#cur.close()
#conn.close()
#tar.close()
#
#try:
#   os.chmod(tarFile, 0440)
#except OSError as e:
#   print "chmod: OSError"
#   print "errno:", e.errno
#   print "args:", e.args
#   print "message:", e.message
#   print "strerror:", e.strerror

if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: {} <JSON config>".format(sys.argv[0]))
        sys.exit(1)
