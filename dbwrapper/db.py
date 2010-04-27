"""
Author: Sven Festersen (sven@sven-festersen.de)
Version: 1.0
Date: 27 April 2010
License: GPL

This module contains a simple thread-safe wrapper for a sqlite3 database.
"""


import sqlite3
import Queue
import threading
import time

class DBWrapper(threading.Thread):
    
    def __init__(self, filename=":memory:", logger=None):
        super(DBWrapper, self).__init__()
        self._filename = filename
        self._queue = Queue.Queue()
        self._stopped = threading.Event()
        self._logger = logger
        self._n = 0
        self.start()
        
    def _log_debug(self, msg):
        if self._logger != None:
            self._logger.debug(msg)
        else:
            print "%-8s %s" % ("DEBUG:", msg)
            
    def _log_info(self, msg):
        if self._logger != None:
            self._logger.info(msg)
        else:
            print "%-8s %s" % ("INFO:", msg)
            
    def _log_error(self, msg):
        if self._logger != None:
            self._logger.error(msg)
        else:
            print "%-8s %s" % ("ERROR:", msg)
        
    def run(self):
        self._log_debug("Database thread started.")
        self._query_lock = threading.Lock()
        self._connection = sqlite3.connect(self._filename)
        self._connection.row_factory = sqlite3.Row
        self._connection.text_factory = str
        self._log_info("Database connection established.")
        self._log_debug("Datbase file is '%s'." % self._filename)
        cursor = self._connection.cursor()
        
        while True:
            cmd, params, q = self._queue.get()
                
            if cmd == "close":
                self._log_debug("Database got 'close' command.")
                q.put([])
                break
            elif cmd == "commit":
                self._log_debug("Database got 'commit' command.")
                self._connection.commit()
                self._log.debug("Database changes committed.")
                q.put([])
                continue
                
            res = []
            try:
                for row in cursor.execute(cmd, params):
                    res.append(row)
            except sqlite3.Error, e:
                self._log_error("Database error: '%s'." % e.args[0])
            
            q.put(res)
        
        self._stopped.set()
        self._connection.close()
        self._log_info("Database connection closed.")
        self._log_debug("Database thread terminated.")
                
    def close(self):
        """
        Close the database connection and terminate thread.
        """
        self.execute("close")
        
    def commit(self):
        """
        Commit changes.
        """
        self.execute("commit")
        
    def execute(self, cmd, params=tuple()):
        """
        Execute the command cmd. '?'s in the command are replaced by the
        params tuple's content.
        """ 
        self._n += 1
        n = self._n
        start = time.time()
        if not self._stopped.isSet():
            q = Queue.Queue()
            self._queue.put((cmd, params, q))
            res = q.get()
            dur = time.time() - start
            self._log_debug("Database query %s executed in %s seconds." % (n, \
                                                                           dur))
            return res
        return None
        
    def get_tables(self):
        """
        Returns a list of table names currently in the database.
        """
        res = self.execute("SELECT tbl_name FROM sqlite_master")
        tables = []
        for row in res:
            name = row[0]
            tables.append(name)
        return tables
