"""
Author:      Sven Festersen (sven@sven-festersen.de)
Version:     1.1
Created:     27 April 2010
Last Change: 29 March 2013
License:     GPL

This module contains a simple thread-safe wrapper for a sqlite3 database.
"""
import logging
import Queue
import sqlite3
import threading
import time


class DBWrapper(threading.Thread):
  
  def __init__(self, filename=":memory:", logger=None, log_level=logging.INFO):
    """
    Create a new DBWrapper instance. All parameters are optional.
    
    Parameters:
      filename:   Path to the database file. If filename is ":memory:", the
                  database is created in the RAM (default).
      logger:     A Python logging.Logger instance or None (default).
      log_level:  If the logger parameter is None, use this log_level to display
                  log messages (default is logging.INFO).    
    """
    super(DBWrapper, self).__init__()
    self._filename = filename
    self._queue = Queue.Queue()
    self._stopped = threading.Event()
    self._n = 0
    #initialize logging
    if logger == None:
      self._logger = logging.getLogger('DBWrapper')
      self._logger.setLevel(log_level)
      msg_handler = logging.StreamHandler()
      fmt_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
      msg_fmt = logging.Formatter(fmt_str)
      msg_handler.setFormatter(msg_fmt)
      self._logger.addHandler(msg_handler)
      msg_handler.setLevel(log_level)
    #start DBWrapper
    self.start()
    
  def run(self):
    self._logger.debug("Database thread started.")
    self._query_lock = threading.Lock()
    self._connection = sqlite3.connect(self._filename)
    self._connection.row_factory = sqlite3.Row
    self._connection.text_factory = str
    self._logger.info("Database connection established.")
    self._logger.debug("Datbase file is '%s'." % self._filename)
    cursor = self._connection.cursor()
    
    while True:
      cmd, params, q = self._queue.get()
        
      if cmd == "close":
        self._logger.debug("Database got 'close' command.")
        q.put([])
        break
      elif cmd == "commit":
        self._logger.debug("Database got 'commit' command.")
        self._connection.commit()
        self._logger.debug("Database changes committed.")
        q.put([])
        continue
        
      res = []
      try:
        for row in cursor.execute(cmd, params):
          res.append(row)
      except sqlite3.Error, e:
        self._logger.error("Database error: '%s'." % e.args[0])
      
      q.put(res)
    
    self._stopped.set()
    self._connection.close()
    self._logger.info("Database connection closed.")
    self._logger.debug("Database thread terminated.")
        
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
      self._logger.debug("Database query %s executed in %s seconds." % (n, \
                                       dur))
      return res
    return None
    
  def __call__(self, cmd, params=tuple()):
    """
    Alias for the execute() method.
    """
    return self.execute(cmd, params)
    
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
