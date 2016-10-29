__author__ = 'Tobias Gill'

import os
import logging
import logging.handlers
import MySQLdb

import BigBlue_errors as BB_err



# Ensures that the logger records that logs came from this script.
bigblue_logger = logging.getLogger(__name__)
# Sets the level of logging
bigblue_logger.setLevel(logging.INFO)

# Finds the bigblue log file location.
bigblue_log_loc = os.path.join(os.path.getcwd(), 'bigblue_logFiles','bigblue.log')
# Sets the log file name, mode 'a'=append, max log file size = 1MB, and then how many logs to rotate through.
bigblue_log_handler = logging.handlers.RotatingFileHandler(filename=bigblue_log_loc, mode='a', maxBytes=10**6,
                                                           backupCount=5)
# Format for log entries.
formatter = logging.Formatter('%(asctime)s: %(name)s - [%(levelname)s] %(message)s')
# Sets the format.
bigblue_log_handler.setFormatter(formatter)

# Adds the handlers to the logger.
bigblue_logger.addHandler(bigblue_log_handler)
# Prevents the log from also appearing in the command line / in a Jupyter notebook.
bigblue_logger.propagate = False

class BigBlue_logging():

    def __init__(self, user, logger=bigblue_logger, log_loc=bigblue_log_loc, logging_level='INFO'):

        # Defines user to log as.
        self.user = user
        # Sets the logger to be used.
        self.logger = logger
        # Sets the location of the log file.
        self.log_loc = log_loc
        # Sets the level of logging to associate with instance of BigBlue() class. Default is INFO level.
        self.logging_level = logging_level
        self.logging_level_init()

    def logging_level_init(self):

        if self.logging_level == 'CRITICAL' or self.logging_level == 'CRIT' or self.logging_level >= 50:
            self.logger.setLevel(logging.CRITICAL)
        elif self.logging_level == 'ERROR' or self.logging_level >= 40:
            self.logger.setLevel(logging.ERROR)
        elif self.logging_level == 'WARNING' or self.logging_level == 'WARN' or self.logging_level >= 30:
            self.logger.setLevel(logging.WARN)
        elif self.logging_level == 'INFO' or self.logging_level >= 20:
            self.logger.setLevel(logging.INFO)
        elif self.logging_level == 'DEBUG' or self.logging_level >= 10:
            self.logger.setLevel(logging.DEBUG)
        else:
            # If input is not known will add a single warning to log file and revert to INFO level.
            self.logger.setLevel(logging.INFO)
            self.logger.warn(self.sys_log_entry('Logging level not recognised. Reverting to default level: INFO'))

    def user_log_entry(self, comment):
        return "[" + self.user + '] ' + comment

    def sys_log_entry(self, comment):
        return "[SYS]" + comment

    def log_mysql_err(self, err):

        self.err = err
        try:
            if self.logger.isEnabledFor(logging.ERROR):
                self.logger.error('MySQL Error [%d]: %s' % (self.err.args[0], self.err.args[1]))
        except IndexError:
            if self.logger.isEnabledFor(logging.ERROR):
                self.logger.error('MySQL Error: %s' % str(self.err))
        finally:
            raise MySQL_Error('MySQL Error. See logfile: %s.' % self.log_loc)

    def log_query(self, query):

        self.query_str = query
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(self.user_log_entry(self.query_str))

    def log_duplicate_err(self, database, table, timestamp):
        """
        Function Type: Logging/Error Handling
        Function Use: Use when database query finds duplicate entries.
        Function Outcome: Logs duplicate error to log file and raises and error.
        """
        self.database = database
        self.table = table
        self.timestamp = timestamp
        if self.logger.isEnabledFor(logging.ERROR):
                self.logger.error(self.user_log_entry('Database Error [DUPLICATE]: Within Database: %s, found multiple '
                                                      'rows in Table: %s, with creation_timestamp: %s'
                                                      % (self.database, self.table, self.timestamp)))
        raise BB_err.DuplicateEntryError('Database Error [DUPLICATE]: Within Database: %s, found multiple rows in '
                                         'Table: %s, with creation_timestamp: %s' % (self.database, self.table,
                                                                                     self.timestamp))

    def log_checkfileexist(self, database, table, timestamp, filename):
        """
        :type: Logging
        :use: Logs that an SQL query has been made that attempts to determine if an entry already exists for the file.
        :param table: name of MySQL database table to check.
        :return: None
        """
        self.database = database
        self.table = table
        self.timestamp = timestamp
        self.filename = filename
        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(self.user_log_entry('Checking Database: %s, Table: %s, for File: %s, with '
                                                 'creation_timestamp: %s' % (self.database, self.table, self.filename,
                                                                             self.timestamp)))

    def log_noexistingfile(self, database, table, timestamp, filename):
        """
        Function Type: Logging
        Function Use: Use when database query does not find an existing entry.
        :param table: name of MySQL database table checked.
        :return: None
        """
        self.database = database
        self.table = table
        self.timestamp = timestamp
        self.filename = filename
        if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(self.user_log_entry('No File: %s  with creation_timestamp: %s found in Database: %s, '
                                                     'Table: %s.' % (self.filename, self.creation_timestamp,
                                                                     self.database, self.table)))
    def log_existingfile(self, database, table, timestamp, filename):
        """

        :param database: string containing the name of the database in use.
        :param table: string containing the name of the table searched within database.
        :param timestamp: string containing the timestamp of the file being searched for.
        :param filename: string containing the name of the file being searched for.
        :return: None.
        """
        self.database = database
        self.table = table
        self.timestamp = timestamp
        self.filename = filename

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(self.user_log_entry('Existing entry found for File: %s with creation_timestamp: %s in '
                                                 'Database: %s, Table: %s.' % (self.filename, self.timestamp,
                                                                               self.database, self.table)))

    def log_returnerror(self, database, table, timestamp, result):
        """

        :param database: string containing the name of the database in use.
        :param table: string containing the name of the table searched within database.
        :param timestamp: string containing the timestamp of the file being searched for.
        :param result: string containing the erroneous search result.
        :return: None.
        """
        self.database = database
        self.table = table
        self.timestamp = timestamp
        self.result = result

        if self.logger.isEnabledFor(logging.ERROR):
            self.logger.error(self.user_log_entry('[RETURN ERROR] Returned result from Database: %s, Table: %s has '
                                                  'timestamp: %s that does not equal the searched timestamp: %s.'
                                                  % (self.result, self.timestamp)))

    def log_logicerror(self, database, table, timestamp, result):
        """

        :param database: string containing te name of the database in use.
        :param table: string containing the name of the table searched within database.
        :param timestamp: string containing the timestamp of the file being searched for.
        :param result: string containing the timestamp of the returned result.
        :return: None.
        """
        self.database = database
        self.table = table
        self.timestamp = timestamp
        self.result = result

        if self.logger.isEnabledFor(logging.ERROR):
            self.logger.error(self.user_log_entry('[LOGIC ERROR] Returned result from Database: %s, Table: %s has '
                                                  'timestamp: %s that does and does not equal searched result: %s.'
                                                  % (self.database, self.table, self.result, self.timestamp)))
