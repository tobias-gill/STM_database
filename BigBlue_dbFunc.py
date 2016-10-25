__author__ = 'Tobias Gill'

import MySQLdb
import os
import time
import logging
import logging.handlers
import flatfile as ff


""" Use to output useful information when debugging scripts """
DEBUG = False

"""
Error Classes
"""

class Error(Exception):
    """Default Error Class for BigBlue_bdFunc"""
    pass

class UnknownFlatFileFormat(Error):
    """File contains unknown data format"""
    pass

class DatabaseConnectionError(Error):
    """Cannot connect to database"""

class CreationCommentError(Error):
    """STM Flat file creation comment is in unknown format"""
    pass

class LastEntryIdError(Error):
    """Unable to find last entry id. database rollback."""
    pass

class UnableToFindEntryError(Error):
    """Unable to find entry in database"""
    pass

class DatabaseEntryError(Error):
    """Unable to add entry to database"""
    pass

class DatabaseDeleteError(Error):
    """Unable to delete entry from database"""
    pass

class LogicError(Error):
    """Unexpected error in logic"""
    pass

class DuplicateEntryError(Error):
    """Found duplicate entries"""
    pass

class ExistingEntryError(Error):
    """Entry already exists"""
    pass

"""
Logging
"""

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

"""
BigBlue
"""

class  BigBlue():

    def __init__(self, user, password, stm_file, database='cryo_stm_data', logging_level='INFO'):
        self.user = user  # SQL database Username.
        self.password = password  # SQL database password.

        # Initialise logging parameters
        self.log_init(logging_level)

        # SQL database host location. Should always remain localhost as users should shh into server.
        self.host = 'localhost'
        # Each experimental system has it's own database. Default at the moment is the cyro as this is the test case.
        self.database = database

        # stm_file: Must be a path that leads to an Omicron .Flat file.

        # Load Flat File data from stm_file. Also creates stm_filePath and stm_fileName and self.stm_file is defined as
        # a flatfile object from the flatfile module.
        self.get_stmFile(stm_file)

        # Get Flat File data type
        self.get_dataType(self.stm_file)

        # Get number of scans in file
        self.get_numberOfScans(self.stm_file)

        # Get the file info's for each scan
        self.get_fileInfos(self.stm_file)

        # Get the experiment creation timestamp from file
        self.get_expTimeStamp()

        # Get creation comment from file
        self.get_creationComment()

        # Test and format creation comment for use in SQL database
        self.commentTest(self.creation_comment)

        # Get stm data from file
        self.get_stmData(self.stm_file)

    """
    Logging Funcs
    """
    def log_init(self, logging_level):
        # Sets the level of logging to associate with instance of BigBlue() class. Default is INFO level.
        self.logging_level = logging_level
        if self.logging_level == 'CRITICAL' or self.logging_level == 'CRIT' or self.logging_level >= 50:
            bigblue_logger.setLevel(logging.CRITICAL)
        elif self.logging_level == 'ERROR' or self.logging_level >= 40:
            bigblue_logger.setLevel(logging.ERROR)
        elif self.logging_level == 'WARNING' or self.logging_level == 'WARN' or self.logging_level >= 30:
            bigblue_logger.setLevel(logging.WARN)
        elif self.logging_level == 'INFO' or self.logging_level >= 20:
            bigblue_logger.setLevel(logging.INFO)
        elif self.logging_level == 'DEBUG' or self.logging_level >= 10:
            bigblue_logger.setLevel(logging.DEBUG)
        else:
            # If input is not known will add a single warning to log file and revert to INFO level.
            bigblue_logger.setLevel(logging.WARN)
            bigblue_logger.warn(self.sys_log_entry('Logging level not recognised. Reverting to default level: INFO'))
            bigblue_logger.setLevel(logging.INFO)

    def user_log_entry(self, comment):
        return "[" + self.user + '] ' + comment

    def sys_log_entry(self, comment):
        return "[SYS]" + comment

    """
    Flat File manipulation
    """
    def get_stmFile(self, stm_file):
        """ the input stm_file is the full path to the desired Omicron Flat file.
        It is then replaced by the loaded data that has been parsed by ff."""
        # FIXME: The replacement of stm_file with stm_file might be stupid, at the very least it is confusing.
        self.stm_filePath = os.path.normpath(stm_file)
        self.stm_fileName = self.stm_filePath.split('\\')[-1]  # Takes last element of split path to get file name.
        self.stm_file = ff.load(self.stm_filePath)  # Uses ff. to parse the file.

    def get_numberOfScans(self, stm_file):
        """ Gets the number of data files within the stm_file. I.E. for a topograph with fwd, bck and up, down images
        this will return the integer 4."""
        self.numberOfScans = len(stm_file)

    def get_dataType(self, stm_file, scan=0):
        """ uses the info funtion from ff to extract the file type of stm_file. By default it uses the first scan as
        this will be in all files."""
        self.stm_fileType = stm_file[scan].info['type']

    def ret_fileInfo(self, stm_file, scan_dir=0):
        """ Simple function to return the info dictionary extracted by ff.info"""
        return stm_file[scan_dir].info

    def get_fileInfos(self, stm_file):
        """ Extracts the ff.info dictionaries from each scan and produces a list of them"""
        self.fileInfos = []
        for i in range(0, self.numberOfScans):
            self.fileInfos.append(self.ret_fileInfo(stm_file, scan_dir=i))

    def get_expTimeStamp(self):
        """Extracts the timestamp of the experiments from the filename"""
        self.expTimeStamp = time.strptime(self.stm_fileName.split('_')[1], "%Y%b%d-%H%M%S")
        # Converts timestamp into a 19 character long string. This is for convenience of adding to SQL database.
        self.creation_timestamp = ''
        for i in range(0, 6):
            if self.expTimeStamp[i] < 10:
                self.creation_timestamp = self.creation_timestamp + '0' + str(self.expTimeStamp[i])
            else:
                self.creation_timestamp = self.creation_timestamp + str(self.expTimeStamp[i])

    def get_creationComment(self):
        """ Takes the first scan creation comment, drops useless information"""
        self.creation_comment = self.fileInfos[0]['comment'].split(';')[2][16:]

    def commentTest(self, creation_comment):
        """
        creation comment is now being used to include extra metadata about each experiment. This is a new addition and
        is therefore not standard across all, even most, data.
        This function tests the structure of the creationcomment to determine what to pass to the database.

        test creation comment structure:
        users: user1, user2,..., userN                          # string with entries separated by ,
        substrate: substrate1, substrate2, ..., substrateN      # string with entries separated by ,
        adsorbates: adsorbate1, adsorbate3, ..., adsorbateN     # string with entries separated by ,
        prep: details of sample prep.                           # string with no requirement for a standard structure
        notebook: notebook number                               # string, but should be an integer
        notes: further details                                  # string, no structure
        """

        self.creation_comment = creation_comment

        if len(self.creation_comment.split('\n')) > 1:
            self.creation_comment_split = self.creation_comment.split('\n')
            for i in range(0, len(self.creation_comment_split)):
                self.creation_comment_split[i] = self.creation_comment_split[i].split(':')
            # FIXME I'm sure there is a better way of doing this than listing all possible iterations.
            self.creation_comment_format_ideal = ['user', 'substrate', 'adsorbate', 'prep', 'notebook', 'note']
            self.creation_comment_format_idealCaps = ['User', 'Substrate', 'Adsorbate', 'Prep', 'Notebook', 'Note']
            self.creation_comment_format_idealPlural = ['users', 'substrates', 'adsorbates', 'preps', 'notebooks',
                                                        'notes']
            self.creation_comment_format_idealCapsPlural = ['Users', 'Substrates', 'Adsorbates', 'Preps', 'Notebooks',
                                                            'Notes']
            self.creation_comment_format_badSpelling = ['user', 'substrate', 'absorbate', 'prep', 'notebook', 'note']
            for i in range(0, 6):
                if self.creation_comment_split[i][0] == self.creation_comment_format_ideal[i] or \
                                self.creation_comment_split[i][0] == self.creation_comment_format_idealCaps[i] or \
                                self.creation_comment_split[i][0] == self.creation_comment_format_idealPlural[i] or \
                                self.creation_comment_split[i][0] == self.creation_comment_format_idealCapsPlural[i] or \
                                self.creation_comment_split[i][0] == self.creation_comment_format_badSpelling[i]:
                    self.creation_metadata ={'exp_users': self.creation_comment_split[0][1][1:], # FIXME the [1:] at the end of these may drop valuable information. Need to make it so this only happens if the first character is a 'space'.
                                             'exp_substrate': self.creation_comment_split[1][1][1:],
                                             'exp_adsorbate': self.creation_comment_split[2][1][1:],
                                             'exp_prep': self.creation_comment_split[3][1][1:],
                                             'exp_notebook': int(self.creation_comment_split[4][1][1:]),
                                             'exp_notes': self.creation_comment_split[5][1][1:]}
                else:
                    self.creation_metadata ={'exp_users': None,
                                             'exp_substrate': None,
                                             'exp_adsorbate': None,
                                             'exp_prep': None,
                                             'exp_notebook': None,
                                             'exp_notes': None}
                    raise CreationCommentError('Creation comment of %s does not match expected format'
                                               % self.stm_fileName)

    def ret_stmData(self, stm_file, scan_dir=0):
        """ Returns the experimental data from a specified ff[scan_dir] object."""
        return stm_file[scan_dir].data

    def get_stmData(self, stm_file):
        self.stm_data = []
        for i in range(0, self.numberOfScans):
            self.stm_data.append(self.ret_stmData(stm_file, scan_dir=i))

    """
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%           SQL Funcs           %%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    """


    """
    ************************************
    ***         General Funcs        ***
    ************************************
    """

    def connectionTest(self):
        """ Tries to connect to the database"""
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        self.db.close() # FIXME Need to find a way to make this raise an exception if it cannot connect.

    def get_lastEntryId(self):
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        self.cursor = self.db.cursor()

        self.query = "SELECT LAST_INSERT_ID()"

        try:
            self.cursor.execute(self.query)
            self.lastEntryId = self.cursor.fetchall()
        except:
            self.db.rollback()
            raise LastEntryIdError('Was unable to extract last entry id from %s' % self.database)
        self.db.close()

    def add_entry(self):
        """ Use this function to intelligently and safely enter any flatfile into the database"""

        if self.stm_fileType == 'topo':
            self.safeAdd_exp_metadata()
            if DEBUG:
                print('safeAdd_exp_metadata complete')
            self.safeAdd_stm_files()
            if DEBUG:
                print('safeAdd_stm_files complete')
            self.safeAdd_stm_topo_metadata()
            if DEBUG:
                print('safeAdd_stm_topo_metadata complete')
        elif self.stm_fileType == 'ivcurve':
            self.safeAdd_exp_metadata()
            if DEBUG:
                print('safeAdd_exp_metadata complete')
            self.safeAdd_stm_files()
            if DEBUG:
                print('safeAdd_stm_files complete')
            self.safeAdd_stm_spec_metadata()
            if DEBUG:
                print('safeAdd_stm_spec_metadata complete')
        elif self.stm_fileType == 'ivmap':
            self.safeAdd_exp_metadata()
            if DEBUG:
                print('safeAdd_exp_metadata complete')
            self.safeAdd_stm_files()
            if DEBUG:
                print('safeAdd_stm_files complete')
            self.safeAdd_stm_cits_metadata()
            if DEBUG:
                print('safeAdd_stm_cits_metadata complete')
        elif self.stm_fileType == 'izcurve':
            print('not yet complete') # FIXME: Need to fix flatfile module, as currently does not read iz data.
        else:
            raise UnknownFlatFileFormat('%s contains an unknown data type' % self.stm_fileName)

    """
    ************************************
    ***         exp_metadata         ***
    ************************************
    """

    def check_expMetadataExist(self):
        """ Function to check whether an entry for exp_metadata already exists for a given file.
        I.E. Many hundreds of flatfiles contain the same experimental metadata and so as not to create excessive
        redundant entries we check to see if one already exists. This is achieved by checking for to experiment
        creation timestamp which is a part of each flatfile name."""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query to return all entries that have a creation timestamp equal to the current flatfile
        self.query = "SELECT exp_timestamp FROM exp_metadata WHERE exp_timestamp = %s" % self.creation_timestamp

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Fetch all the rows in a list of lists.
            self.results = self.cursor.fetchone()
            # Check to see if log needs to be generated.
            if bigblue_logger.isEnabledFor(logging.INFO):
                # Adds shortened details of query to logfile is logging level is set to INFO
                bigblue_logger.info(self.user_log_entry('check_expMetaDataExist on file: %s'
                                                        % self.stm_fileName))
            if bigblue_logger.isEnabledFor(logging.DEBUG):
                # Adds the full query used to log file if logging level is set to DEBUG
                bigblue_logger.debug(self.user_log_entry(self.query))
        except:
            # If no result log error and raise exception.
            if bigblue_logger.isEnabledFor(logging.ERROR):
                bigblue_logger.error(self.user_log_entry('Unable to connect to database: %s' % self.database))
            raise DatabaseConnectionError('Unable to connect to database: %s' % self.database)

        # Close connection to database.
        self.db.close()


        if self.results == None:
            # If no entry in database has equivalent timestamp, log and return False.
            if bigblue_logger.isEnabledFor(logging.INFO):
                bigblue_logger.info(self.user_log_entry('No file with timestamp: %s found in database: %s'
                                                        % (self.creation_timestamp, self.database)))
            return False
        elif self.results[0] == self.creation_timestamp:
            # If an entry already exists with equivalent timestamp, log and return True.
            if bigblue_logger.isEnabledFor(logging.INFO):
                bigblue_logger.info(self.user_log_entry('File with timestamp: %s found in database: %s'
                                                        % (self.creation_timestamp, self.database)))
            return True
        elif self.results[0] != self.creation_timestamp:
            # If result returns entry with different timestamp there may have been a mistake in the query generated.
            if bigblue_logger.isEnabledFor(logging.ERROR):
                bigblue_logger.error(self.user_log_entry('Returned file timestamp: %s does not equal queried '
                                                         'timestamp: %s.\nCheck submitted query: %s'
                                                         % (self.results[0], self.creation_timestamp, self.query)))
            return False
        else:
            if bigblue_logger.isEnabledFor(logging.ERROR):
                # If we get this far things have gone very wrong as returned entry neither matches or does not match
                # the files creation timestamp.
                bigblue_logger.error(self.user_log_entry('LOGIC ERROR: check_expMetadataExist has returned result '
                                                         'that neither matches or does not match the timestamp of %s'
                                                         % self.stm_fileName))

    def add_exp_metadata(self):
        """ Inserts experiment metadata from stm_file into SQL database """

        # Open database connection
        self.db  = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        # Prepare SQL query to insert experiment metadata into database exp_metadata
        self.query = "INSERT INTO exp_metadata(exp_users, exp_substrate, exp_adsorbate, exp_prep, exp_notebook, " \
                     "exp_notes, exp_timestamp) VALUES ('%s', '%s', '%s', '%s', '%d', '%s', '%s')" % \
                     (self.creation_metadata['exp_users'], self.creation_metadata['exp_substrate'],
                      self.creation_metadata['exp_adsorbate'], self.creation_metadata['exp_prep'],
                      int(float(self.creation_metadata['exp_notebook'])), self.creation_metadata['exp_notes'],
                      self.creation_timestamp)

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit insertion into database
            self.db.commit()
            if bigblue_logger.isEnabledFor(logging.INFO):
                # Log that a file has been added to the database.
                bigblue_logger.info(self.user_log_entry('File: %s added to database: %s by add_exp_metadata()'
                                                        % (self.stm_fileName, self.database)))
            if bigblue_logger.isEnabledFor(logging.DEBUG):
                # If DEBUG level logging is enabled log the entire query.
                bigblue_logger.debug(self.user_log_entry(self.query))

        except:
            # If error in execute rolls back database
            self.db.rollback()
            if bigblue_logger.isEnabledFor(logging.ERROR):
                bigblue_logger.error(self.user_log_entry('Unable to add %s into exp_metadata within %s. '
                                                         'Database rolled back.'% (self.stm_fileName, self.database)))
            raise DatabaseEntryError('Unable to add %s into exp_metadata within %s'
                                     % (self.stm_fileName, self.database))
        # Close database connection
        self.db.close()

    def safeAdd_exp_metadata(self):
        """ This function uses check_expMetaDataExist to see if an entry already exists with the same timestamp
        as stm_file. If there is no such entry it is inserted into the database, however if it is an error is raised"""

        # Check to see if an entry with timestamp exists
        if self.check_expMetadataExist():
            if bigblue_logger.isEnabledFor(logging.INFO):
                bigblue_logger.info(self.user_log_entry('exp_metadata in %s already contains entry with timestamp: %s'
                                                        % (self.database, self.creation_timestamp)))
        elif not self.check_expMetadataExist():
            # If no entry in table exists with the same timestamp, add file into table.
            self.add_exp_metadata()
            if bigblue_logger.isEnabledFor(logging.INFO):
                # Log entry into log file.
                bigblue_logger.info(self.user_log_entry('No existing entry with timestamp: %s found in exp_metadata '
                                                        'in database: %s.' % (self.creation_timestamp, self.database)))

    def delete_exp_metadata(self):
        """ Can be used to delete an entry with the same timestamp as stm_file from exp_metadata in database"""

        # Open connection with database.
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL command to delete all entries with exp_timestamp equal to creation_timestamp of current stm_file
        self.query = "DELETE FROM exp_metadata WHERE exp_timestamp = '%s'" % self.creation_timestamp

        try:
            self.cursor.execute(self.query)
            self.db.commit()
            if bigblue_logger.isEnabledFor(logging.INFO):
                # Log File Deletion general info
                bigblue_logger.info(self.user_log_entry('File: %s DELETED from database: %s'
                                                        % (self.stm_fileName, self.database)))
            if bigblue_logger.isEnabledFor(logging.WARN):
                # log warning that file has been deleted. Use full query for better tracking.
                bigblue_logger.warn(self.user_log_entry(self.query))
        except:
            self.db.rollback()
            if bigblue_logger.isEnabledFor(logging.ERROR):
                # Log error is connection failed and rollback occurred.
                bigblue_logger.error(self.user_log_entry('Unable to connect to database: %s' % self.database))
            raise DatabaseDeleteError('Could not delete entries with exp_timestamp: %s from exp_metadata in %s'
                                      % (self.creation_timestamp, self.database))
        self.db.close()

    """
    ************************************
    ***           stm_files          ***
    ************************************
    """

    def check_stmFilesExist(self):
        """ Each stmFile should be unique therefore we shall check to ensure no entry exists with the same filename.
        Unlike exp_metadata there should not be multiple files that create this, however it can potentially be
        'referenced' multiple times, depending on the number of scans in a file. i.e fwd, bwd etc. """

        # Open connection to datbase
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor obeject with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to return all results in database with stm_fileName
        self.query = "SELECT file_name FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        try:
            self.cursor.execute(self.query)  # Try to execute SQL command
            self.results = self.cursor.fetchall()  # Return results of query as a list of lists.
            if bigblue_logger.isEnabledFor(logging.INFO):
                # Adds shortened details of query to logfile if logging level is set to INFO
                bigblue_logger.info(self.user_log_entry('check_stmFilesExist on file: %s'
                                                        % self.stm_fileName))
            if bigblue_logger.isEnabledFor(logging.DEBUG):
                # Adds the full query used to log file if logging level is set to DEBUG
                bigblue_logger.debug(self.user_log_entry(self.query))

        except:
            # If no connection can be made, log an error and raise and exception.
            if bigblue_logger.isEnabledFor(logging.ERROR):
                bigblue_logger.error(self.user_log_entry('Unable to connect to database: %s' % self.database))
            raise DatabaseConnectionError('Unable to connect to database: %s' % self.database)
        self.db.close()

        # Test to see if there is a unique result
        if len(self.results) > 1:
            if bigblue_logger.isEnabledFor(logging.WARN):
                bigblue_logger.warn(self.user_log_entry('[DUPLICATE] Found duplicate entry in database: %s. table: '
                                                        'stm_files of file: %s' % (self.database, self.stm_fileName)))
            if bigblue_logger.isEnabledFor(logging.DEBUG):
                bigblue_logger.debug(self.user_log_entry('[DUPLICATE INFO] %s' % self.query))
            raise DuplicateEntryError('Found a duplicate entry of %s in stm_files within %s'
                                      % (self.stm_fileName, self.database))
        # If a unique results is found, check that it does in fact equal stm_fileName
        elif len(self.results) == 1:
            if self.results[0][0] == self.stm_fileName:
                if bigblue_logger.isEnabledFor(logging.DEBUG):
                    bigblue_logger.debug(self.user_log_entry('File: %s already exists in database: %s table: stm_files'
                                                             % (self.stm_fileName, self.database)))
                # entry returned from database is definitely equal to stm_fileName
                return True
            elif self.results[0][0] != self.stm_fileName:
                # Entry returned from SQL command does not actually equal stm_fileName
                return False
        # If len(self.results) is neither == 1 or > 1 then return false.
        else:
            return False

    def add_stm_files(self):
        """ Inserts stm_files entry from stm_file into SQL database """

        # Need to find the correct exp_metadata_id from exp_metadata to associate file with.
        # Could achieve this by using get_lastEntryId() however this would only work upon sequential adding of data,
        # instead we shall query the exp_metadata table for an exp_metadata_id that matches the exp_timestamp in the
        # current stm_file fileName.

        # Open database connection.
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method.
        self.cursor = self.db.cursor()
        # Prepare SQL command, to retrieve exp_metadata_id
        self.query = "SELECT exp_metadata_id FROM exp_metadata WHERE exp_timestamp = '%s'" % self.creation_timestamp
        if DEBUG:
            print self.query

        try:
            # Execute SQL command.
            self.cursor.execute(self.query)
            # Fetch the exp_metadata_id.
            self.exp_metadata_id = self.cursor.fetchone()[0]
        except:
            # If no result found.
            raise UnableToFindEntryError('Unable to find entry in exp_metadata that has same timestamp as %s'
                                         % self.stm_fileName)
        # Close connection to database.
        self.db.close()
        # FIXME: It is probably redundant to close then reopen the connection. Look into this.
        # Open database connection
        self.db  = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()

        self.file_timestamp = time.strptime(self.fileInfos[0]['date'], "%Y-%m-%d %H:%M:%S")
        self.stm_fileDate = ''
        for i in range(0, 6):
            if self.file_timestamp[i] < 10:
                self.stm_fileDate = self.stm_fileDate + '0' + str(self.file_timestamp[i])
            else:
                self.stm_fileDate = self.stm_fileDate + str(self.file_timestamp[i])

        # Prepare SQL query to insert stm_file into database
        self.query = "INSERT INTO stm_files(exp_metadata_id, file_name, file_date, file_type, file_location)" \
                     "VALUES ('%d', '%s', '%s', '%s', '%s')" % \
                     (int(float(self.exp_metadata_id)), self.stm_fileName, self.stm_fileDate, self.stm_fileType,
                      str(self.stm_filePath.replace('\\', '/')))
                      # the .replace() function is to avoid an exlcusion 'error' for double backslashes in MySQL.

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit insertion into database
            self.db.commit()
            if DEBUG:
                # Prints confirmation of insertion
                print('%s added to stm_files in %s' % (self.stm_fileName, self.database))
        except:
            # If error in execute rolls back database
            self.db.rollback()
            raise DatabaseEntryError, 'Unable to add %s into stm_files within %s' %(self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

    def safeAdd_stm_files(self):
        """ This function uses check_stmFilesExist to see if an entry already exists with the same fileName
        as stm_file. If there is no such entry it is inserted into the database, however if it is an error is raised"""

        # Check to see if an entry with fileName exists
        if self.check_stmFilesExist():
            print 'stm_files in %s already contains entry with fileName: %s' % (self.database, self.stm_fileName)
        else:
            # If not add entry
            self.add_stm_files()

    def delete_stm_files(self):
        """ Can be used to delete an entry with the same stm_fileName as stm_file from stm_files in database"""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query. Drop entry with same filename as stm_file.
        self.query = "DELETE FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        try:
            # Excecute SQL command
            self.cursor.execute(self.query)
            # Commit changes to database
            self.db.commit()
            if DEBUG:
                print ('Entry with file name: %s removed from stm_files in %s') % (self.stm_fileName, self.database)
        except:
            self.db.rollback()
            raise DatabaseDeleteError, 'Entry with file name: %s could not be deleted from stm_files in %s' % \
                                       (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

    """
    ************************************
    ***      stm_topo_metadata       ***
    ************************************
    """

    def check_stmTopoMetadataExist(self):
        """ Each stm_topo_metadata entry should link to a single stm_file entry ad single exp_metadata entry. It can
         then be referenced by multiple stm_topo_data entries.
         First the file_id of the appropriate stm_files entry is found by looking for the file_name.
            If no file_id can be found raise an error.
         Once we have the file_id we can check stm_topo_metadata for entries linked to this file_id.
         If there are none then return False, if there is an entry return True."""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query to return the file_id of all entries that have a fileName equal to the current flatfile
        self.query = "SELECT file_id FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        if DEBUG:
            print self.query

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Fetch all the rows in a list of lists.
            self.results = self.cursor.fetchall()
        except:
            # If fail close connection and end here.
            self.db.close()
            raise UnableToFindEntryError, 'Could not find %s in stm_files within %s.' % \
                                          (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

        if len(self.results) > 1:
            raise DuplicateEntryError, 'Duplicate entry found in stm_files within %s with file name: %s' % \
                                       (self.database, self.stm_fileName)
        elif len(self.results) == 1:
            if self.results[0] == None:
                raise UnableToFindEntryError, 'Was unable to find %s in stm_files within %d' % \
                                              (self.stm_fileName, self.database)
            else:
                self.stm_file_id = self.results[0][0]

        else:
            raise UnableToFindEntryError, 'Unable to find an entry in stm_files within %s with filename: %s' % \
                                          (self.database, self.stm_fileName)

        # Connect to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL command to return stm_topo_metadata_id associated with stm_file_id of current stm_file
        self.query = "SELECT topo_metadata_id FROM stm_topo_metadata WHERE file_id = '%d'" % self.stm_file_id

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            self.results = self.cursor.fetchall()

        except:
            # If no results returned
            self.db.close()
            raise DatabaseConnectionError, 'could not connect here'

        if len(self.results) > 1:
            raise DuplicateEntryError, 'Duplicate entry found in stm_topo_metadata within %s with file_id %s' % \
                                           (self.database, self.stm_file_id)
        elif len(self.results) == 1:
            self.stm_topo_metadata_id = self.results[0][0]
            self.db.close()
            return True
        else:
            return False

    def add_stm_topo_metadata(self):
        """
        Each stm_files (that is a topography) entry should be associated to a single entry within stm_topo_metadata that
        can then be associated to multiple stm_topo_data entries.
        Here we must find the stm_file_id and exp_metadata_id associated with the stm_topo_metadata of the current
        stm_file.
        """
        # Open connection to databas
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to return the exp_metadata_id and stm_file_id
        self.query = "SELECT * FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        if DEBUG:
            print(self.query)

        try:
            # Execute SQL command.
            self.cursor.execute(self.query)
            # Return all results from query.
            self.results = self.cursor.fetchall()
        except:
            # If not able to connect
            raise DatabaseConnectionError, 'Unable to connect to stm_files within %s' % self.database
        # Close connection to database
        self.db.close()

        # There should only be a single entry for a given filename.
        if len(self.results) > 1:
            raise DuplicateEntryError, 'There are multiple entries in stm_files within %s with the filename: %s' % \
                                        (self.database, self.stm_fileName)
        elif len(self.results) == 1:
            self.file_id = self.results[0][0]
            self.exp_metadata_id = self.results[0][1]
        else:
            raise UnableToFindEntryError, 'Unable to find entry in stm_files within %s with filename: %s' % \
                                            (self.database, self.stm_fileName)
        # FIXME: It is probably redundant to close then re-open database connection.

        # Open connection to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to add entry to stm_topo_metadata
        self.query = "INSERT INTO stm_topo_metadata(exp_metadata_id, file_id, v_gap, i_set, x_res, y_res, x_inc, y_inc, xy_unit) VALUES ('%d', '%d', '%g', '%g','%d','%d','%g','%g', '%s')" % \
                     (self.exp_metadata_id, self.file_id, self.fileInfos[0]['vgap'], float(self.fileInfos[0]['current']),
                      self.fileInfos[0]['xres'], self.fileInfos[0]['yres'], self.fileInfos[0]['xinc'],
                      self.fileInfos[0]['yinc'], self.fileInfos[0]['unitxy'])

        if DEBUG:
            print self.query

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit insertion to database
            self.db.commit()
        except:
            # If unsuccessful roll back the database
            self.db.rollback()
            raise DatabaseEntryError, 'Unable to add %s into stm_topo_metadata within %s' % \
                                      (self.stm_fileName, self.database)

        if DEBUG:
            print 'filename: %s added into stm_topo_metadata within %s.' % (self.stm_fileName, self.database)
        # Close connection to database
        self.db.close()

    def safeAdd_stm_topo_metadata(self):
        """ This function uses check_stmTopoMetadataExist to see if an entry already exists with the same fileName
        as stm_file. If there is no such entry it is inserted into the database, however if it is an error is raised"""

        # Check to see if an entry with fileName exists
        if self.check_stmTopoMetadataExist():
            print 'stm_topo_metadata in %s already contains entry with fileName: %s' % \
                  (self.database, self.stm_fileName)
        else:
            # If not add entry
            self.add_stm_topo_metadata()

    def delete_stm_topo_metadata(self):
        """ Can be used to delete an entry with the same stm_fileName as stm_file from stm_topo_metadata in database"""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL commnad to get stm_file_id from stm_files
        self.query = "SELECT file_id FROM stm_files where file_name = '%s'" % self.stm_fileName

        try:
            self.cursor.execute(self.query)
            self.results = self.cursor.fetchall()
        except:
            raise UnableToFindEntryError, 'Unable to find entry in stm_files with filename: %s' % self.stm_fileName

        self.db.close()

        if len(self.results) > 1:
            raise DuplicateEntryError, 'A duplicate entry of %s has been found in stm_files' % self.stm_fileName
        elif len(self.results) == 1:
            self.file_id = self.results[0][0]
        else:
            raise UnableToFindEntryError, 'Unable to find a file_id for %s in stm_files' % self.stm_fileName

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query. Drop entry with same filename as stm_file.
        self.query = "DELETE FROM stm_topo_metadata WHERE file_id = '%d'" % self.file_id

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit changes to database
            self.db.commit()
            if DEBUG:
                print ('Entry with file name: %s removed from stm_topo_metadata in %s') % (self.stm_fileName,
                                                                                           self.database)
        except:
            self.db.rollback()
            raise DatabaseDeleteError, 'Entry with file name: %s could not be deleted from stm_topo_metadata in %s' % \
                                       (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

    """
    ************************************
    ***      stm_spec_metadata       ***
    ************************************
    """

    def check_stmSpecMetadataExist(self):
        """ Each stm_topo_metadata entry should link to a single stm_file entry ad single exp_metadata entry. It can
         then be referenced by multiple stm_topo_data entries.
         First the file_id of the appropriate stm_files entry is found by looking for the file_name.
            If no file_id can be found raise an error.
         Once we have the file_id we can check stm_spec_metadata for entries linked to this file_id.
         If there are none then return False, if there is an entry return True."""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query to return the file_id of all entries that have a fileName equal to the current flatfile
        self.query = "SELECT file_id FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        if DEBUG:
            print self.query

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Fetch all the rows in a list of lists.
            self.results = self.cursor.fetchall()
        except:
            # If fail close connection and end here.
            self.db.close()
            raise UnableToFindEntryError, 'Could not find %s in stm_files within %s.' % \
                                          (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

        if len(self.results) > 1:
            raise DuplicateEntryError, 'Duplicate entry found in stm_files within %s with file name: %s' % \
                                       (self.database, self.stm_fileName)
        elif len(self.results) == 1:
            if self.results[0] == None:
                raise UnableToFindEntryError, 'Was unable to find %s in stm_files within %d' % \
                                              (self.stm_fileName, self.database)
            else:
                self.stm_file_id = self.results[0][0]

        else:
            raise UnableToFindEntryError, 'Could not find %s in stm_files within %s.' % \
                                          (self.stm_fileName, self.database)

        # Connect to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL command to return stm_topo_metadata_id associated with stm_file_id of current stm_file
        self.query = "SELECT spec_metadata_id FROM stm_spec_metadata WHERE file_id = '%d'" % self.stm_file_id

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            self.results = self.cursor.fetchall()

        except:
            # If no results returned
            self.db.close()
            raise DatabaseConnectionError, 'could not connect to stm_spec_metadata within %s' % self.database

        if len(self.results) > 1:
            raise DuplicateEntryError, 'Duplicate entry found in stm_spec_metadata within %s with file_id %s' % \
                                           (self.database, self.stm_file_id)
        elif len(self.results) == 1:
            self.stm_spec_metadata_id = self.results[0][0]
            self.db.close()
            return True

    def add_stm_spec_metadata(self):
        """
        Each stm_file (that is a spectroscopy) entry should be associated to a single entry within stm_spec_metadata that
        can then be associated to multiple stm_spec_data entries.
        Here we must find the stm_file_id and exp_metadata_id associated with the stm_spec_metadata of the current
        stm_file.
        """
        # Open connection to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to return the exp_metadata_id and stm_file_id
        self.query = "SELECT * FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        if DEBUG:
            print(self.query)

        try:
            # Execute SQL command.
            self.cursor.execute(self.query)
            # Return all results from query.
            self.results = self.cursor.fetchall()
        except:
            # If not able to connect
            raise DatabaseConnectionError, 'Unable to connect to stm_files within %s' % self.database
        # Close connection to database
        self.db.close()

        # There should only be a single entry for a given filename.
        if len(self.results) > 1:
            raise DuplicateEntryError, 'There are multiple entries in stm_files within %s with the filename: %s' % \
                                        (self.database, self.stm_fileName)
        elif len(self.results) == 1:
            self.file_id = self.results[0][0]
            self.exp_metadata_id = self.results[0][1]
        else:
            raise UnableToFindEntryError, 'Unable to find entry in stm_files within %s with filename: %s' % \
                                            (self.database, self.stm_fileName)
        # FIXME: It is probably redundant to close then re-open database connection.

        # Open connection to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to add entry to stm_topo_metadata
        self.query = "INSERT INTO stm_spec_metadata(exp_metadata_id, file_id, v_gap, v_start, i_set, v_res, v_inc, v_unit, x_offset, y_offset) VALUES ('%d', '%d', '%g', '%g', '%g', '%d', '%g', '%s', '%g', '%g')" % \
                     (self.exp_metadata_id, self.file_id, self.fileInfos[0]['vgap'], self.fileInfos[0]['vstart'],
                      float(self.fileInfos[0]['current']), self.fileInfos[0]['vres'], self.fileInfos[0]['vinc'],
                      self.fileInfos[0]['unitv'], self.fileinfos[0]['offset'][0][0], self.fileinfos[0]['offset'][0][1])

        if DEBUG:
            print self.query

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit insertion to database
            self.db.commit()
        except:
            # If unsuccessful roll back the database
            self.db.rollback()
            raise DatabaseEntryError, 'Unable to add %s into stm_spec_metadata within %s' % \
                                      (self.stm_fileName, self.database)

        if DEBUG:
            print 'filename: %s added into stm_spec_metadata within %s.' % (self.stm_fileName, self.database)
        # Close connection to database
        self.db.close()

    def safeAdd_stm_spec_metadata(self):
        """ This function uses check_stmSpecMetadataExist to see if an entry already exists with the same fileName
        as stm_file. If there is no such entry it is inserted into the database, however if it is an error is raised"""

        # Check to see if an entry with fileName exists
        if self.check_stmTopoMetadataExist():
            print 'stm_topo_metadata in %s already contains entry with fileName: %s' % \
                  (self.database, self.stm_fileName)
        else:
            # If not add entry
            self.add_stm_spec_metadata()

    def delete_stm_spec_metadata(self):
        """ Can be used to delete an entry with the same stm_fileName as stm_file from stm_topo_metadata in database"""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL commnad to get stm_file_id from stm_files
        self.query = "SELECT file_id FROM stm_files where file_name = '%s'" % self.stm_fileName

        try:
            self.cursor.execute(self.query)
            self.results = self.cursor.fetchall()
        except:
            raise UnableToFindEntryError, 'Unable to find entry in stm_files with filename: %s' % self.stm_fileName

        self.db.close()

        if len(self.results) > 1:
            raise DuplicateEntryError, 'A duplicate entry of %s has been found in stm_files' % self.stm_fileName
        elif len(self.results) == 1:
            self.file_id = self.results[0][0]
        else:
            raise UnableToFindEntryError, 'Unable to find a file_id for %s in stm_files' % self.stm_fileName

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query. Drop entry with same filename as stm_file.
        self.query = "DELETE FROM stm_spec_metadata WHERE file_id = '%d'" % self.file_id

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit changes to database
            self.db.commit()
            if DEBUG:
                print ('Entry with file name: %s removed from stm_spec_metadata in %s') % (self.stm_fileName,
                                                                                           self.database)
        except:
            self.db.rollback()
            raise DatabaseDeleteError, 'Entry with file name: %s could not be deleted from stm_spec_metadata in %s' % \
                                       (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

    """
    ************************************
    ***      stm_cits_metadata       ***
    ************************************
    """

    def check_stmCitsMetadataExist(self):
        """ Each stm_tcits_metadata entry should link to a single stm_file entry and a single exp_metadata entry. It can
         then be referenced by multiple stm_cits_data entries.
         First the file_id of the appropriate stm_files entry is found by looking for the file_name.
            If no file_id can be found raise an error.
         Once we have the file_id we can check stm_spec_metadata for entries linked to this file_id.
         If there are none then return False, if there is an entry return True."""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query to return the file_id of all entries that have a fileName equal to the current flatfile
        self.query = "SELECT file_id FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        if DEBUG:
            print self.query

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Fetch all the rows in a list of lists.
            self.results = self.cursor.fetchall()
        except:
            # If fail close connection and end here.
            self.db.close()
            raise UnableToFindEntryError, 'Could not find %s in stm_files within %s.' % \
                                          (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()

        if len(self.results) > 1:
            raise DuplicateEntryError, 'Duplicate entry found in stm_files within %s with file name: %s' % \
                                       (self.database, self.stm_fileName)
        elif len(self.results) == 1:
            if self.results[0] == None:
                raise UnableToFindEntryError, 'Was unable to find %s in stm_files within %d' % \
                                              (self.stm_fileName, self.database)
            else:
                self.stm_file_id = self.results[0][0]

        else:
            raise UnableToFindEntryError, 'Could not find %s in stm_files within %s.' % \
                                          (self.stm_fileName, self.database)

        # Connect to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object using cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL command to return stm_topo_metadata_id associated with stm_file_id of current stm_file
        self.query = "SELECT cits_metadata_id FROM stm_cits_metadata WHERE file_id = '%d'" % self.stm_file_id

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            self.results = self.cursor.fetchall()

        except:
            # If no results returned
            self.db.close()
            raise DatabaseConnectionError, 'could not connect to stm_cits_metadata within %s' % self.database

        if len(self.results) > 1:
            raise DuplicateEntryError, 'Duplicate entry found in stm_cits_metadata within %s with file_id %s' % \
                                           (self.database, self.stm_file_id)
        elif len(self.results) == 1:
            self.stm_spec_metadata_id = self.results[0][0]
            self.db.close()
            return True

    def add_stm_cits_metadata(self):
        """
        Each stm_file (that is a cits) entry should be associated to a single entry within stm_cits_metadata that
        can then be associated to multiple stm_cits_data entries.
        Here we must find the stm_file_id and exp_metadata_id associated with the stm_cits_metadata of the current
        stm_file.
        """
        # Open connection to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to return the exp_metadata_id and stm_file_id
        self.query = "SELECT * FROM stm_files WHERE file_name = '%s'" % self.stm_fileName

        if DEBUG:
            print(self.query)

        try:
            # Execute SQL command.
            self.cursor.execute(self.query)
            # Return all results from query.
            self.results = self.cursor.fetchall()
        except:
            # If not able to connect
            raise DatabaseConnectionError, 'Unable to connect to stm_files within %s' % self.database
        # Close connection to database
        self.db.close()

        # There should only be a single entry for a given filename.
        if len(self.results) > 1:
            raise DuplicateEntryError, 'There are multiple entries in stm_files within %s with the filename: %s' % \
                                        (self.database, self.stm_fileName)
        elif len(self.results) == 1:
            self.file_id = self.results[0][0]
            self.exp_metadata_id = self.results[0][1]
        else:
            raise UnableToFindEntryError, 'Unable to find entry in stm_files within %s with filename: %s' % \
                                            (self.database, self.stm_fileName)
        # FIXME: It is probably redundant to close then re-open database connection.

        # Open connection to database
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL command to add entry to stm_topo_metadata
        self.query = "INSERT INTO stm_cits_metadata(exp_metadata_id, file_id, v_gap, i_set, x_res, y_res, x_inc, y_inc, xy_unit, v_start, v_res, v_inc, v_unit) VALUES ('%d', '%d', '%g', '%g', '%d', '%d', '%g', '%g', '%s', '%g', '%d', '%g', '%s', )" % \
                     (self.exp_metadata_id, self.file_id, self.fileInfos[0]['vgap'],
                      float(self.fileInfos[0]['current']), self.fileInfos[0]['xres'], self.fileInfos[0]['yres'],
                      self.fileInfos[0]['xinc'], self.fileInfos[0]['yinc'], self.fileInfos[0]['unitxy'],
                      self.fileInfos[0]['vstart'], self.fileinfos[0]['vres'], self.fileinfos[0]['vinc'],
                      self.fileInfos[0]['unitv'])

        if DEBUG:
            print self.query

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit insertion to database
            self.db.commit()
        except:
            # If unsuccessful roll back the database
            self.db.rollback()
            raise DatabaseEntryError, 'Unable to add %s into stm_cits_metadata within %s' % \
                                      (self.stm_fileName, self.database)

        if DEBUG:
            print 'filename: %s added into stm_cits_metadata within %s.' % (self.stm_fileName, self.database)
        # Close connection to database
        self.db.close()

    def safeAdd_stm_cits_metadata(self):
        """ This function uses check_stmCitsMetadataExist to see if an entry already exists with the same fileName
        as stm_file. If there is no such entry it is inserted into the database, however if it is an error is raised"""

        # Check to see if an entry with fileName exists
        if self.check_stmCitsMetadataExist():
            print 'stm_cits_metadata in %s already contains entry with fileName: %s' % \
                  (self.database, self.stm_fileName)
        else:
            # If not add entry
            self.add_stm_cits_metadata()

    def delete_stm_cits_metadata(self):
        """ Can be used to delete an entry with the same stm_fileName as stm_file from stm_cits_metadata in database"""

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method.
        self.cursor = self.db.cursor()

        # Prepare SQL commnad to get stm_file_id from stm_files
        self.query = "SELECT file_id FROM stm_files where file_name = '%s'" % self.stm_fileName

        try:
            self.cursor.execute(self.query)
            self.results = self.cursor.fetchall()
        except:
            raise UnableToFindEntryError, 'Unable to find entry in stm_files with filename: %s' % self.stm_fileName

        self.db.close()

        if len(self.results) > 1:
            raise DuplicateEntryError, 'A duplicate entry of %s has been found in stm_files' % self.stm_fileName
        elif len(self.results) == 1:
            self.file_id = self.results[0][0]
        else:
            raise UnableToFindEntryError, 'Unable to find a file_id for %s in stm_files' % self.stm_fileName

        # Open database connection
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        # Prepare cursor object with cursor() method
        self.cursor = self.db.cursor()

        # Prepare SQL query. Drop entry with same filename as stm_file.
        self.query = "DELETE FROM stm_cits_metadata WHERE file_id = '%d'" % self.file_id

        try:
            # Execute SQL command
            self.cursor.execute(self.query)
            # Commit changes to database
            self.db.commit()
            if DEBUG:
                print ('Entry with file name: %s removed from stm_cits_metadata in %s') % (self.stm_fileName,
                                                                                           self.database)
        except:
            self.db.rollback()
            raise DatabaseDeleteError, 'Entry with file name: %s could not be deleted from stm_cits_metadata in %s' % \
                                       (self.stm_fileName, self.database)
        # Close database connection
        self.db.close()