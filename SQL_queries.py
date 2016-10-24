__author__ = 'Tobias Gill'
'''
Title: SQL Queries

Description: A set of classes that are used to extract standard query information from an SQL database containing stm
data in the format of BigBlue().

Change Log:
2016-04-15: First Version

'''
import MySQLdb

class Error(Exception):
    '''Default error class'''
    pass


class DatabaseError(Error):
    '''Undefined database error'''
    pass


class sqlQuery(object):
    """
    Parent for all queries
    """

    def __init__(self, username, password, database='cryo_stm_data'):

        self.user = username  # SQL database username
        self.password = password  # SQL database password
        self.database = database  # SQL database. Default is cryo for testing
        self.host = 'localhost'  # Should always be 'localhost' as users will ssh into server.

        self.queryDef()  # Defines the SQL query to be passed to execute.
        self.connect()  # Opens a connection to the database.
        self.execute()  # Executes the SQL commands defined by queryDef().
        self.close_connection()  # Closes the database connection.

    def queryDef(self):
        self.query = ''  # Parent class has no query. This method is overridden by child classes.

    def queryConstructor(self, queryDictionary, tableName):
        """ constructs an SQL query based on a dictionary of input parameters

        :param queryDictionary: A python dictionary that contains elements that correspond to values in a database.
        :param tableName: The name of the table in the database which is to be queried.
        :return: Noting is returned by this function. However the variable query is defined.
        """
        self.query = "SELECT * FROM %s WHERE " % tableName  # Common initial query components.
        self.singleCondition = True  # Initially define that there has not been any clauses added.
        for i in queryDictionary:  # Iterates through the variables in queryDictionary
            if queryDictionary[i] is not None:  # If no clause parameter given, pass.
                if self.singleCondition == True:  # If clause given, check to see if it is the first.
                    # If value is a float both values must be rounded otherwise they will ever so slightly differ and
                    # no results will be returned.
                    if type(queryDictionary[i]) == float:
                        # Add clause to query.
                        self.query = self.query + "ROUND(" + i + ") " + "= ROUND('%f')" % queryDictionary[i]
                    elif type(queryDictionary[i]) == str:
                        self.query = self.query + i + " = '%s'" % str(queryDictionary[i])  # Add clause to query.
                    else:
                        self.query = self.query + i + " = %s" % str(queryDictionary[i])  # Add clause to query.
                    self.singleCondition = False  # First clause has been added.
                elif self.singleCondition == False:  # If not a single clause query.
                    if type(queryDictionary[i]) == float:
                        # Add clause to query.
                        self.query = self.query + " AND " + "ROUND(" + i + ") " + "= ROUND('%f')" % queryDictionary[i]
                    elif type(queryDictionary[i]) == str:
                        self.query = self.query + i + " = '%s'" % str(queryDictionary[i])  # Add clause to query.
                    else:
                        # Add clause to query.
                        self.query = self.query + " AND " + i + " = %s" % str(queryDictionary[i])

    def connect(self):
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.database)   # opens database connection
        self.cursor = self.db.cursor()  # Creates cursor object.

    def execute(self):
        try:
            self.cursor.execute(self.query)  # Attempts to run the commands in query.
            self.result = self.cursor.fetchall()  # Returns all results found by query.
        except:
            # If exectute fails an error is raised. Unfortunately it is unknown if there is a connection error or that
            # the query command is incorrectly formatted.
            raise DatabaseError, "There has been an error trying to execute: '%s'. \
            If query is formatted correctly you may unable to connect to the database: '%s'" % \
                                 (self.query, self.database)

    def close_connection(self):
        self.db.close()  # Closes database connection


class allExperiments(sqlQuery):

    def queryDef(self):
        self.query = 'SELECT * FROM exp_metadata'  # A simplistic query that returns all metadata for all experiments.


class experimentMetadata(sqlQuery):

    def __init__(self, username, password, database='cryo_stm_data',
                 exp_metadata_id=None, exp_timestamp=None, exp_users=None, exp_substrate=None, exp_adsorbate=None,
                 exp_prep=None, exp_notebook=None, exp_notes=None):

        self.tableName = 'exp_metadata'  # Name of the SQL database table to queried.
        self.exp_metadata = {  # Dictionary of possible search parameters that are set by keywords.
            "exp_metadata_id": exp_metadata_id,
            "exp_timestamp": exp_timestamp,
            "exp_users": exp_users,
            "exp_substrate": exp_substrate,
            "exp_adsorbate": exp_adsorbate,
            "exp_prep": exp_prep,
            "exp_notebook": exp_notebook,
            "notes": exp_notes
        }

        # Ensures __init__ methods from parent are inherited.
        super(experimentMetadata, self).__init__(username, password, database)

    def queryDef(self):

        # Constructs SQL query from exp_metadata
        self.queryConstructor(self.exp_metadata, self.tableName)


class stmFiles(sqlQuery):

    def __init__(self, username, password, database='cryo_stm_data',
                 file_id=None, exp_metadata_id=None, file_name=None, file_date=None, file_type=None,
                 file_location=None):

        self.tableName = 'stm_files'  # Name of the SQL database table to queried.
        self.stm_files = {  # Dictionary of possible search parameters that are set by keywords.
            "file_id": file_id,
            "exp_metadata_id": exp_metadata_id,
            "file_name": file_name,
            "file_date": file_date,
            "file_type": file_type,
            "file_location": file_location
        }

        # Ensures __init__ methods from parent are inherited.
        super(stmFiles, self).__init__(username, password, database)

    def queryDef(self):

        # Constructs SQL query from exp_metadata
        self.queryConstructor(self.stm_files, self.tableName)


class topoMetadata(sqlQuery):

    def __init__(self, username, password, database='cryo_stm_data',
                 topo_metadata_id=None, exp_metadata_id=None, file_id=None, v_gap=None, i_set=None, x_res=None,
                 y_res=None, x_inc=None, y_inc=None, xy_unit=None, lockin_measurement=None):

        self.tableName = 'stm_topo_metadata'  # Name of the SQL database table to queried.
        self.stm_topo_metadata = {  # Dictionary of possible search parameters that are set by keywords.
            "topo_metadata_id": topo_metadata_id,
            "exp_metadata_id": exp_metadata_id,
            "file_id": file_id,
            "v_gap": v_gap,
            "i_set": i_set,
            "x_res": x_res,
            "y_res": y_res,
            "x_inc": x_inc,
            "y_inc": y_inc,
            "xy_unit": xy_unit,
            "lockin_measurement": lockin_measurement
        }

        # Ensures __init__ methods from parent are inherited.
        super(topoMetadata, self).__init__(username, password, database)

    def queryDef(self):

        # Constructs SQL query from stm_topo_metadata
        self.queryConstructor(self.stm_topo_metadata, self.tableName)


class specMetadata(sqlQuery):

    def __init__(self, username, password, database='cryo_stm_data',
                 spec_metadata_id=None, exp_metadata_id=None, file_id=None, topo_metadata_id=None, object=None,
                 v_gap=None, v_start=None, i_set=None, v_res=None, v_inc=None, v_unit=None, xy_offset=None,
                 lockin_measurement=None):

        self.tableName = 'stm_spec_metadata'  # Name of the SQL database table to queried.
        self.stm_spec_metadata = {  # Dictionary of possible search parameters that are set by keywords.
            "spec_metadata_id": spec_metadata_id,
            "exp_metadata_id": exp_metadata_id,
            "file_id": file_id,
            "topo_metadata_id": topo_metadata_id,
            "object": object,
            "v_gap": v_gap,
            "v_start": v_start,
            "i_set": i_set,
            "v_res": v_res,
            "v_inc": v_inc,
            "v_unit": v_unit,
            "xy_offset": xy_offset,
            "lockin_measurement": lockin_measurement
        }

        # Ensures __init__ methods from parent are inherited.
        super(specMetadata, self).__init__(username, password, database)

    def queryDef(self):

        # Constructs SQL query from stm_spec_metadata
        self.queryConstructor(self.stm_spec_metadata, self.tableName)


class citsMetadata(sqlQuery):

    def __init__(self, username, password, database='cryo_stm_data',
                 cits_metadata_id=None, exp_metadata_id=None, file_id=None, topo_metadata_id=None, object=None,
                 v_gap=None, i_set=None, x_res=None, y_res=None, x_inc=None, y_inc=None, xy_unit=None, v_start=None,
                 v_res=None, v_inc=None, v_unit=None, lockin_measurement=None):

        self.tableName = 'stm_cits_metadata'  # Name of the SQL database table to queried.
        self.stm_cits_metadata = {  # Dictionary of possible search parameters that are set by keywords.
            "cits_metadata_id": cits_metadata_id,
            "exp_metadata_id": exp_metadata_id,
            "file_id": file_id,
            "topo_metadata_id": topo_metadata_id,
            "object": object,
            "v_gap": v_gap,
            "i_set": i_set,
            "x_res": x_res,
            "y_res": y_res,
            "x_inc": x_inc,
            "y_inc": y_inc,
            "xy_unit": xy_unit,
            "v_start": v_start,
            "v_res": v_res,
            "v_inc": v_inc,
            "v_unit": v_unit,
            "lockin_measurement": lockin_measurement
        }

        # Ensures __init__ methods from parent are inherited.
        super(citsMetadata, self).__init__(username, password, database)

    def queryDef(self):

        # Constructs SQL query from stm_cits_metadata
        self.queryConstructor(self.stm_cits_metadata, self.tableName)


class lockinMetadata(sqlQuery):

    def __init__(self, username, password, database='cryo_stm_data',
                 lockin_metadata_id=None, exp_metadata_id=None, file_id=None, spec_metadata_id=None,
                 cits_metadata_id=None, v_mod=None, v_sen=None, t_meas=None, frequency=None, phase=None, harmonic=None):

        self.tableName = 'lockin_metadata'  # Name of the SQL database table to queried.
        self.lockin_metadata = {  # Dictionary of possible search parameters that are set by keywords.
            "lockin_metadata_id": lockin_metadata_id,
            "exp_metadata_id": exp_metadata_id,
            "file_id": file_id,
            "spec_metadata_id": spec_metadata_id,
            "cits_metadata_id": cits_metadata_id,
            "v_mod": v_mod,
            "v_sen": v_sen,
            "t_meas": t_meas,
            "frequency": frequency,
            "phase": phase,
            "harmonic": harmonic
        }

        # Ensures __init__ methods from parent are inherited.
        super(lockinMetadata, self).__init__(username, password, database)

    def queryDef(self):

        # Constructs SQL query from lockin_metadata
        self.queryConstructor(self.lockin_metadata, self.tableName)

#class listQuery(sqlQuery):
#
#    def __init__(self, username, password, database='cyro_stm_database',
#                 list, table, row):
#
#        self.queryList = list
#        self.tableName = table
#        self.rowName = row
#
#        super(listQuery, self).__init__(username, password, database)

    #def queryDef(self):

#        if type(list[0]) == str:
#            self.query = "SELECT * FROM %s WHERE %s = '%s'" % (self.tableName, self.rowName, self.queryList[i])
