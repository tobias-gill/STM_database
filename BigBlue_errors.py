__author__ = 'Tobias Gill'

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

class MySQL_Error(Error):
    pass
