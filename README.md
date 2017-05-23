# validate-sql-backups
Powershell script to restore and validate SQL backup files

If SQL Backups are not tested, its possible the file is written but it may not be valid.   To ensure the files are healthy the backups should be restored and verified on a regular basis. 

By setting a few parameters in this script it will scan all sub folders and restore the most recent backup file found in a given directory.  Once restored it runs DBCC CheckDB against the database and then deletes it, logging success or failure with any provided error messages.  This will automatically pick up any new databases as long as they are put into their own sub folder.
