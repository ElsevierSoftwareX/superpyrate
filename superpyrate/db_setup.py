"""Sets up the tables in a newly created database, ready for data ingest
"""
from pyrate.repositories.aisdb import AISdb
from superpyrate.pipeline import get_environment_variable

options = {}
options['host'] = get_environment_variable('DBHOSTNAME')
options['db'] = get_environment_variable('DBNAME')
options['user'] = get_environment_variable('DBUSER')
options['pass'] = get_environment_variable('DBUSERPASS')

db = AISdb(options)
with db:
    db.create()
