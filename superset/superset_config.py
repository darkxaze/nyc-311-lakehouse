import os

SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'
WTF_CSRF_ENABLED = False
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'change_me')
SUPERSET_WEBSERVER_TIMEOUT = 300

# local dev config only. production would use postgresql for metadata and
# enable csrf.
