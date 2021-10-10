#
# dataset migration to the database
#

from db_connection import connect_db

def migrate() -> None:
    print(connect_db())

if __name__ == "__main__":
    migrate()