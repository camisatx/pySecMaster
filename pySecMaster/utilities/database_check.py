import psycopg2


def postgres_test(database_options):
    """Test the connection to the postgres database.

    :param database_options: Dictionary with database parameters
    :return: Boolean indicating whether the database connection was successful
    """

    host = database_options['host']
    port = database_options['port']
    database = database_options['database']
    user = database_options['user']
    password = database_options['password']
    try:
        conn = psycopg2.connect(host=host, port=port, database=database,
                                user=user, password=password)
        conn.close()
        return True
    except psycopg2.Error as e:
        print(e)
        return False
