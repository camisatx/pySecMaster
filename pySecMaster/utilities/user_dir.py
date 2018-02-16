import getpass


def user_dir():
    """ This function returns the relavant file directories and passwords for
    the current system user in a dictionary. """

    if getpass.getuser() == 'root':
        # Docker container will use these variables

        load_tables = '/load_tables'

        # PostgreSQL default database information
        main_db = 'postgres'
        main_user = 'postgres'
        main_password = 'correct horse battery staple'          # Change this!!
        main_host = 'postgres_pysecmaster'          # the docker container name
        main_port = '5432'

        # PostgreSQL pysecmaster database information
        pysecmaster_db = 'pysecmaster'
        pysecmaster_user = 'pymaster'
        pysecmaster_password = 'correct horse battery staple'   # Change this!!
        pysecmaster_host = 'postgres_pysecmaster'   # the docker container name
        pysecmaster_port = '5432'

        # Quandl information
        quandl_token = 'XXXXXXXXX'       # Keep this secret!!

    elif getpass.getuser() == 'josh':
        # Local user will use thee variables

        load_tables = '/load_tables'

        # PostgreSQL default database information
        main_db = 'postgres'
        main_user = 'postgres'
        main_password = 'correct horse battery staple'          # Change this!!
        main_host = '127.0.0.1'
        main_port = '5432'

        # PostgreSQL pysecmaster database information
        pysecmaster_db = 'pysecmaster'
        pysecmaster_user = 'pymaster'
        pysecmaster_password = 'correct horse battery staple'   # Change this!!
        pysecmaster_host = '127.0.0.1'
        pysecmaster_port = '5432'

        # Quandl information
        quandl_token = 'XXXXXXXXX'       # Keep this secret!!

    else:
        raise NotImplementedError('Need to set data variables for user %s in '
                                  'pySecMaster/utilities/user_dir.py' %
                                  getpass.getuser())

    return {'load_tables': load_tables,
            'postgresql':
                {'main_db': main_db,
                 'main_user': main_user,
                 'main_password': main_password,
                 'main_host': main_host,
                 'main_port': main_port,
                 'pysecmaster_db': pysecmaster_db,
                 'pysecmaster_user': pysecmaster_user,
                 'pysecmaster_password': pysecmaster_password,
                 'pysecmaster_host': pysecmaster_host,
                 'pysecmaster_port': pysecmaster_port,
                 },
            'quandl':
                {'quandl_token': quandl_token},
            }
