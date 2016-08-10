import getpass


def user_dir():
    """ This function returns the relavant file directories and passwords for
    the current system user in a dictionary. """

    if getpass.getuser() == 'joshs':

        load_tables = 'C:/Users/joshs/Programming/PycharmProjects/Git_Sync/' \
                      'pySecMaster/pySecMaster/load_tables'

        # PostgreSQL default database information
        main_db = 'postgres'
        main_user = 'postgres'
        main_password = 'password'
        main_host = 'localhost'
        main_port = '5432'

        # PostgreSQL pysecmaster database information
        pysecmaster_db = 'pysecmaster'
        pysecmaster_user = 'postgres'
        pysecmaster_password = 'password'
        pysecmaster_host = 'localhost'
        pysecmaster_port = '5432'

        # PostgreSQL pysecmaster TEST database information
        pysecmaster_test_db = 'pysecmaster_test'
        pysecmaster_test_user = 'test'
        pysecmaster_test_password = 'password'
        pysecmaster_test_host = 'localhost'
        pysecmaster_test_port = '5432'

        # PostgreSQL sayvmaster transactions database information
        sayvmaster_db = 'sayvmaster'
        sayvmaster_user = 'postgres'
        sayvmaster_password = 'postgres'
        sayvmaster_host = 'localhost'
        sayvmaster_port = '5432'

        # Quandl information
        quandl_token = 'XXXXXXXXXXXX'

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
                 'pysecmaster_test_db': pysecmaster_test_db,
                 'pysecmaster_test_user': pysecmaster_test_user,
                 'pysecmaster_test_password': pysecmaster_test_password,
                 'pysecmaster_test_host': pysecmaster_test_host,
                 'pysecmaster_test_port': pysecmaster_test_port,
                 'sayvmaster_db': sayvmaster_db,
                 'sayvmaster_user': sayvmaster_user,
                 'sayvmaster_password': sayvmaster_password,
                 'sayvmaster_host': sayvmaster_host,
                 'sayvmaster_port': sayvmaster_port,
                 },
            'quandl':
                {'quandl_token': quandl_token},
            }
