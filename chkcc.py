#!/usr/bin/env python

'''

Dependencies
- PyYaml
- psycopg2
- MySQLdb
'''

import sys
import time
import urllib2

import yaml
import psycopg2
import MySQLdb
import memcache


def _read_config(filename):
    '''Return a dict of a config file's contents.'''
    
    with open(filename) as f:
        config = yaml.load(f)
        
    return config
    
    
def _print_header(title):
    print "{0:^80}".format(title)
    
    
def _print_result(status, start_time, end_time, width=80, alignment='>'):
    """Print whether a check passed or failed.

    status is a boolean
    start_time is a time.time() value
    end_time is a time.time() value
    width is a decimal integer controlling how wide to print
    alignment is a string ">" (right), "<" (left) or "^" (center) 
        which aligns text output

    """

    check_elapsed = end_time - start_time

    if status:
        message = "OK (%fs)" % check_elapsed
    else:
        message = "FAILED (%fs)" % check_elapsed

    control = "{{:{align}{width}}}".format(align=alignment, width=width)
    print control.format(message)
    
    
def _check_memcache(memcache_servers):
    '''Return True after connecting to memcached, setting and deleting a key.'''

    try:
        mc = memcache.Client(memcache_servers, debug=0)
    
        test_value = time.time()
        set_success = mc.set("health_check", test_value)
        if set_success is not True:
            print "Could not set a key!"
            return False
            
        get_success = mc.get("health_check")
        if get_success != test_value:
            print "Got a value for key that was not what we set!"
            return False
            
        delete_success = mc.delete("health_check")
        if delete_success != 1:
            print "Could not delete test key!"
            return False
    
        return True
        
    except Exception, e:
        print "Memcached check failed: %s" % e
        
        return False
    
    finally:
        mc.disconnect_all()
    

def _check_mysql(host, user, passwd, db, test_query="select 1"):
    """Return True after testing mysql database and query."""

    params = { 'host': host,
               'user': user,
               'passwd': passwd,
               'db': db }
   
    try:
        db_conn = None
        db_conn = MySQLdb.connect(**params)
        cur = db_conn.cursor()
        success = cur.execute("select 1")
        return True
    except MySQLdb.Error, e:
        print "%s" % e
        return False
    except _mysql_exceptions.OperationalError:
        print "%s" % e
        return False
    finally:
        if db_conn is not None:
            db_conn.close()


def _check_pgsql(host, user, passwd, db, test_query="select 1"):
    """Return True after testing pgsql database and query."""

    # out of order to be consistent with connect_mysql args
    conn_str = "host='%s' dbname='%s' user='%s' password='%s'" % (host, db, user, passwd)
    
    try:
        conn = None
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(test_query)
        
        records = cursor.fetchall()
        
        return True
        
    except Exception, e:
        print "%s" % e
        
        return False
        
    finally:
        if conn is not None:
            conn.close()

    
def check_databases(config, section_name='databases'):
    '''Health check databaes found in config dict.'''
    
    _print_header("DATABASES")
    
    # just going to hardcode the types of dbs for now, fuck it
    
    mysql_dbs = ['cloud_control']
    pgsql_dbs = ['jprov', 'hostingmatrix']
    
    for database in config[section_name].keys():
        try:
            params = { 'host': config[section_name][database]['endpoint'], 
                       'user': config[section_name][database]['username'],
                       'passwd': config[section_name][database]['password'],
                       'db': config[section_name][database]['db_name'] }
        except KeyError:
            # not all database are really databases
            params = {}
                   
        print "Checking %s... " % database
        start_time = time.time()
        
        if database in mysql_dbs:
            ok = _check_mysql(**params)
        elif database in pgsql_dbs:
            ok = _check_pgsql(**params)
        elif database in 'memcached':
            ok = _check_memcache(config[section_name][database]['servers'])
        elif database in 'elastic_search':
            ok = False
        
        end_time = time.time()

        _print_result(ok, start_time, end_time)
           
           
def _check_api(url, timeout=5, fail_status=500):
    "Return True if a HTTP status code is less than fail_status."
    
    try:
        conn = None
        conn = urllib2.urlopen(url, timeout=timeout)
    except urllib2.HTTPError, e:
        if e.code < fail_status:
            print "Got a response (%d), but with error: %s" % (e.code, e.reason)
            return True
        else:
            print "%s" % e
    except urllib2.URLError, e:
        print "%s" % e
    else:
        # prevents OK messages from going out on the same line
        print
    finally:
        if conn:
            conn.close()
        
    if conn is not None and conn.code < fail_status:
        return True
    else:
        return False
        

def _check_edir():
    pass
        
    
def check_apis(config):
    '''Health check restful APIs found in config dict'''
    
    _print_header("INTERNAL SERVICES, AUTH")
    
    # maybe i should break this out into 2 diff funcs
    
    #
    # figure out which endpoints to check first
    #
    
    # major config sections for shortening
    auth_svcs = config['auth_endpoints']
    int_svcs = config['internal_services']
    
    # and because of the fuckin oneoffs like edir and global auth
    # it's just easier for reading to be explicit rather than auto-
    # mate some endpoint population here and there then do some others
    # manually.
    endpoints = { 'ga_int': auth_svcs['global_auth']['internal_endpoint'],
                  'ga_ext': auth_svcs['global_auth']['external_endpoint'],
                  'auth_v11': auth_svcs['auth_v11']['endpoint'],
                  'radar': int_svcs['radar_monitoring']['endpoint'],
                  'decrypt': int_svcs['decript_service']['endpoint'],
                  'valkyrie': int_svcs['valkyrie']['endpoint'],
                  'shack': int_svcs['shack']['endpoint'],
                  'monitor': int_svcs['monitor_service']['endpoint'],
                  'smix': int_svcs['servicemix']['endpoint'],
                  'rba': int_svcs['RBA']['endpoint'],
                  'elastic': config['databases']['elastic_search']['host'],
                 }
                  
    # fuckin' one-offs
    edir_endpoints = []
    for failover in int_svcs['edir']:
        edir_endpoints.append(failover['host'])
    
    #    
    # ok now we can start checking these endpoints
    #
    
    for endpoint in endpoints.iteritems():
        print "Checking %s..." % endpoint[0],
        
        start_time = time.time()
        
        ok = _check_api(endpoint[1])
        
        end_time = time.time()

        _print_result(ok, start_time, end_time)
        
    for endpoint in edir_endpoints:
        # how do we check ldap endpoints?
        pass
        
        
if __name__ == '__main__':
    
    config_file = sys.argv[1]
    config = _read_config(config_file)
    
    check_apis(config)
    
    check_databases(config)