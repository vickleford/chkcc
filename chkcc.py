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


def _read_config(filename, section_names=['auth_endpoints', 'internal_services']):
    '''Return a dict of a config file's contents.'''
    
    with open(filename) as f:
        config = yaml.load(f)
        
    return config
    
    
def _check_memcache(memcache_servers):
    '''Return True after connecting to memcached, setting and deleting a key.'''

    try:
        mc = memcache.Client(memcache_servers, debug=0)
    
        mc.set("health_check", time.time())
        value = mc.get("health_check")
        mc.delete("health_check")
    
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
        db_conn = MySQLdb.connect(**params)
        print db_conn
        cur = db_conn.cursor()
        success = cur.execute("select 1")
    
        return True
        
    except MySQLdb.Error, e:
        print "Check blew up! %s" % e
        
        return False
        
    finally:
        print db_conn
        cur.close()
        db_conn.close()


def _check_pgsql(host, user, passwd, db, test_query="select 1"):
    """Return True after testing pgsql database and query."""

    # out of order to be consistent with connect_mysql args
    conn_str = "host='%s' dbname='%s' user='%s' password='%s'" % (host, db, user, passwd)
    
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(test_query)
        
        return True
        
    except Exception, e:
        print "Check blew up! %s" % e
        
        return False
        
    finally:
        cursor.close()
        conn.close()

    
def check_databases(config, section_name='databases'):
    '''Health check databaes found in config dict.'''
    
    # just going to hardcode the types of dbs for now, fuck it
    
    mysql_dbs = ['cloud_control']
    pgsql_dbs = ['jprov', 'hostinagmatrix']
    
    for database in config[section_name].keys():
        try:
            params = { 'host': config[section_name][database]['endpoint'], 
                       'user': config[section_name][database]['username'],
                       'passwd': config[section_name][database]['password'],
                       'db': config[section_name][database]['db_name'] }
        except KeyError:
            # not all database are really databases
            params = {}
                   
        print "Checking database %s... " % database,
        start_time = time.time()
        
        if database in mysql_dbs:
            ok = _check_mysql(**params)
        elif database in pgsql_dbs:
            ok = _check_pgsql(**params)
        elif database in 'memcached':
            _check_memcache(config[section_name][database]['servers'])
        elif database in 'elastic_search':
            ok = False
        
        end_time = time.time()
        check_elapsed = end_time - start_time
        
        if ok:
            print "OK in %fs" % check_elapsed
        else:
            print "FAILED in %fs" % check_elapsed
            
    
def check_apis(config):
    '''Health check restful APIs found in config dict'''
    
    pass

        
if __name__ == '__main__':
    
    config_file = sys.argv[1]
    config = _read_config(config_file)
    
    check_apis(config)
    
    check_databases(config)