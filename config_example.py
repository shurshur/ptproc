#!/usr/bin/python
# -*- coding:utf-8 -*-
# vim:shiftwidth=2:autoindent:et
# OSM Public Transport Processor
# See COPYING and AUTHORS for license info

# possible values: pgsql, osm-simple
pgtype = 'pgsql'

# PostGIS database with OSM data
pguser = None
pgpass = None
pgdata = 'public_transport'
pghost = None

# Target database for <ptprefix>_*. If ptdata=None, script will use pgdata instead. 
ptuser = None
ptpass = None
ptdata = None
pthost = None

# prefix for pt tables
ptprefix = "pt"

# only for pgtype=pgsql
prefix = "planet"

debug = 0
warns = 0
# check if route is valid (new routes only)
checkvalid = 0
# store new routes summary in pt_routes table
storeroutes = 0
# create geometry for routes in pt_routes table (requires storeroutes=1)
georoutes = 0

