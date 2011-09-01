#!/usr/bin/python
# -*- coding:utf-8 -*-
# vim:shiftwidth=2:autoindent:et
# OSM Public Transport Processor
# See COPYING and AUTHORS for license info
import sys
reload(sys)
sys.setdefaultencoding("utf-8")          # a hack to support UTF-8 
from time import time
import re
import psycopg2
from psycopg2.extensions import adapt

try:
  import psyco
  psyco.full()
except ImportError:
  pass

# possible values: pgsql, osm-simple
pgtype = 'pgsql'

# PostGIS database with OSM data
pguser = None
pgpass = None
pgdata = 'public_transport'
pghost = None

# Target database for pt_*. If ptdata=None, script will use pgdata instead. 
ptuser = None
ptpass = None
ptdata = None
pthost = None

debug = 0
warns = 0
# check if route is valid (new routes only)
checkvalid = 0
# store new routes summary in pt_routes table
storeroutes = 0

route_types = ["bus", "trolleybus", "tram", "share_taxi"]
old_stop_roles = ["stop", "forward:stop", "backward:stop", "forward_stop", "backward_stop"]
new_stop_roles = ["stop", "stop_exit_only", "stop_entry_only" ]
new_platform_roles = ["platform", "platform_exit_only", "platform_entry_only" ]

# prefix for pt tables
ptprefix = "pt"

# only for pgtype=pgsql
prefix = "planet"

# сравнение двух номеров маршрутов
# сравниваются префиксы как числа, или как строки, а если равны -
# сравниваются остатки строк рекурсивно
def ptrefcmp(a, b):
  if a == b:
    return 0
  ma = re.match(r'^(\d+)(.*?)$', a)
  mb = re.match(r'^(\d+)(.*?)$', b)
  if ma and mb:
    pa = int(ma.group(1))
    pb = int(mb.group(1))
    if pa == pb:
      return ptrefcmp(ma.group(2), mb.group(2))
    else:
      return cmp(pa,pb)
  elif ma:
    return -1
  elif mb:
    return 1
  return cmp(a,b)

def sqlesc(value):
  adapted = adapt(value)
  if hasattr(adapted, 'getquoted'):
    adapted = adapted.getquoted()
  return adapted

def pgconn(_host,_user,_pass,_data):
  if not _data:
    return pgconn(pghost,pguser,pgpass,pgdata)
  conn = "dbname='%s'" % _data
  if _host:
    conn = conn + " host='%s'" % _host
  if _user:
    conn = conn + " user='%s'" % _user
  if _pass:
    conn = conn + " password='%s'" % _pass
  return psycopg2.connect(conn)

if pgtype == 'osm-simple':
  from psycopg2.extras import register_hstore

pg = pgconn(pghost,pguser,pgpass,pgdata)
cc=pg.cursor()
cc2=pg.cursor()
if pgtype == 'osm-simple':
  register_hstore(cc)

pg2=pgconn(pthost,ptuser,ptpass,ptdata)
cu=pg2.cursor()

refs = {}

tm=time()

for otype in ["node", "way"]:
  cu.execute("DELETE FROM %s_%ss" % (ptprefix, otype))

if storeroutes:
  cu.execute("DELETE FROM %s_routes" % (ptprefix))

# route masters
rm = {}

# счётчик route_master
count_rm = 0
# счётчик новых route как мемберов route_master
count_rn = 0

# сначала извлечём все route_master
if pgtype == 'pgsql':
  q="SELECT id,tags,members FROM %s_rels WHERE 'type=route_master'=ANY(tags2pairs(tags))" % prefix
elif pgtype == 'osm-simple':
  q="SELECT id,UNNEST(ARRAY_AGG(tags)) AS tags,ARRAY_AGG(LOWER(member_type)||member_id||':'||member_role) AS members FROM relations JOIN relation_members ON id=relation_id WHERE 'type'=>'route_master' <@ tags GROUP BY id"
else:
  raise ArgumentError("invalid pgtype")

cc.execute(q)

while True:
  row = cc.fetchone()
  if not row:
    break
  if pgtype == 'pgsql':
    id, _tags, members = row
    tags = {}
    for i in range(0,len(_tags)/2):
      tags[_tags[2*i]] = _tags[2*i+1]
  elif pgtype == 'osm-simple':
    id, tags, _members = row
    members = {}
    for i in range(0,len(_members)):
      mtmp = _members[i].split(":")
      mkey = mtmp[0]
      mrole = ":".join(mtmp[1:])
      members[2*i] = mkey
      members[2*i+1] = mrole

  try:
    rtype = tags["route_master"]
  except KeyError:
    continue
  if rtype not in route_types:
    continue

  if not members:
    if warns > 0:
      print "Warning: route_master relation %d has no members" % id
    continue
  
  try:
    ref = tags["ref"]
  except KeyError:
    continue

  if debug > 0:
    print "%d: new %s route_master %s" % (id,rtype,ref)

  count_rm = count_rm+1
  for i in range(0,len(members)/2):
    mkey = members[2*i]
    mid = int(mkey[1:])
    mtype = mkey[0]
    mrole = members[2*i+1]
    if mtype == "r":
      try:
        rm[mid]
      except KeyError:
        rm[mid] = {}
        rm[mid]["ref"] = ref
        rm[mid]["master"] = id
        count_rn = count_rn+1
        continue
      if id == rm[mid]["master"]:
        continue
      if warns > 0:
        print "Warning: route_master relation %d has member relation %d that owned by another relation %d" % (id, mid, rm[mid]["master"])
    else:
      if warns > 0:
        print "Warning: route_master relation %d has non-relation member %d" % (id, mid)
      continue

print "route_masters: %d relations with %d route members" % (count_rm, count_rn)

# счётчик новых route
count_r = 0
# счётчик старых route
count_o = 0

# извлекаем все route
if pgtype == 'pgsql':
  q = "SELECT id,tags,members FROM %s_rels WHERE 'type=route'=ANY(tags2pairs(tags))" % prefix
elif pgtype == 'osm-simple':
  # одним запросом получается неэффективно, поэтому members извлекаем отдельными запросами
  #q="SELECT id,UNNEST(ARRAY_AGG(tags)) AS tags,ARRAY_AGG(LOWER(member_type)||member_id||':'||member_role) AS members FROM relations JOIN relation_members ON id=relation_id WHERE 'type'=>'route' <@ tags GROUP BY id"
  q="SELECT id,UNNEST(ARRAY_AGG(tags)) AS tags FROM relations WHERE 'type'=>'route' <@ tags GROUP BY id"

cc.execute(q)

while True:
  row = cc.fetchone()
  if not row:
    break
  if pgtype == 'pgsql':
    id, _tags, members = row
    tags = {}
    for i in range(0,len(_tags)/2):
      tags[_tags[2*i]] = _tags[2*i+1]
  elif pgtype == 'osm-simple':
    id, tags = row
    cc2.execute("SELECT ARRAY_AGG(LOWER(member_type)||member_id||':'||member_role) AS members FROM relation_members WHERE relation_id=%d" % id)
    row = cc2.fetchone()
    if not row:
      continue
    _members, = row
    members = {}
    if not _members:
      _members = {}
    for i in range(0,len(_members)):
      mtmp = _members[i].split(":")
      mkey = mtmp[0]
      mrole = ":".join(mtmp[1:])
      members[2*i] = mkey
      members[2*i+1] = mrole

  # если route является частью какого-то route_master, то он новый
  try:
    rm[id]
    master = rm[id]["master"]
    new = 1
  except:
    new = 0
    pass

  try:
    rtype = tags["route"]
  except KeyError:
    continue
  if rtype not in route_types:
    continue

  if new:
    mref = rm[id]["ref"]
  # если ref не указан, то для новых маршрутов возьмём его из route_master
  try:
    ref = tags["ref"]
    tref = ref
  except KeyError:
    tref = "(null)"
    if new:
      ref = mref
    else:
      continue

  if not members or not len(members):
    if warns > 0:
      print "Warning: route relation %d has no members" % id
    continue
  if debug > 0:
    print "%d: new %s route %s" % (id,rtype,ref)

  if new:
    count_r = count_r+1
  else:
    count_o = count_o+1

  # assume route is valid if checking is off
  valid = 1
  if checkvalid:
    ways = []
    n0 = 0
    n1 = 0
    n2 = 0
    rwarns = []

  for i in range(0,len(members)/2):
    mkey = members[2*i]
    mid = int(mkey[1:])
    mtype = mkey[0] # n = node, w = way, r = relation
    mrole = members[2*i+1]

    if checkvalid and mtype == "w" and new:
      ways.append(str(mid))

    # для новых маршрутов остановка должна одну из ролей new_platform_roles, для старых - одну из old_stop_roles
    if (mtype == "n" and (not new and (mrole in old_stop_roles)) or (new and (mrole in new_platform_roles))) or mtype == "w":
      if debug > 0:
        print "%d: new %s %d on %s route %s" % (id, otype, mid, rtype, ref)
      try:
        refs[mkey]
      except:
        refs[mkey] = {}
      try:
        oref = refs[mkey][rtype]
      except:
        oref = None
      if not oref:
        oref = ref
      else:
        lref = re.split(r'\s*,\s*', oref)
        if ref not in lref:
          lref.append(ref)
          lref.sort(cmp=ptrefcmp)
          oref = ", ".join(lref)
      refs[mkey][rtype] = oref
    elif mtype == "n":
      if not (new and (mrole in new_stop_roles)):
        if checkvalid:
          valid = 0
          if new:
            rwarns.append("Non-stop node %d (role=\"%s\") in new route relation %d" % (mid, mrole, id))
          else:
            rwarns.append("Non-stop node %d (role=\"%s\") in old route relation %d" % (mid, mrole, id))
        if warns > 0:
          print "Warning: route relation %d has non-stop node %d (new=%d, role=%s)" % (id, mid, new, mrole)
    elif mtype == "w" and new and mrole != "":
      if checkvalid:
        valid = 0
        rwarns.append("Non-empty role for way %d (role=\"%s\") in new route relation %d" % (mid, mrole, id))
      if warns > 0:
        print "Warning: route relation %d is new and has non-empty role %s for way %d" % (id, mrole, mid)
    elif mtype == "r":
      if checkvalid:
        valid = 0
        rwarns.append("Relation member %d (role=\"%s\") in route relation %d" % (mid, mrole, id))
      if warns > 0:
        print "Warning: route relation %d has relation member %d" % (id, mid)
    else:
      raise BaseException("This cannot happen!")

  if checkvalid and new and len(ways):
    if pgtype == 'pgsql':
      q = "SELECT id, nodes FROM %s_ways WHERE id IN (%s)" % (prefix, ",".join(ways))
    elif pgtype == 'osm-simple':
      q = "SELECT MIN(way_id), ARRAY_AGG(node_id) FROM way_nodes WHERE way_id IN (%s) GROUP BY way_id" % (",".join(ways))
    cc2.execute(q)
    waynodes = {}
    while True:
      row = cc2.fetchone()
      if not row:
        break
      wid, nodes = row
      waynodes[str(wid)] = nodes

    cnt = 0
    for wid in ways:
      nodes = waynodes[wid]
      if not n0:
        n0 = nodes[0]
        n1 = nodes[-1]
        n2 = 0
      elif not n2:
        if n0 == nodes[0] or n1 == nodes[0]:
          n0 = nodes[-1]
        elif n0 == nodes[-1] or n1 == nodes[-1]:
          n0 = nodes[0]
        else:
          if warns > 0:
            print "Warning: discontinuity between ways %s and %s at route relation %d" % (wid,ways[cnt-1],id)
          rwarns.append("Discontinuity between ways %s and %s" % (wid,ways[cnt-1]))
          valid = 0
          n0 = nodes[0]
          n1 = nodes[-1]
          n2 = 0
      else:
        if n0 == nodes[0]:
          n0 = nodes[-1]
        elif n0 == nodes[-1]:
          n0 = nodes[0]
        else:
          if warns > 0:
            print "Warning: discontinuity between ways %s and %s at route relation %d" % (wid,ways[cnt-1],id)
          rwarns.append("Discontinuity between ways %s and %s" % (wid,ways[cnt-1]))
          valid = 0
          n0 = nodes[0]
          n1 = nodes[-1]
          n2 = 0
      cnt = cnt + 1
  if new and storeroutes:
    if checkvalid:
      rwarns = "\n".join(rwarns)
    else:
      rwarns = ""
    q = "INSERT INTO %s_routes (master_id, route_id, route, ref, rref, mref, valid, warns) VALUES (%s,%s,'%s','%s','%s','%s',%d,%s)" % (ptprefix, master, id, rtype, ref, mref, tref, valid, sqlesc(rwarns))
    cu.execute(q)

print "routes: %d new routes and %d old routes" % (count_r, count_o)
print "pt objects: %d" % len(refs)

print "Process time elapsed: %d seconds" % (time()-tm)

tm = time()
up = 0

for mkey in refs.keys():
  mtype = mkey[0]
  mid = int(mkey[1:])
  if mtype == "n":
    otype = "node"
  elif mtype == "w":
    otype = "way"
  else:
    raise BaseException("This cannot happen!")
  k = [otype + "_id"]
  v = [str(mid)]
  if len(refs[mkey])<1:
    continue
  for rtype in refs[mkey].keys():
    oref = refs[mkey][rtype]
    k.append("%s_ref" % rtype)
    v.append(sqlesc(oref))
  k = ",".join(k)
  v = ",".join(v)
  cu.execute("INSERT INTO %s_%ss (%s) VALUES (%s)" % (ptprefix, otype, k, v))
  up = up+1
  if up % 10000 == 0:
    tmd = time()-tm
    try:
      qr = up*1./tmd
    except ZeroDivisionError:
      qr = 0
    #print "%d inserts completed at %d seconds, %.2ld queries/sec" % (up, tmd, qr)

pg2.commit()

#print "Inserts: %d" % up
print "Database update time elapsed: %d seconds" % (time()-tm)

