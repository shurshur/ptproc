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

from config import *

route_types = ["bus", "trolleybus", "tram", "share_taxi"]
old_stop_roles = ["stop", "forward:stop", "backward:stop", "forward_stop", "backward_stop"]
new_stop_roles = ["stop", "stop_exit_only", "stop_entry_only" ]
new_platform_roles = ["platform", "platform_exit_only", "platform_entry_only" ]
old_way_roles = ["forward", "backward", ""]

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

if georoutes:
  if not storeroutes:
    raise BaseException("georoutes=1 requires storeroutes=1")

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
  cu.execute("DELETE FROM %s_routes" % ptprefix)
  cu.execute("DELETE FROM %s_stops" % ptprefix)

# route masters
rm = {}

# счётчик route_master
count_rm = 0
# счётчик новых route как мемберов route_master
count_rn = 0

# извлечение stop_area
sa = {}
if pgtype == 'pgsql':
  q="SELECT id,tags,members FROM %s_rels WHERE 'public_transport=stop_area'=ANY(tags2pairs(tags))" % prefix
elif pgtype == 'osm-simple':
  print "stop_area is not supported now for pgtype=osm-simple"
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
    name = tags["name"]
  except:
    name = "(null)"
  # TODO брать name из stop/platform

  if not members:
    members = []

  stop = None
  platform = None
  for i in range(0,len(members)/2):
    mkey = members[2*i]
    mid = int(mkey[1:])
    mtype = mkey[0] # n = node, w = way, r = relation
    mrole = members[2*i+1]
    if mrole in new_stop_roles:
      1 #FIXME TODO
    elif mrole in new_platform_roles:
      if platform:
        continue # use only first platform
      platform = {"type": mtype, "id": mid}
    else:
      1 #FIXME TODO

  sa[id] = {"name": name, "platform": platform}


print "stop_areas: %d" % len(sa)

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
        if debug > 0:
          print "%d: new route_master's member %d" % (id, mid)
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
    if not members:
      members = []
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
    members = []
    if not _members:
      _members = []
    for i in range(0,len(_members)):
      mtmp = _members[i].split(":")
      mkey = mtmp[0]
      mrole = ":".join(mtmp[1:])
      members.append(mkey)
      members.append(mrole)

  try:
    rtype = tags["route"]
  except KeyError:
    continue
  if rtype not in route_types:
    continue

  try:
    rfrom = sqlesc(tags["from"])
  except:
    rfrom = "NULL"
  try:
    rto = sqlesc(tags["to"])
  except:
    rto = "NULL"

  # если route является частью какого-то route_master, то он новый
  try:
    rm[id]
    master = rm[id]["master"]
    new = 1
  except:
    master = None
    new = 0
    pass
  # если route имеет хоть один platform, то он новый
  if not new:
    for i in new_platform_roles:
      if i in members[1::2]:
        if debug > 0:
          print "id=%d is new because platform found" % id
        new = 1
        break

  if new and master:
    mref = rm[id]["ref"]
  # если ref не указан, то для новых маршрутов возьмём его из route_master
  try:
    ref = tags["ref"]
    tref = ref
  except KeyError:
    tref = "(null)"
    if new and master:
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
  if checkvalid or georoutes:
    ways = []
  if checkvalid:
    n0 = 0
    n1 = 0
    n2 = 0
    rwarns = []

  seq = 0

  for i in range(0,len(members)/2):
    mkey = members[2*i]
    mid = int(mkey[1:])
    mtype = mkey[0] # n = node, w = way, r = relation
    mrole = members[2*i+1]

    if (checkvalid or georoutes) and (mtype == "w" and ((new and mrole=="") or (not new and mrole in old_way_roles))):
      ways.append(str(mid))

    # для новых маршрутов остановка должна одну из ролей new_platform_roles, для старых - одну из old_stop_roles
    if (mtype == "n" and ((not new and (mrole in old_stop_roles)) or (new and (mrole in new_platform_roles)))) or (mtype == "w" and not new and (mrole in old_way_roles)) or (mtype == "w" and new and mrole == ""):
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
      if new and mrole in new_platform_roles:
        seq = seq + 1
        cu.execute("INSERT INTO %s_stops (route_id, seq, osm_id, osm_type) VALUES (%d, %d, %s, '%s')" % (ptprefix, id, seq, mid, mtype))
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
    elif mtype == "w" and not new and mrole not in old_way_roles:
      if checkvalid:
        valid = 0
        rwarns.append("Invalid role for way %d (role=\"%s\") in old route relation %d" % (mid, mrole, id))
      if warns > 0:
        print "Warning: route relation %d is old and has invalid role %s for way %d" % (id, mrole, mid)
    elif mtype == "r":
      if checkvalid:
        if mrole not in new_stop_roles:
          valid = 0
          rwarns.append("Non-stop role \"%s\" for relation member %d in route relation %d" % (mrole, mid, id))
        else:
          try:
            sa[mid]
          except:
            valid = 0
            rwarns.append("Relation member %d (role=\"%s\") in route relation %d should be stop_area" % (mid, mrole, id))
          try:
            p = sa[mid]["platform"]
            mkey = p["type"] + p["id"]
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
            if new and mrole in new_platform_roles:
              seq = seq + 1
              cu.execute("INSERT INTO %s_stops (route_id, seq, osm_id, osm_type) VALUES (%d, %d, %s, '%s')" % (ptprefix, id, seq, mid, mtype))
          except:
            rwarns.append("No platform node in stop_area relation \"%d\"" % mid)
    else:
      raise BaseException("This cannot happen!")

  geom = None
  # если ptdata=None, значит, результат будет записан в отдельную базу и нужно извлечь геометрию в текстовом виде
  if not ptdata and georoutes and len(ways):
    if pgtype == 'pgsql':
      q = "SELECT ST_AsText(ST_Multi(ST_Collect(way))),MAX(ST_SRID(way)) FROM %s_line WHERE osm_id IN (%s)" % (prefix, ",".join(ways))
      q = q + " UNION "
      q = q + "SELECT ST_AsText(ST_Multi(ST_Collect(way))),MAX(ST_SRID(way)) FROM %s_polygon WHERE osm_id IN (%s)" % (prefix, ",".join(ways))
    elif pgtype == 'osm-simple':
      q = "SELECT ST_AsText(ST_Multi(ST_Collect(linestring))),MAX(ST_SRID(linestring)) FROM ways WHERE id IN (%s)" % (",".join(ways))
    cc2.execute(q)
    row = cc2.fetchone()
    if row:
      geom, srid = row
      if not srid:
        if warns > 0:
          print "Route id=%d geometry is invalid, try fix it" % id
        rwarns.append("Geometry is invalid, try fix it")
        continue
    else:
      if warns > 0:
        print "Route id=%d creating geometry failed"
      rwarns.append("Creating geometry failed")

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
      try:
        nodes = waynodes[wid]
      except KeyError:
        if warns > 0:
          print "Oops! No waynodes for way %s in route %d!" % (wid, id)
        rwarns.append("Oops! No waynodes for way %s!" % (wid))
        continue
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

  if storeroutes:
    q = None
    # два варианта:
    #  ptdata=None и геометрию будем копировать запросом в пределах базы
    #  ptdata!=None и geom!=None, тогда геометрию берём в виде текста, извлечённую ранее
    # третий вариант - когда геометрии нет - будет далее
    if georoutes and len(ways) and ((geom and ptdata) or (not ptdata)):
      if checkvalid:
        rwarns = "\n".join(rwarns)
      else:
        rwarns = ""
      if not master:
        master = 0
        mref = ""
      if ptdata:
        geom = "ST_GeomFromText(%s,%d)" % (sqlesc(geom), srid)
        q = "INSERT INTO %s_routes (master_id, route_id, route, ref, rref, mref, valid, warns, way, newroute, route_from, route_to) VALUES (%s,%s,'%s',%s,%s,%s,%d,%s,%s,%d,%s,%s)" % (ptprefix, master, id, rtype, sqlesc(ref), sqlesc(mref), sqlesc(tref), valid, sqlesc(rwarns), geom, new, rfrom, rto)
      else:
        geom = "ST_LineMerge(ST_Collect(way)) AS way"
        q = "INSERT INTO %s_routes (master_id, route_id, route, ref, rref, mref, valid, warns, way, newroute, route_from, route_to) SELECT %s,%s,'%s',%s,%s,%s,%d,%s,%s,%d,%s,%s FROM %s_line WHERE osm_id IN (%s)" % (ptprefix, master, id, rtype, sqlesc(ref), sqlesc(mref), sqlesc(tref), valid, sqlesc(rwarns), geom, new, rfrom, rto, prefix, ",".join(ways))
    
    # route without geometry
    if not q:
      if checkvalid:
        rwarns = "\n".join(rwarns)
      else:
        rwarns = ""
      if not master:
        master = 0
        mref = ""
      q = "INSERT INTO %s_routes (master_id, route_id, route, ref, rref, mref, valid, warns, newroute, route_from, route_to) VALUES (%s,%s,'%s',%s,%s,%s,%d,%s,%d,%s,%s)" % (ptprefix, master, id, rtype, sqlesc(ref), sqlesc(mref), sqlesc(tref), valid, sqlesc(rwarns), new, rfrom, rto)
    
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
  elif mtype == "r":
    otype = "relation"
    # чё делать в таких случаях? не представляю пока что...
    continue
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

