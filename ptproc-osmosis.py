#!/usr/bin/python
# OSM Public Transport Processor (experimental osm-simple version)
# See COPYING and AUTHORS for license info
# -*- coding:utf-8 -*-
# vim:shiftwidth=2:autoindent:et
import sys
reload(sys)
sys.setdefaultencoding("utf-8")          # a hack to support UTF-8 
from time import time
import re
import psycopg2
from psycopg2.extras import register_hstore
from psycopg2.extensions import adapt

try:
  import psyco
  psyco.full()
except ImportError:
  pass

debug = 0
warns = 0
route_types = ["bus", "trolleybus", "tram", "share_taxi"]
old_stop_roles = ["stop", "forward:stop", "backward:stop", "forward_stop", "backward_stop"]
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

pg=psycopg2.connect("dbname='osm_simple'")
cc=pg.cursor()
cc2=pg.cursor()
register_hstore(cc)

pg2=psycopg2.connect("dbname='public_transport'")
cu=pg2.cursor()

refs = {}

tm=time()

for otype in ["node", "way"]:
  cu.execute("DELETE FROM pt_%ss" % otype)

# route masters
rm = {}

# счётчик route_master
count_rm = 0
# счётчик новых route как мемберов route_master
count_rn = 0

# сначала извлечём все route_master
cc.execute("SELECT id,UNNEST(ARRAY_AGG(tags)) AS tags,ARRAY_AGG(LOWER(member_type)||member_id||':'||member_role) AS members FROM relations JOIN relation_members ON id=relation_id WHERE 'type'=>'route_master' <@ tags GROUP BY id")
while True:
  row = cc.fetchone()
  if not row:
    break
  id, tags, members = row

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
  for i in range(0,len(members)):
    mtmp = members[i].split(":")
    mkey = mtmp[0]
    mid = int(mkey[1:])
    mtype = mkey[0]
    mrole = ":".join(mtmp[1:])
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
# одним запросом получается неэффективно, поэтому members извлекаем отдельными запросами
#cc.execute("SELECT id,UNNEST(ARRAY_AGG(tags)) AS tags,ARRAY_AGG(LOWER(member_type)||member_id||':'||member_role) AS members FROM relations JOIN relation_members ON id=relation_id WHERE 'type'=>'route' <@ tags GROUP BY id")
cc.execute("SELECT id,UNNEST(ARRAY_AGG(tags)) AS tags FROM relations WHERE 'type'=>'route' <@ tags GROUP BY id")
while True:
  row = cc.fetchone()
  if not row:
    break
  id, tags = row
  cc2.execute("SELECT ARRAY_AGG(LOWER(member_type)||member_id||':'||member_role) AS members FROM relation_members WHERE relation_id=%d" % id)
  row = cc2.fetchone()
  if not row:
    continue
  members, = row

  # если route является частью какого-то route_master, то он новый
  try:
    rm[id]
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

  # если ref не указан, то для новых маршрутов возьмём его из route_master
  try:
    ref = tags["ref"]
  except KeyError:
    if new:
      ref = rm[id]["ref"]
    else:
      continue

  if not members:
    if warns > 0:
      print "Warning: route relation %d has no members" % id
    continue
  if debug > 0:
    print "%d: new %s route %s" % (id,rtype,ref)

  if new:
    count_r = count_r+1
  else:
    count_o = count_o+1

  for i in range(0,len(members)):
    mtmp = members[i].split(":")
    mkey = mtmp[0]
    mid = int(mkey[1:])
    mtype = mkey[0]
    mrole = ":".join(mtmp[1:])
    if mtype == "n":
      ptype = "stop"
    elif mtype == "w":
      ptype = "way"
    # для новых маршрутов остановка должна иметь роль platform, для старых - одну из old_stop_roles
    if (mtype == "n" and (not new and (mrole in old_stop_roles)) or (new and (mrole == "platform"))) or mtype == "w":
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
      if warns > 0:
        if not (new and mrole == "stop"):
          print "Warning: route relation %d has non-stop node %d (new=%d, role=%s)" % (id, mid, new, mrole)
    elif mtype == "w" and new and mrole != "":
      if warns > 0:
        print "Warning: route relation %d is new and has non-empty role %s for way %d" % (id, mrole, mid)
    elif mtype == "r":
      if warns > 0:
        print "Warning: route relation %d has relation member %d" % (id, mid)
    else:
      raise BaseException("This cannot happen!")

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
  cu.execute("INSERT INTO pt_%ss (%s) VALUES (%s)" % (otype, k, v))
  up = up+1
  if up % 10000 == 0:
    tmd = time()-tm
    try:
      qr = up*1./tmd
    except ZeroDivisionError:
      qr = 0
    #print "%d inserts completed at %d seconds, %.2ld queries/sec" % (up, tmd, qr)

pg.commit()

#print "Inserts: %d" % up
print "Database update time elapsed: %d seconds" % (time()-tm)

