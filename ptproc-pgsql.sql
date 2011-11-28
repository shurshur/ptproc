CREATE TABLE pt_nodes (node_id INT PRIMARY KEY,bus_ref TEXT,trolleybus_ref TEXT,tram_ref TEXT,share_taxi_ref TEXT);
CREATE TABLE pt_ways (way_id INT PRIMARY KEY,bus_ref TEXT,trolleybus_ref TEXT,tram_ref TEXT,share_taxi_ref TEXT);
CREATE TABLE pt_routes (
  master_id INT,
  route_id INT,
  route TEXT,
  ref TEXT,
  mref TEXT,
  rref TEXT,
  valid INT,
  warns TEXT,
  route_from TEXT,
  route_to TEXT
);
CREATE OR REPLACE FUNCTION tags2pairs(a text[]) RETURNS text[] AS $SQL$
    SELECT array_agg($1[i] || '=' || $1[i+1])
    FROM generate_series(1, array_upper($1,1)) i
    WHERE i % 2 = 1
$SQL$ LANGUAGE sql;
ALTER TABLE pt_routes ADD way GEOMETRY;
ALTER TABLE pt_routes ADD newroute INT NOT NULL DEFAULT 0;
CREATE TABLE pt_stops (
  route_id bigint not null,
  seq int not null,
  osm_id bigint not null,
  osm_type char not null,
  name text,
  point geometry
);
