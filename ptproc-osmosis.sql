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
  warns TEXT
);
ALTER TABLE pt_routes ADD way GEOMETRY;
