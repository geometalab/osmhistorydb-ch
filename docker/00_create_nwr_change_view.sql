create or replace view nwr_changes_v as (
  select id, version, deleted, changeset_id, created, uid, tags, 'n' as type, st_x(geom) as lat, st_y(geom) as lon
  from nodes
  union all
  select id, version, deleted, changeset_id, created, uid, tags, 'w' as type, st_x(node.geom) as lat, st_y(node.geom) as lon
  from ways, lateral (select geom from nodes where id=(ways.nodes[(array_length(ways.nodes,1)+1)/2]) limit 1) as node
  union all
  select id, version, deleted, changeset_id, created, uid, tags, 'r' as type, st_x(node.geom) as lat, st_y(node.geom) as lon
  from relations, lateral (select geom from nodes where id=jsonb_path_query_first(members, '$ ? (@.type=="n") .ref')::bigint) as node
);
