MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation)
WITH count(i) AS total
MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation)
ORDER BY i.created_at DESC
SKIP $skip
LIMIT $limit
OPTIONAL MATCH (i)-[:INCLUDES]->(e)
WITH total, i, collect(e.id) AS eids
RETURN total,
       i.id AS id,
       i.title AS title,
       i.description AS description,
       i.created_at AS created_at,
       i.updated_at AS updated_at,
       i.share_token AS share_token,
       [x IN eids WHERE x IS NOT NULL] AS entity_ids
