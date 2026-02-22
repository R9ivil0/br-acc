MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation {id: $id})
SET i.title = COALESCE($title, i.title),
    i.description = COALESCE($description, i.description),
    i.updated_at = datetime()
WITH i
OPTIONAL MATCH (i)-[:INCLUDES]->(e)
WITH i, collect(e.id) AS eids
RETURN i.id AS id,
       i.title AS title,
       i.description AS description,
       i.created_at AS created_at,
       i.updated_at AS updated_at,
       i.share_token AS share_token,
       [x IN eids WHERE x IS NOT NULL] AS entity_ids
