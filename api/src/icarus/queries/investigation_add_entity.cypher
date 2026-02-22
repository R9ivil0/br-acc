MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation {id: $investigation_id})
MATCH (e) WHERE e.id = $entity_id
MERGE (i)-[:INCLUDES]->(e)
RETURN i.id AS investigation_id, e.id AS entity_id
