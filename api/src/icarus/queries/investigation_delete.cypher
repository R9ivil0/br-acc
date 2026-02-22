MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation {id: $id})
DETACH DELETE i
RETURN count(i) AS deleted
