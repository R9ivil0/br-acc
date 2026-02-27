// ICARUS — Probabilistic Partner->Person linking (non-factual)
//
// Creates POSSIBLE_SAME_AS edges to bridge masked CPF partners to strong Person identities
// without mutating factual SOCIO_DE semantics.
//
// Rules:
// - Partner.doc_type = cpf_partial
// - Partner.doc_partial has 6 digits
// - Partner.name = Person.name
// - Partner.doc_partial = Person.cpf_middle6
// - Keep only 1:1 candidate pairs
// - Exclude high-ambiguity names (candidate frequency > 5)
//
// Parameters expected:
// - $run_id (string)

// Phase 0: Ensure supporting index exists (idempotent)
CREATE INDEX partner_name_doc_partial IF NOT EXISTS
FOR (p:Partner) ON (p.name, p.doc_partial);

// Phase 1: Remove previously generated edges for this run id (idempotent reruns)
MATCH (:Partner)-[r:POSSIBLE_SAME_AS]->(:Person)
WHERE r.run_id = $run_id
DELETE r;

// Phase 2: Build high-precision 1:1 candidate pairs and write edges in batches
CALL {
  MATCH (person:Person)
  WHERE person.name IS NOT NULL
    AND person.cpf_middle6 =~ "\\d{6}"
  WITH person

  // Candidate partial partner from exact (name, middle6) key
  MATCH (partner:Partner)
  WHERE partner.doc_type = "cpf_partial"
    AND partner.name = person.name
    AND partner.doc_partial = person.cpf_middle6
  WITH person, collect(partner) AS partners
  WHERE size(partners) = 1
  WITH person, partners[0] AS partner

  // 1:1 on (name, middle6): only one Person candidate for this key
  MATCH (person_key:Person)
  WHERE person_key.name = person.name
    AND person_key.cpf_middle6 = person.cpf_middle6
  WITH person, partner, count(person_key) AS person_key_count
  WHERE person_key_count = 1

  // 1:1 on (name, middle6): only one Partner candidate for this key
  MATCH (partner_key:Partner)
  WHERE partner_key.doc_type = "cpf_partial"
    AND partner_key.name = person.name
    AND partner_key.doc_partial = person.cpf_middle6
  WITH person, partner, count(partner_key) AS partner_key_count
  WHERE partner_key_count = 1

  // Ambiguity guard by name frequency among partial partners
  MATCH (name_peer:Partner)
  WHERE name_peer.doc_type = "cpf_partial"
    AND name_peer.name = person.name
  WITH person, partner, count(name_peer) AS name_frequency
  WHERE name_frequency <= 5

  MERGE (partner)-[r:POSSIBLE_SAME_AS]->(person)
  ON CREATE SET
    r.confidence = 0.85,
    r.method = "name_middle6_unique",
    r.evidence = "cpf_partial+name_exact+uniqueness",
    r.created_at = datetime(),
    r.run_id = $run_id
  ON MATCH SET
    r.confidence = 0.85,
    r.method = "name_middle6_unique",
    r.evidence = "cpf_partial+name_exact+uniqueness",
    r.created_at = datetime(),
    r.run_id = $run_id
} IN TRANSACTIONS OF 2000 ROWS;
