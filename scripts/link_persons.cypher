// ICARUS — Person Node SAME_AS Linking
// Creates SAME_AS relationships between Person nodes representing the same individual.
// Non-destructive: keeps separate nodes for source attribution, links them for traversal.
// Run once as a migration, then periodically after ETL reloads.

// ── Phase 0: Pre-compute cpf_middle6 on existing full-CPF Person nodes ──
// Strips formatting (XXX.XXX.XXX-XX → 11 digits), extracts middle 6 digits
// (positions [3:9]), stores as indexed property for partial-CPF matching.
CALL {
  MATCH (p:Person)
  WHERE p.cpf IS NOT NULL AND p.cpf_middle6 IS NULL
  WITH p, replace(replace(p.cpf, '.', ''), '-', '') AS digits
  WHERE size(digits) = 11
  SET p.cpf_middle6 = substring(digits, 3, 6)
} IN TRANSACTIONS OF 10000 ROWS;

// ── Phase 1: CPF match (confidence 0.95) ──────────────────────────
// TSE candidates that have unmasked CPF → CNPJ persons with same CPF.
// Both pipelines store formatted CPFs, so exact match is reliable.
CALL {
  MATCH (a:Person)
  WHERE a.sq_candidato IS NOT NULL AND a.cpf IS NOT NULL
  WITH a
  MATCH (b:Person {cpf: a.cpf})
  WHERE b.sq_candidato IS NULL AND b <> a
  MERGE (a)-[:SAME_AS {confidence: 0.95, method: "cpf_match"}]->(b)
} IN TRANSACTIONS OF 5000 ROWS;

// ── Phase 2: Author → TSE candidate by name (confidence 0.90) ────
// Transparencia/TransfereGov authors → TSE candidates.
// Both use normalize_name() from same transform module → exact match safe.
// Small set (~1K authors) vs medium set (TSE candidates).
CALL {
  MATCH (a:Person)
  WHERE a.author_key IS NOT NULL AND a.name IS NOT NULL
  WITH a
  MATCH (b:Person {name: a.name})
  WHERE b.sq_candidato IS NOT NULL AND b <> a
  MERGE (a)-[:SAME_AS {confidence: 0.90, method: "name_match_author_tse"}]->(b)
} IN TRANSACTIONS OF 2000 ROWS;

// ── Phase 3: Author → CNPJ person by name (confidence 0.80) ──────
// Transparencia/TransfereGov authors → CNPJ persons.
// Small set (~1K) vs large set (2M). Person(name) index required.
// Only links if no SAME_AS already exists between pair (avoids duplicates from Phase 2 chains).
CALL {
  MATCH (a:Person)
  WHERE a.author_key IS NOT NULL AND a.name IS NOT NULL
  WITH a
  MATCH (b:Person {name: a.name})
  WHERE b.cpf IS NOT NULL
    AND b <> a
    AND NOT EXISTS { (a)-[:SAME_AS]-(b) }
  MERGE (a)-[:SAME_AS {confidence: 0.80, method: "name_match_author_cnpj"}]->(b)
} IN TRANSACTIONS OF 2000 ROWS;

// ── Phase 4: Disabled partial-document matching ─────────────────────
// Partial CPF-based SAME_AS can create ambiguous merges at national scale.
// Keep phase number for migration compatibility, but do not emit SAME_AS.
MATCH ()-[r:SAME_AS]-()
WHERE r.method = "partial_cpf_name_match"
DELETE r;

// ── Phase 5: Classified servidores — unique name match (confidence 0.85) ──
// For ~34K servidores with blank CPF: match by name only when the name
// appears exactly once among blank-CPF servidores AND exactly once among
// full-CPF persons. Common names auto-excluded by size() != 1.
CALL {
  MATCH (s:Person)-[:RECEBEU_SALARIO]->(:PublicOffice)
  WHERE s.cpf_partial IS NULL AND s.name IS NOT NULL
  WITH s.name AS name, collect(DISTINCT s) AS servidores
  WHERE size(servidores) = 1
  WITH name, servidores[0] AS s
  MATCH (p:Person {name: name})
  WHERE p.cpf_middle6 IS NOT NULL
    AND s <> p
    AND NOT EXISTS { (s)-[:SAME_AS]-(p) }
  WITH s, collect(p) AS targets
  WHERE size(targets) = 1
  WITH s, targets[0] AS target
  MERGE (s)-[:SAME_AS {confidence: 0.85, method: "unique_name_match_servidor"}]->(target)
} IN TRANSACTIONS OF 1000 ROWS;
