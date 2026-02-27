// Neo4j Identity Integrity Gates
// Run this file against the shadow database before cutover.
// Expected results are documented per query.

// G1: Person CPF must never be masked.
MATCH (p:Person)
WHERE p.cpf CONTAINS '*'
RETURN count(p) AS person_cpf_masked;
// expected: 0

// G2: Person CPF must never be 14-digit (CNPJ-like).
MATCH (p:Person)
WHERE replace(replace(p.cpf, '.', ''), '-', '') =~ '\\d{14}'
RETURN count(p) AS person_cpf_14_digits;
// expected: 0

// G3: Person->Company SOCIO_DE must come from formatted CPF identities only.
MATCH (p:Person)-[:SOCIO_DE]->(:Company)
WHERE NOT p.cpf =~ '\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}'
RETURN count(p) AS invalid_person_company_socio_links;
// expected: 0

// G4: Company->Company SOCIO_DE must exist (PJ socios present).
MATCH (:Company)-[r:SOCIO_DE]->(:Company)
RETURN count(r) AS company_company_socio_links;
// expected: > 0

// G5: Partner->Company SOCIO_DE must exist for partial/invalid PF records.
MATCH (:Partner)-[r:SOCIO_DE]->(:Company)
RETURN count(r) AS partner_company_socio_links;
// expected: > 0

// G6: Disabled partial-doc SAME_AS method must be absent.
MATCH ()-[r:SAME_AS]-()
WHERE r.method = 'partial_cpf_name_match'
RETURN count(r) AS partial_doc_same_as_edges;
// expected: 0

