Create a reusable architecture documentation skill at
~/.claude/skills/arch-docs/ with this exact structure:

arch-docs/
├── SKILL.md
├── templates/
│   ├── td-template.md
│   └── c4-context.md
└── scripts/
    └── fingerprint.sh

=== SKILL.md requirements ===

YAML frontmatter:
- name: arch-docs
- description: "Generates a complete ARCHITECTURE-ANALYSIS.md for any
  repository (C++, Spring, or Spring Boot). Use when asked to analyze,
  document, or generate architecture docs, technical designs (TDs),
  C4 diagrams, system context, API inventories, database/data-layer
  maps, or security/PCI scoping for a codebase. Runs Phase 0
  fingerprinting first, then produces C4 Context/Container/Component
  Mermaid diagrams, a full inbound/outbound interface inventory,
  a database access map, a security & compliance assessment, and
  a Technical Design document."

Body — a phased playbook:

PHASE 0 — FINGERPRINTING (always first):
- Run scripts/fingerprint.sh from the repo root and use its output.
- Do NOT read source files until fingerprinting completes.
- Output a fingerprint block: language(s), framework + version,
  build tool, entry points, datastores, messaging, external
  integrations, deployment artifacts.

PHASE 1 — STRUCTURAL & INTERFACE ANALYSIS:
- Map top-level modules/packages. For Spring Boot: locate
  @SpringBootApplication, controllers, services, repositories,
  config classes, application.yml/properties profiles.
- For C++: locate main(), CMakeLists/Makefile targets, shared libs.
- INBOUND interfaces — for each REST endpoint, batch/Quartz job,
  Kafka/JMS listener, or SFTP poller, capture:
  endpoint/path or trigger, method/schedule, auth mechanism
  (OAuth2 scope, mTLS, API key, none), request/response payload
  summary, and whether sensitive data (PAN/PII/CHD) transits it.
- OUTBOUND dependencies — for each DB, Kafka topic, Vault call,
  SFTP target, or external API, capture:
  target system, protocol (HTTPS/JDBC/SFTP/Kafka), TLS version
  enforcement, credential source (Vault path, keystore, env var,
  hardcoded — flag if hardcoded), retry/timeout config.
- DATA LAYER ANALYSIS — for each database identified, capture:
  * DB engine and version (from driver + config)
  * JDBC URL pattern per environment (redact hosts to env-var
    names; note multi-host/failover URLs indicating Patroni/HA
    or read replicas)
  * Connection pool: HikariCP max/min size, connection timeout,
    leak detection settings
  * Schemas and key tables accessed — derive from JPA entities,
    @Table annotations, MyBatis/mapper XML, or raw SQL strings;
    mark each as READ, WRITE, or BOTH, and note the owning
    component/service class
  * Transaction management: @Transactional usage, isolation
    levels, propagation, any distributed/XA transactions
  * Migrations: Flyway/Liquibase presence and script locations
  * Sensitive columns: any table storing PAN, tokens, keys, PII
  * Flag if the DB appears to be shared with other applications
    (integration-via-database anti-pattern)

PHASE 2 — C4 DIAGRAMS:
- Read templates/c4-context.md for the required Mermaid style.
- Produce: Level 1 System Context, Level 2 Container,
  and Level 3 Component for the core module only.
- Every external system and database found in Phase 1 must
  appear in Level 1/2 with its protocol labeled on the arrow
  (JDBC+TLS, HTTPS, SFTP, Kafka).

PHASE 3 — SECURITY & COMPLIANCE ASSESSMENT:
- Read templates/td-template.md and follow section 8 exactly.
- Secrets inventory: list every credential/key/cert the app uses
  and where it is sourced from.
- Encryption at rest (TDE, pgcrypto, PGP files) and in transit
  per network hop identified in Phase 1 — including JDBC
  sslmode/TLS settings per database.
- SAST-relevant patterns: hardcoded secrets, weak or deprecated
  algorithms (flag anything like PBEWithMD5AndDES, MD5, SHA-1,
  DES/3DES, ECB mode), missing HSTS, cleartext HTTP, disabled
  cert validation, sslmode=disable on JDBC URLs.
- PCI DSS scope determination: does this repo store, process, or
  transmit CHD/PAN? If yes, list the applicable requirements
  (e.g. 3.4 rendering PAN unreadable, 3.6 key management,
  4.1 transmission encryption) and where in the code each
  applies — including which database tables/columns hold PAN
  or tokens.

PHASE 4 — TECHNICAL DESIGN DOC:
- Fill every section of templates/td-template.md.
- Include: data flow narrative, config matrix per environment,
  known risks/tech debt.

PHASE 5 — OUTPUT:
- Write a single ARCHITECTURE-ANALYSIS.md at the repo root.
- Embed all Mermaid diagrams, interface tables, and the database
  access map inline. No separate files.
- End with a "Confidence & Gaps" section listing anything
  that could not be verified from code alone.

Rules:
- Markdown output only, never docx.
- Keep SKILL.md under 200 lines; details live in templates/.
- Never print actual secret values found in code — reference
  file/line only.

=== templates/td-template.md ===
A skeleton with these sections and one-line guidance each:
1. Overview & Business Purpose
2. Fingerprint Summary (from Phase 0)
3. System Context (C4 L1 diagram + narrative)
4. Container View (C4 L2 diagram + narrative)
5. Component View (C4 L3, core module)
6. Data Flow (step-numbered narrative)
7. Interfaces
   7a. Inbound table — columns: Interface | Type (REST/Batch/
       Listener/SFTP) | Path or Trigger | Auth Mechanism |
       Payload Summary | Sensitive Data (Y/N + type)
   7b. Outbound table — columns: Target System | Protocol |
       TLS Enforcement | Credential Source | Retry/Timeout Config
   7c. Database access map — columns: Engine | Schema.Table |
       Access (R/W/RW) | Owning Component | Sensitive Data |
       Notes (HA topology, pooling, migration tool, shared-DB flag)
8. Security & Compliance
   8a. Secrets Inventory (credential | source | rotation notes)
   8b. Encryption — at rest and in transit per hop, including
       per-database JDBC TLS settings
   8c. SAST Findings (weak algorithms, hardcoded secrets,
       missing HSTS, cert validation issues, insecure JDBC —
       with file:line refs)
   8d. PCI DSS Scope (CHD touchpoints, applicable requirements,
       code locations, PAN-bearing tables/columns)
9. Configuration Matrix (per environment)
10. Operational Concerns (logging, metrics, HA, batch schedules)
11. Risks & Tech Debt
12. Confidence & Gaps

=== templates/c4-context.md ===
Two complete Mermaid examples using C4-style flowchart syntax:
1. A Level 1 System Context example: a Spring Boot batch system
   with actors (Ops team), external systems (SFTP server,
   HashiCorp Vault, PostgreSQL with Patroni HA, Kafka), styled
   with classDef for system/external/person/database, and
   protocol labels on every arrow (JDBC+TLS, HTTPS, SFTP).
2. A Level 2 Container example: web app broken into API,
   scheduler/Quartz worker, DB (primary + replica), cache.
State that all generated diagrams must follow these exact
styling conventions for portfolio-wide consistency.

=== scripts/fingerprint.sh ===
A POSIX-compatible bash script that, from the repo root:
- Detects build tool: pom.xml, build.gradle(.kts), CMakeLists.txt,
  Makefile, package.json
- For Maven: extracts groupId/artifactId, Java version,
  spring-boot-starter-parent version, and key dependency families
  (spring-boot, quartz, kafka, redis, vault, postgresql, oracle,
  mysql, mongodb, sshj, bouncycastle, jasypt, flyway, liquibase,
  hikari, mybatis, hibernate) via grep
- For Gradle: same via grep on build files
- For C++: lists CMake targets or Makefile targets
- Counts source files by extension (.java, .kt, .cpp, .h, .xml)
- Finds config files: application*.yml/properties, logback*.xml
- Database quick-scan:
  * grep for jdbc: URLs in config — print driver type and host
    count per URL only (redact hostnames), flag multi-host URLs
    (HA/failover) and any sslmode/ssl parameters found
  * detect flyway (db/migration) or liquibase changelog dirs
  * count @Entity and @Table annotations, count mapper XML files
  * grep for @Transactional count
- Security quick-scan (grep, case-insensitive, report counts +
  file paths only, never values):
  * password/secret/apikey literals in config and source
  * weak crypto strings: PBEWithMD5, MD5, SHA1, DES, ECB
  * http:// URLs in config (excluding localhost and XML namespaces)
  * sslmode=disable or ssl=false in config
- Detects Dockerfile / docker-compose / Jenkinsfile / .gitlab-ci.yml
- Prints everything as a compact labeled summary to stdout
- Must never fail hard: guard every check, exit 0 always

Make fingerprint.sh executable (chmod +x).
After creating everything, print the tree and confirm the skill
is discoverable.
