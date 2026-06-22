# Infobip Kafka Topic Decryption Low-Level Design

## 1. Purpose

Design a Databricks Spark Structured Streaming pipeline that reads CRM events from an Infobip Kafka topic, decrypts configured sensitive fields, and writes the decrypted payload to the downstream target in near real time.

## 2. Scope

This design covers:

- Kafka message ingestion from the Infobip topic
- Retrieval of the RSA private key through Databricks secrets backed by Azure Key Vault
- Decryption of the per-message AES key
- Decryption of parameterized sensitive fields in dynamic JSON messages
- Streaming error handling, observability, and operational controls

This design does not cover:

- Upstream Infobip encryption implementation
- Downstream CRM persistence schema design
- Key rotation process inside Azure Key Vault beyond the runtime usage of versioned keys

## 3. Business Context

Infobip publishes CRM-related events where selected fields are encrypted with a one-time AES key. The AES key itself is encrypted with the bank-managed RSA public key and added to each message. The bank must use the RSA private key to recover the AES key and decrypt only the configured fields before the data is consumed downstream.

## 4. High-Level Flow

1. Infobip publishes an event to Kafka.
2. Spark Structured Streaming reads the raw Kafka record.
3. The streaming job parses the JSON payload without requiring a fixed schema for all business attributes.
4. The job reads the encrypted AES key from `encKey` and key version from `encKeyVersion`.
5. The RSA private key is retrieved from Databricks secrets (Azure Key Vault-backed scope).
6. The job decrypts `encKey` to obtain the AES key for the current message.
7. The job decrypts only the configured sensitive fields, such as `from`, `to`, and `text`.
8. The job emits the decrypted event together with processing metadata.
9. Records that cannot be decrypted are routed to an error stream/table with the failure reason and raw payload reference.

## 5. Source Message Characteristics

### 5.1 Example source fields

- Business metadata: `eventType`, `channel`, `accountName`, `messageId`, `sendAt`
- Encrypted fields: `from`, `to`, `text`
- Key metadata: `encKeyVersion`, `encKey`
- Optional nested content represented as stringified JSON: `dataPayload`

### 5.2 Important characteristics

- Message shape is dynamic; new attributes may appear over time.
- Not every field is encrypted.
- Some encrypted fields may be `null`.
- Encrypted values are Base64-encoded strings.
- `encKeyVersion` identifies which RSA key version was used by Infobip.

## 6. Target Architecture

### 6.1 Components

1. **Kafka Source**
   - Reads from the configured Infobip topic
   - Uses checkpointing for exactly-once-compatible Spark processing semantics

2. **Databricks Streaming Job**
   - Spark Structured Streaming application
   - Parses Kafka value as JSON
   - Applies decryption transformation
   - Writes successful and failed records separately

3. **Databricks Secret Scope**
   - Holds references to Azure Key Vault secrets
   - Provides the RSA private key material to the job at runtime

4. **Azure Key Vault**
   - System of record for RSA private keys
   - Supports controlled rotation and access governance

5. **Output Layer**
   - Decrypted bronze/silver table, Delta table, or target sink for downstream CRM usage
   - Error table or dead-letter output for failed records

### 6.2 Logical data flow

`Infobip Kafka Topic -> Databricks Structured Streaming -> JSON Parsing -> AES Key Decryption via RSA Private Key -> Field-Level AES Decryption -> Success Output / Error Output`

## 7. Detailed Design

### 7.1 Runtime parameters

The streaming job must be parameterized with:

- Kafka bootstrap servers / connection settings
- Source topic name
- Consumer group or checkpoint location
- Output sink location/table
- Error sink location/table
- Secret scope name
- Secret key name for RSA private key
- List of fields to decrypt
- Optional list of fields containing nested JSON strings that should be preserved as-is after decryption

Example decryptable field list for the first integration:

- `from`
- `to`
- `text`

### 7.2 Secret and key handling

- The RSA private key is stored in Azure Key Vault.
- Databricks accesses the key through an Azure Key Vault-backed secret scope.
- The job retrieves the secret once during initialization and broadcasts or reuses it across executors in a controlled way.
- The private key must never be logged, persisted, or exposed in output data.
- Access to the secret scope must be limited to the job identity and authorized operators only.

### 7.3 Message parsing strategy

Because the schema is dynamic, the pipeline should not rely on a rigid case class or static full-schema contract.

Recommended approach:

- Parse the Kafka payload into a map-like JSON representation that preserves all top-level fields.
- Extract mandatory decryption control fields (`encKey`, `encKeyVersion`) explicitly.
- Iterate over the configured sensitive field list and decrypt only when:
  - the field exists
  - the value is not null
  - the value is a non-empty string

Unknown fields must pass through unchanged.

### 7.4 Decryption logic

For each message:

1. Base64-decode `encKey`.
2. Decrypt `encKey` with the RSA private key to obtain the AES key.
3. For each configured sensitive field:
   - Base64-decode the field value
   - Decrypt the value using the recovered AES key
   - Replace the encrypted value in the output payload with the plaintext value
4. Preserve all non-sensitive fields without modification.
5. Add processing metadata.

### 7.5 Processing metadata

The output record should include operational metadata such as:

- `decryptionStatus` (`SUCCESS` / `FAILED`)
- `decryptionTimestamp`
- `sourceTopic`
- `sourcePartition`
- `sourceOffset`
- `encKeyVersion`
- `failedFieldName` (for failures, when applicable)
- `failureReason` (for failures)

## 8. Error Handling

### 8.1 Failure scenarios

- Missing `encKey`
- Missing `encKeyVersion`
- Private key retrieval failure
- Unsupported or unexpected key version
- Invalid Base64 content
- RSA decryption failure for `encKey`
- AES decryption failure for one or more target fields
- Malformed JSON payload

### 8.2 Error-handling behavior

- Do not stop the full stream for bad business records.
- Route failed records to a dedicated error sink with:
  - raw payload
  - Kafka metadata
  - failure category
  - failure reason
  - processing timestamp
- Raise alerts when failure rate crosses an operational threshold.
- Reserve stream termination for infrastructure failures such as inability to access Kafka or secrets.

## 9. Schema and Data Handling Decisions

### 9.1 Dynamic JSON support

- Top-level schema evolution is allowed.
- The decryption logic is driven by configuration, not hard-coded field positions.
- New non-sensitive fields flow through automatically.

### 9.2 Null and optional values

- `null` sensitive fields remain `null`.
- Empty strings are not decrypted and should be passed through unchanged unless business rules later require validation.

### 9.3 Nested payloads

- Fields like `dataPayload` may contain serialized JSON text.
- For the first version, the pipeline will treat such fields as plain strings unless explicitly added to the decryptable field list.
- If future integrations require nested-field decryption, the design can be extended with path-based configuration.

## 10. Security Design

- Use Databricks secret references only; do not embed private keys in notebooks, configs, or code.
- Restrict secret scope access with least privilege.
- Mask sensitive values in logs and monitoring outputs.
- Do not persist decrypted sensitive values to intermediate debug storage.
- Use encrypted transport for Kafka connectivity and Databricks platform integrations.
- Maintain key version awareness through `encKeyVersion` for traceability and future rotation support.

## 11. Performance Considerations

- RSA decryption is performed once per message to recover the AES key.
- AES decryption is then applied only to the configured fields.
- Reuse initialized cryptographic objects where safely possible within executor constraints.
- Broadcast immutable key material or parsed key objects carefully to reduce repeated secret retrieval overhead.
- Tune micro-batch size and parallelism according to Kafka throughput and cryptographic cost.

## 12. Observability

The pipeline should expose metrics for:

- Total records consumed
- Successfully decrypted records
- Failed records
- Failure rate by category
- Processing latency per micro-batch
- Kafka lag

Operational logs must include:

- job run identifier
- batch identifier
- source topic / partition / offset range
- decryption outcome summary

Logs must not include decrypted PII or private key contents.

## 13. Deployment View

### 13.1 Databricks job

- Runs as a scheduled continuous streaming job
- Uses a service principal or managed identity with:
  - Kafka access
  - Databricks secret scope access
  - output storage/table write access

### 13.2 Environment-specific configuration

Each environment must externalize:

- Kafka endpoints
- topic names
- checkpoint locations
- output locations
- secret scope names
- secret key names
- decryptable field list

## 14. Suggested Output Contract

### 14.1 Successful record

- Original message fields
- Decrypted values in configured sensitive fields
- Processing metadata fields

### 14.2 Failed record

- Raw source payload
- Kafka metadata
- `encKeyVersion`
- failure details
- processing timestamp

## 15. Initial Configuration for Infobip Integration

### 15.1 Integration name

- `infobip-kafka-topic-decryption`

### 15.2 Initial sensitive field list

- `from`
- `to`
- `text`

### 15.3 Mandatory control fields

- `encKey`
- `encKeyVersion`

## 16. Open Points

- Confirm the exact AES mode/padding used by Infobip for field encryption.
- Confirm whether all encrypted fields share the same AES mode and IV handling approach.
- Confirm whether IV, nonce, or tag data is embedded in each encrypted field value or transported separately.
- Confirm the target downstream sink and retention policy for error records.
- Confirm whether any nested fields inside `dataPayload` will require decryption in later phases.

## 17. Summary

The proposed design uses Databricks Structured Streaming to ingest dynamic Infobip Kafka events, retrieve the RSA private key securely from Azure Key Vault through Databricks secrets, decrypt the message-level AES key, and decrypt only the configured sensitive fields. The solution preserves schema flexibility, supports future integrations through configuration, and separates successful and failed processing paths for operational resilience.
