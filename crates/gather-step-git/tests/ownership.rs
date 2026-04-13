//! Ownership privacy tests for the gather-step-git crate.
//!
//! Verifies that `redact_email` correctly hides raw author emails in
//! `who_owns` output by default.

#![forbid(unsafe_code)]

use gather_step_git::redact_email;

#[test]
fn who_owns_does_not_expose_raw_author_email_by_default() {
    let raw_email = "a@example.com";
    let redacted = redact_email(raw_email);

    // The raw email domain must not appear in the output.
    assert!(
        !redacted.contains("@example.com"),
        "redacted id must not contain raw email domain; got: {redacted}"
    );
    // The raw local-part ("a") followed by "@" must not appear as a verbatim
    // copy of the original address.  We check that the redacted id does not
    // equal or start with the original local-part followed by the original "@"
    // domain separator, i.e. the literal string "a@example.com" is absent.
    assert!(
        !redacted.contains("a@example.com"),
        "redacted id must not contain the raw email address; got: {redacted}"
    );
    // Must end with @redacted suffix.
    assert!(
        redacted.ends_with("@redacted"),
        "redacted id must end with @redacted; got: {redacted}"
    );
}

#[test]
fn redact_email_is_deterministic() {
    let email = "contributor@org.example";
    assert_eq!(
        redact_email(email),
        redact_email(email),
        "redact_email must produce the same output for the same input"
    );
}

#[test]
fn redact_email_prefix_is_16_hex_chars() {
    let id = redact_email("test@example.com");
    let prefix = id.trim_end_matches("@redacted");
    assert_eq!(
        prefix.len(),
        16,
        "prefix must be exactly 16 hex chars; got {prefix:?}"
    );
    assert!(
        prefix.chars().all(|c| c.is_ascii_hexdigit()),
        "prefix must consist of hex digits only; got {prefix:?}"
    );
}

#[test]
fn distinct_emails_produce_distinct_identifiers() {
    // Statistical property: two different emails should (almost certainly)
    // produce different 16-hex-char prefixes.  For a fixed test we use two
    // emails that are known to differ in their SHA-256 prefix.
    let a = redact_email("alice@example.com");
    let b = redact_email("bob@example.com");
    assert_ne!(a, b, "distinct emails must produce distinct redacted ids");
}

#[test]
fn author_node_redacts_email_in_all_four_fields() {
    let raw_email = "someone@example.internal";
    let node = gather_step_git::intelligence::author_node_for_test(raw_email);

    assert!(
        !node.file_path.contains(raw_email),
        "file_path must not contain raw email; got {:?}",
        node.file_path
    );
    assert!(
        !node.file_path.contains('@') || node.file_path.ends_with("@redacted"),
        "file_path must only contain @ as part of the @redacted suffix; got {:?}",
        node.file_path
    );
    assert!(
        !node.name.contains(raw_email),
        "name must not contain raw email; got {:?}",
        node.name
    );
    assert!(
        !node
            .qualified_name
            .as_deref()
            .unwrap_or("")
            .contains(raw_email),
        "qualified_name must not contain raw email; got {:?}",
        node.qualified_name
    );
    assert!(
        !node
            .external_id
            .as_deref()
            .unwrap_or("")
            .contains(raw_email),
        "external_id must not contain raw email; got {:?}",
        node.external_id
    );
    // Full domain must never appear in any field.
    assert!(!node.file_path.contains("@example.internal"));
    assert!(!node.name.contains("@example.internal"));
    assert!(
        !node
            .qualified_name
            .as_deref()
            .unwrap_or("")
            .contains("@example.internal")
    );
    assert!(
        !node
            .external_id
            .as_deref()
            .unwrap_or("")
            .contains("@example.internal")
    );
}

#[test]
fn author_node_redaction_is_deterministic() {
    let a = gather_step_git::intelligence::author_node_for_test("someone@example.internal");
    let b = gather_step_git::intelligence::author_node_for_test("someone@example.internal");
    assert_eq!(a.id, b.id);
    assert_eq!(a.name, b.name);
    assert_eq!(a.file_path, b.file_path);
}

#[test]
fn author_node_distinct_emails_produce_distinct_identifiers() {
    let a = gather_step_git::intelligence::author_node_for_test("someone@example.internal");
    let b = gather_step_git::intelligence::author_node_for_test("other@example.internal");
    assert_ne!(a.id, b.id);
    assert_ne!(a.name, b.name);
    assert_ne!(a.file_path, b.file_path);
}

#[test]
fn redact_email_uses_keyed_blake3_first_sixteen_hex_chars_with_redacted_suffix() {
    // redact_email now uses keyed BLAKE3 (preimage recovery requires the
    // per-instance key).  The test verifies structural invariants rather than
    // a specific hash value, because the key is private to the module.
    let redacted = redact_email("someone@example.com");
    let prefix = redacted.trim_end_matches("@redacted");

    assert_eq!(
        prefix.len(),
        16,
        "prefix must be exactly 16 hex chars; got {prefix:?}"
    );
    assert!(
        prefix.chars().all(|c| c.is_ascii_hexdigit()),
        "prefix must consist of hex digits only; got {prefix:?}"
    );
    assert!(
        redacted.ends_with("@redacted"),
        "must end with @redacted suffix; got {redacted:?}"
    );
    // The keyed output must differ from the unkeyed digest of the same input.
    let unkeyed_prefix: String = blake3::hash(b"someone@example.com")
        .to_hex()
        .as_str()
        .chars()
        .take(16)
        .collect();
    assert_ne!(
        prefix, unkeyed_prefix,
        "keyed hash output must differ from unkeyed hash (key is non-trivial)"
    );
}
