use crate::{ContextPackRecord, MetadataStoreDb, MetadataStoreError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackBlob {
    pub key: String,
    pub mode: String,
    pub target: String,
    pub generation: i64,
    pub response: Vec<u8>,
    pub created_at: i64,
    pub last_read_at: i64,
    pub byte_size: i64,
    pub hit_count: i64,
}

pub struct PackStore<'a> {
    metadata: &'a MetadataStoreDb,
}

impl<'a> PackStore<'a> {
    #[must_use]
    pub const fn new(metadata: &'a MetadataStoreDb) -> Self {
        Self { metadata }
    }

    pub fn get_latest(&self, key: &str) -> Result<Option<PackBlob>, MetadataStoreError> {
        self.metadata.get_context_pack(key).map(|record| {
            record.map(|record| PackBlob {
                key: record.pack_key,
                mode: record.mode,
                target: record.target,
                generation: record.generation,
                response: record.response,
                created_at: record.created_at,
                last_read_at: record.last_read_at,
                byte_size: record.byte_size,
                hit_count: record.hit_count,
            })
        })
    }

    pub fn put(
        &self,
        blob: &PackBlob,
        files: &[(String, String)],
    ) -> Result<(), MetadataStoreError> {
        self.metadata.put_context_pack(
            &ContextPackRecord {
                pack_key: blob.key.clone(),
                mode: blob.mode.clone(),
                target: blob.target.clone(),
                generation: blob.generation,
                response: blob.response.clone(),
                created_at: blob.created_at,
                last_read_at: blob.last_read_at,
                byte_size: blob.byte_size,
                hit_count: blob.hit_count,
            },
            files,
        )
    }

    pub fn invalidate_generation(
        &self,
        repo: &str,
        file_paths: &[String],
    ) -> Result<usize, MetadataStoreError> {
        let targets = file_paths
            .iter()
            .cloned()
            .map(|file_path| (repo.to_owned(), file_path))
            .collect::<Vec<_>>();
        self.invalidate_generation_scope(&targets)
    }

    pub fn invalidate_generation_scope(
        &self,
        targets: &[(String, String)],
    ) -> Result<usize, MetadataStoreError> {
        self.metadata.invalidate_context_packs_for_targets(targets)
    }

    pub fn touch(&self, key: &str, now_unix: i64) -> Result<(), MetadataStoreError> {
        self.metadata.touch_context_pack(key, now_unix)
    }

    pub fn evict_if_needed(&self, max_bytes: i64) -> Result<(), MetadataStoreError> {
        if max_bytes <= 0 {
            return Ok(());
        }

        let mut records = self.metadata.list_context_packs()?;
        let mut total_bytes = records
            .iter()
            .map(|record| record.byte_size.max(0))
            .sum::<i64>();
        if total_bytes <= max_bytes {
            return Ok(());
        }

        records.sort_by(|left, right| {
            left.last_read_at
                .cmp(&right.last_read_at)
                .then(left.created_at.cmp(&right.created_at))
                .then(left.pack_key.cmp(&right.pack_key))
        });

        let mut evicted = Vec::new();
        for record in records {
            if total_bytes <= max_bytes {
                break;
            }
            total_bytes = total_bytes.saturating_sub(record.byte_size.max(0));
            evicted.push(record.pack_key);
        }

        self.metadata.delete_context_packs(&evicted)?;
        Ok(())
    }
}
