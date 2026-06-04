use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    GraphStoreDb, GraphStoreError, MetadataStoreDb, MetadataStoreError, SearchStoreError,
    TantivySearchStore, search_store::SearchWorkload,
};

#[derive(Clone)]
pub struct WorkspaceStores {
    root: PathBuf,
    graph: Arc<GraphStoreDb>,
    search: Arc<TantivySearchStore>,
    metadata: Arc<MetadataStoreDb>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceStoresError {
    #[error("graph store error: {0}")]
    Graph(#[from] GraphStoreError),
    #[error("search store error: {0}")]
    Search(#[from] SearchStoreError),
    #[error("metadata store error: {0}")]
    Metadata(#[from] MetadataStoreError),
}

impl WorkspaceStores {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, WorkspaceStoresError> {
        Self::open_with_workload(root, SearchWorkload::OneShot)
    }

    pub fn open_with_workload(
        root: impl AsRef<Path>,
        workload: SearchWorkload,
    ) -> Result<Self, WorkspaceStoresError> {
        let root = root.as_ref().to_path_buf();
        let graph = Arc::new(GraphStoreDb::open(root.join("graph.redb"))?);
        let search = Arc::new(TantivySearchStore::open_with_workload(
            root.join("search"),
            workload,
        )?);
        let metadata = Arc::new(MetadataStoreDb::open(root.join("metadata.sqlite"))?);
        Ok(Self {
            root,
            graph,
            search,
            metadata,
        })
    }

    pub fn open_read_only_search(root: impl AsRef<Path>) -> Result<Self, WorkspaceStoresError> {
        let root = root.as_ref().to_path_buf();
        let graph = Arc::new(GraphStoreDb::open(root.join("graph.redb"))?);
        let search = Arc::new(TantivySearchStore::open_read_only(root.join("search"))?);
        let metadata = Arc::new(MetadataStoreDb::open(root.join("metadata.sqlite"))?);
        Ok(Self {
            root,
            graph,
            search,
            metadata,
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn open_with_broken_search(root: impl AsRef<Path>) -> Result<Self, WorkspaceStoresError> {
        let root = root.as_ref().to_path_buf();
        let graph = Arc::new(GraphStoreDb::open(root.join("graph.redb"))?);
        let search = Arc::new(TantivySearchStore::open_read_only(root.join("search"))?);
        let metadata = Arc::new(MetadataStoreDb::open(root.join("metadata.sqlite"))?);
        Ok(Self {
            root,
            graph,
            search,
            metadata,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn graph(&self) -> &GraphStoreDb {
        self.graph.as_ref()
    }

    pub(crate) fn graph_mut(&mut self) -> Option<&mut GraphStoreDb> {
        Arc::get_mut(&mut self.graph)
    }

    #[must_use]
    pub fn search(&self) -> &TantivySearchStore {
        self.search.as_ref()
    }

    #[must_use]
    pub fn metadata(&self) -> &MetadataStoreDb {
        self.metadata.as_ref()
    }
}

impl std::fmt::Debug for WorkspaceStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceStores")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}
