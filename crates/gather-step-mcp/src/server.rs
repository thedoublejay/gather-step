use std::{sync::Arc, time::Instant};

use rmcp::{
    Json, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::McpContext,
    error::McpServerError,
    tool_trace::{ToolCallRecord, count_results_from_json, elapsed_ms, redact_args},
    tools::{
        composite::{
            BatchQueryRequest, BatchQueryResponse, BriefRequest, BriefResponse, ContextRequest,
            ContextResponse, batch_query_tool as run_batch_query, brief_tool as run_brief,
            context_tool as run_context,
        },
        contract::{
            BreakingChangeCandidatesRequest, BreakingChangeCandidatesResponse,
            ContractDriftRequest, ContractDriftResponse, PayloadSchemaRequest,
            PayloadSchemaResponse,
            breaking_change_candidates_tool as run_breaking_change_candidates,
            contract_drift_tool as run_contract_drift, payload_schema_tool as run_payload_schema,
        },
        cross_repo::{
            CrossRepoDepsRequest, CrossRepoDepsResponse, SharedTypeUsageRequest,
            SharedTypeUsageResponse, TraceImpactRequest, TraceImpactResponse,
            cross_repo_deps_tool as run_cross_repo_deps,
            get_shared_type_usage_tool as run_shared_type_usage,
            trace_impact_tool as run_trace_impact,
        },
        crud_trace::{CrudTraceRequest, CrudTraceResponse, crud_trace_tool as run_crud_trace},
        deployment_topology::{
            DeploymentTopologyResponse, EnvVarTopologyRequest, RepoTopologyRequest,
            ServiceTopologyRequest, deployed_but_no_code_tool as run_deployed_but_no_code,
            env_var_consumers_tool as run_env_var_consumers, service_env_tool as run_service_env,
            shared_infra_tool as run_shared_infra,
            undeployed_services_tool as run_undeployed_services,
            where_deployed_tool as run_where_deployed,
        },
        events::{
            EventBlastRadiusRequest, EventBlastRadiusResponse, ListOrphanTopicsRequest,
            ListOrphanTopicsResponse, TraceEventRequest, TraceEventResponse, TraceRouteRequest,
            TraceRouteResponse, event_blast_radius_tool as run_event_blast_radius,
            list_orphan_topics_tool as run_list_orphan_topics, trace_event_tool as run_trace_event,
            trace_route_tool as run_trace_route,
        },
        orientation::{GraphSchemaResponse, ListReposResponse, get_graph_schema, list_repos},
        packs::{
            ContextPackRequest, ContextPackResponse, ModePackRequest,
            change_impact_pack_tool as run_change_impact_pack,
            context_pack_tool as run_context_pack, debug_pack_tool as run_debug_pack,
            fix_pack_tool as run_fix_pack, planning_pack_tool as run_planning_pack,
            review_pack_tool as run_review_pack,
        },
        projection_impact::{
            ProjectionImpactRequest, ProjectionImpactResponse,
            projection_impact_tool as run_projection_impact,
        },
        repo_intelligence::{
            ConventionResponse, DeadCodeRequest, DeadCodeResponse, OverviewResponse,
            RepoScopedRequest, WhoOwnsRequest, WhoOwnsResponse,
            get_conventions_tool as run_get_conventions, get_dead_code_tool as run_get_dead_code,
            get_overview_tool as run_get_overview, who_owns_tool as run_who_owns,
        },
        search::{
            SearchRequest, SearchResponse, SymbolRequest, SymbolResponse, TraversalRequest,
            TraversalResponse, get_callees, get_callers, get_symbol, search_symbols,
        },
    },
};

#[derive(Clone)]
pub struct GatherStepMcpServer {
    ctx: Arc<McpContext>,
    tool_router: ToolRouter<Self>,
    cancel: CancellationToken,
}

impl std::fmt::Debug for GatherStepMcpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatherStepMcpServer")
            .field("ctx", &self.ctx)
            .field("cancelled", &self.cancel.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl GatherStepMcpServer {
    #[must_use]
    pub fn new(ctx: McpContext) -> Self {
        Self::with_cancel(ctx, CancellationToken::new())
    }

    #[must_use]
    pub fn with_cancel(ctx: McpContext, cancel: CancellationToken) -> Self {
        Self {
            ctx: Arc::new(ctx),
            tool_router: Self::tool_router(),
            cancel,
        }
    }

    /// Return the names of all tools registered on this server instance.
    ///
    /// Intended for use in tests and diagnostics.
    #[doc(hidden)]
    #[must_use]
    pub fn registered_tool_names() -> Vec<String> {
        Self::tool_router()
            .map
            .keys()
            .map(std::string::ToString::to_string)
            .collect()
    }

    /// Execute a blocking tool closure, time it, and emit a trace record.
    ///
    /// `tool_name` — the MCP tool name string (e.g. `"search"`).
    /// `args` — the raw request value used only for the privacy-redacted summary.
    /// `f` — a `FnOnce` that returns `Result<Json<T>, String>` where `T: serde::Serialize`.
    ///
    /// The return value is identical to calling `spawn_blocking(f).await`
    /// directly so callers can use `?` as before.
    async fn traced_call<T, F>(
        &self,
        tool_name: &'static str,
        args: &serde_json::Value,
        f: F,
    ) -> Result<Json<T>, String>
    where
        T: serde::Serialize + Send + 'static,
        F: FnOnce() -> Result<Json<T>, String> + Send + 'static,
    {
        if self.cancel.is_cancelled() {
            return Err("tool call cancelled: server is shutting down".to_owned());
        }
        let tracer = self.ctx.tracer().clone();
        let session_id = tracer.session_id().to_owned();
        let args_summary = redact_args(args);
        let start = Instant::now();
        let cancel = self.cancel.clone();
        let result = tokio::task::spawn_blocking(move || {
            if cancel.is_cancelled() {
                Err("tool call cancelled: server is shutting down".to_owned())
            } else {
                f()
            }
        })
        .await
        .map_err(|error| format!("blocking task join error: {error}"))?;
        let elapsed = elapsed_ms(start);
        let (result_count, error_msg) = match &result {
            Ok(json_val) => {
                let v = serde_json::to_value(&json_val.0).unwrap_or(serde_json::Value::Null);
                (count_results_from_json(&v), None)
            }
            Err(e) => (0, Some(e.clone())),
        };
        let record = ToolCallRecord::new(
            &session_id,
            tool_name,
            args_summary,
            elapsed,
            result_count,
            error_msg,
        );
        tracer.emit(&record);
        result
    }

    pub async fn serve_stdio(self) -> Result<(), McpServerError> {
        let server = self
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|error| McpServerError::Initialize(error.to_string()))?;
        server
            .waiting()
            .await
            .map_err(|error| McpServerError::Join(error.to_string()))?;
        Ok(())
    }

    pub async fn serve_stdio_until_cancelled(
        self,
        cancel: CancellationToken,
    ) -> Result<(), McpServerError> {
        let server = self
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|error| McpServerError::Initialize(error.to_string()))?;
        tokio::select! {
            result = server.waiting() => {
                result.map_err(|error| McpServerError::Join(error.to_string()))?;
            }
            () = cancel.cancelled() => {}
        }
        Ok(())
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for GatherStepMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(self.ctx.config.instructions.clone())
            .with_server_info(
                Implementation::new("gather-step", env!("CARGO_PKG_VERSION"))
                    .with_title(self.ctx.config.server_name.clone()),
            )
    }
}

#[tool_router(router = tool_router)]
impl GatherStepMcpServer {
    #[tool(
        name = "get_graph_schema",
        description = "Return a compact summary of node and edge kinds in the indexed graph.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_graph_schema_tool(&self) -> Result<Json<GraphSchemaResponse>, String> {
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_graph_schema", &serde_json::Value::Null, move || {
            get_graph_schema(&ctx)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_graph_schema_summary",
        description = "Return a compact summary of node and edge kinds in the indexed graph.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_graph_schema_summary_tool(&self) -> Result<Json<GraphSchemaResponse>, String> {
        let ctx = Arc::clone(&self.ctx);
        self.traced_call(
            "get_graph_schema_summary",
            &serde_json::Value::Null,
            move || {
                get_graph_schema(&ctx)
                    .map(Json)
                    .map_err(|error| error.to_string())
            },
        )
        .await
    }

    #[tool(
        name = "list_repos",
        description = "List indexed repositories and their basic freshness metadata.",
        annotations(read_only_hint = true)
    )]
    pub async fn list_repos_tool(&self) -> Result<Json<ListReposResponse>, String> {
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("list_repos", &serde_json::Value::Null, move || {
            list_repos(&ctx)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "search",
        description = "Search indexed symbols with optional repo, language, and kind filters.",
        annotations(read_only_hint = true)
    )]
    pub async fn search_tool(
        &self,
        Parameters(request): Parameters<SearchRequest>,
    ) -> Result<Json<SearchResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("search", &args, move || {
            search_symbols(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_symbol",
        description = "Return stored metadata for a symbol by its stable symbol_id.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_symbol_tool(
        &self,
        Parameters(request): Parameters<SymbolRequest>,
    ) -> Result<Json<SymbolResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_symbol", &args, move || {
            get_symbol(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_callers",
        description = "Return caller symbols that reach the target over CALLS edges.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_callers_tool(
        &self,
        Parameters(request): Parameters<TraversalRequest>,
    ) -> Result<Json<TraversalResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_callers", &args, move || {
            get_callers(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_callees",
        description = "Return callee symbols reached from the target over CALLS edges.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_callees_tool(
        &self,
        Parameters(request): Parameters<TraversalRequest>,
    ) -> Result<Json<TraversalResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_callees", &args, move || {
            get_callees(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "trace_impact",
        description = "Trace cross-repo impact through virtual nodes such as routes, topics, queues, and shared symbols.",
        annotations(read_only_hint = true)
    )]
    pub async fn trace_impact_tool(
        &self,
        Parameters(request): Parameters<TraceImpactRequest>,
    ) -> Result<Json<TraceImpactResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("trace_impact", &args, move || {
            run_trace_impact(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "trace_event",
        description = "Trace producer and consumer symbols attached to a topic, queue, subject, stream, or event.",
        annotations(read_only_hint = true)
    )]
    pub async fn trace_event_tool(
        &self,
        Parameters(request): Parameters<TraceEventRequest>,
    ) -> Result<Json<TraceEventResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("trace_event", &args, move || {
            run_trace_event(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "trace_route",
        description = "Trace server handlers and client callers attached to a route virtual node.",
        annotations(read_only_hint = true)
    )]
    pub async fn trace_route_tool(
        &self,
        Parameters(request): Parameters<TraceRouteRequest>,
    ) -> Result<Json<TraceRouteResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("trace_route", &args, move || {
            run_trace_route(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "crud_trace",
        description = "Trace frontend callers, backend handlers, and persistence touchpoints for a CRUD route.",
        annotations(read_only_hint = true)
    )]
    pub async fn crud_trace_tool(
        &self,
        Parameters(request): Parameters<CrudTraceRequest>,
    ) -> Result<Json<CrudTraceResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("crud_trace", &args, move || {
            run_crud_trace(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "event_blast_radius",
        description = "Trace transitive downstream impact from an event-like virtual node.",
        annotations(read_only_hint = true)
    )]
    pub async fn event_blast_radius_tool(
        &self,
        Parameters(request): Parameters<EventBlastRadiusRequest>,
    ) -> Result<Json<EventBlastRadiusResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("event_blast_radius", &args, move || {
            run_event_blast_radius(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "list_orphan_topics",
        description = "List topic, event, queue, subject, or stream targets with only producers or only consumers.",
        annotations(read_only_hint = true)
    )]
    pub async fn list_orphan_topics_tool(
        &self,
        Parameters(request): Parameters<ListOrphanTopicsRequest>,
    ) -> Result<Json<ListOrphanTopicsResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("list_orphan_topics", &args, move || {
            run_list_orphan_topics(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "cross_repo_deps",
        description = "List repositories connected to the target repo through shared virtual nodes.",
        annotations(read_only_hint = true)
    )]
    pub async fn cross_repo_deps_tool(
        &self,
        Parameters(request): Parameters<CrossRepoDepsRequest>,
    ) -> Result<Json<CrossRepoDepsResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("cross_repo_deps", &args, move || {
            run_cross_repo_deps(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_shared_type_usage",
        description = "Find shared symbol nodes matching a type name and summarize which repos use them.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_shared_type_usage_tool(
        &self,
        Parameters(request): Parameters<SharedTypeUsageRequest>,
    ) -> Result<Json<SharedTypeUsageResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_shared_type_usage", &args, move || {
            run_shared_type_usage(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "payload_schema",
        description = "Return inferred producer and consumer payload schemas for a virtual target.",
        annotations(read_only_hint = true)
    )]
    pub async fn payload_schema_tool(
        &self,
        Parameters(request): Parameters<PayloadSchemaRequest>,
    ) -> Result<Json<PayloadSchemaResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("payload_schema", &args, move || {
            run_payload_schema(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "contract_drift",
        description = "Compare producer and consumer payload contracts for a virtual target and surface mismatches.",
        annotations(read_only_hint = true)
    )]
    pub async fn contract_drift_tool(
        &self,
        Parameters(request): Parameters<ContractDriftRequest>,
    ) -> Result<Json<ContractDriftResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("contract_drift", &args, move || {
            run_contract_drift(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "breaking_change_candidates",
        description = "Given a producer symbol or DTO name, list affected consumers and drift candidates.",
        annotations(read_only_hint = true)
    )]
    pub async fn breaking_change_candidates_tool(
        &self,
        Parameters(request): Parameters<BreakingChangeCandidatesRequest>,
    ) -> Result<Json<BreakingChangeCandidatesResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("breaking_change_candidates", &args, move || {
            run_breaking_change_candidates(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "brief",
        description = "Return a small one-screen summary for a target symbol or search term.",
        annotations(read_only_hint = true)
    )]
    pub async fn brief_tool(
        &self,
        Parameters(request): Parameters<BriefRequest>,
    ) -> Result<Json<BriefResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("brief", &args, move || {
            run_brief(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "context",
        description = "Return combined symbol, traversal, repo, and impact context for a target.",
        annotations(read_only_hint = true)
    )]
    pub async fn context_tool(
        &self,
        Parameters(request): Parameters<ContextRequest>,
    ) -> Result<Json<ContextResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("context", &args, move || {
            run_context(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "context_pack",
        description = "Return a bounded planning, debug, fix, review, or change-impact pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn context_pack_tool(
        &self,
        Parameters(request): Parameters<ContextPackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("context_pack", &args, move || {
            run_context_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "projection_impact",
        description = "Return source/projected field chains, direct readers/writers, runtime surfaces, and planning risk hints for a data field.",
        annotations(read_only_hint = true)
    )]
    pub async fn projection_impact_tool(
        &self,
        Parameters(request): Parameters<ProjectionImpactRequest>,
    ) -> Result<Json<ProjectionImpactResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("projection_impact", &args, move || {
            run_projection_impact(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "where_deployed",
        description = "Return deployments linked to an indexed service.",
        annotations(read_only_hint = true)
    )]
    pub async fn where_deployed_tool(
        &self,
        Parameters(request): Parameters<ServiceTopologyRequest>,
    ) -> Result<Json<DeploymentTopologyResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("where_deployed", &args, move || {
            run_where_deployed(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "service_env",
        description = "Return environment variables linked to an indexed service.",
        annotations(read_only_hint = true)
    )]
    pub async fn service_env_tool(
        &self,
        Parameters(request): Parameters<ServiceTopologyRequest>,
    ) -> Result<Json<DeploymentTopologyResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("service_env", &args, move || {
            run_service_env(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "env_var_consumers",
        description = "Return services that read an indexed environment variable.",
        annotations(read_only_hint = true)
    )]
    pub async fn env_var_consumers_tool(
        &self,
        Parameters(request): Parameters<EnvVarTopologyRequest>,
    ) -> Result<Json<DeploymentTopologyResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("env_var_consumers", &args, move || {
            run_env_var_consumers(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "undeployed_services",
        description = "Return indexed service nodes without deployment evidence.",
        annotations(read_only_hint = true)
    )]
    pub async fn undeployed_services_tool(
        &self,
        Parameters(request): Parameters<RepoTopologyRequest>,
    ) -> Result<Json<DeploymentTopologyResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("undeployed_services", &args, move || {
            run_undeployed_services(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "deployed_but_no_code",
        description = "Return deployment nodes that lack service-code linkage.",
        annotations(read_only_hint = true)
    )]
    pub async fn deployed_but_no_code_tool(
        &self,
        Parameters(request): Parameters<RepoTopologyRequest>,
    ) -> Result<Json<DeploymentTopologyResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("deployed_but_no_code", &args, move || {
            run_deployed_but_no_code(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "shared_infra",
        description = "Return indexed database and broker nodes with service consumers.",
        annotations(read_only_hint = true)
    )]
    pub async fn shared_infra_tool(
        &self,
        Parameters(request): Parameters<RepoTopologyRequest>,
    ) -> Result<Json<DeploymentTopologyResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("shared_infra", &args, move || {
            run_shared_infra(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_context_pack",
        description = "Return a bounded planning, debug, fix, review, or change-impact pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_context_pack_tool(
        &self,
        Parameters(request): Parameters<ContextPackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_context_pack", &args, move || {
            run_context_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "planning_pack",
        description = "Return a bounded planning pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn planning_pack_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("planning_pack", &args, move || {
            run_planning_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "plan_change",
        description = "Return a bounded planning pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn plan_change_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("plan_change", &args, move || {
            run_planning_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "debug_pack",
        description = "Return a bounded debug pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn debug_pack_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("debug_pack", &args, move || {
            run_debug_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "fix_pack",
        description = "Return a bounded fix pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn fix_pack_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("fix_pack", &args, move || {
            run_fix_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "fix_surface",
        description = "Return a bounded fix pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn fix_surface_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("fix_surface", &args, move || {
            run_fix_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "review_pack",
        description = "Return a bounded review pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn review_pack_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("review_pack", &args, move || {
            run_review_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "change_impact_pack",
        description = "Return a bounded change-impact pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn change_impact_pack_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("change_impact_pack", &args, move || {
            run_change_impact_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_change_impact_pack",
        description = "Return a bounded change-impact pack for a target symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_change_impact_pack_tool(
        &self,
        Parameters(request): Parameters<ModePackRequest>,
    ) -> Result<Json<ContextPackResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_change_impact_pack", &args, move || {
            run_change_impact_pack(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "batch_query",
        description = "Execute multiple bounded MCP tool queries in a single round-trip.",
        annotations(read_only_hint = true)
    )]
    pub async fn batch_query_tool(
        &self,
        Parameters(request): Parameters<BatchQueryRequest>,
    ) -> Result<Json<BatchQueryResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("batch_query", &args, move || {
            run_batch_query(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "who_owns",
        description = "Return history-based ownership percentages and bus-factor hints for a file or symbol.",
        annotations(read_only_hint = true)
    )]
    pub async fn who_owns_tool(
        &self,
        Parameters(request): Parameters<WhoOwnsRequest>,
    ) -> Result<Json<WhoOwnsResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("who_owns", &args, move || {
            run_who_owns(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_dead_code",
        description = "List graph-reachability dead-code candidates for a repo.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_dead_code_tool(
        &self,
        Parameters(request): Parameters<DeadCodeRequest>,
    ) -> Result<Json<DeadCodeResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_dead_code", &args, move || {
            run_get_dead_code(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_conventions",
        description = "Detect repeated structural conventions in a repo.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_conventions_tool(
        &self,
        Parameters(request): Parameters<RepoScopedRequest>,
    ) -> Result<Json<ConventionResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_conventions", &args, move || {
            run_get_conventions(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[tool(
        name = "get_overview",
        description = "Return a repo overview enriched with graph and git analytics summaries.",
        annotations(read_only_hint = true)
    )]
    pub async fn get_overview_tool(
        &self,
        Parameters(request): Parameters<RepoScopedRequest>,
    ) -> Result<Json<OverviewResponse>, String> {
        let args = serde_json::to_value(&request).unwrap_or_default();
        let ctx = Arc::clone(&self.ctx);
        self.traced_call("get_overview", &args, move || {
            run_get_overview(&ctx, request)
                .map(Json)
                .map_err(|error| error.to_string())
        })
        .await
    }
}
