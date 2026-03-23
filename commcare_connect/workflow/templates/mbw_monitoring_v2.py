"""
MBW Monitoring V2 Workflow Template.

Pipeline-based version of the MBW monitoring dashboard. Uses 3 pipeline
sources (Connect visits, CCHQ registrations, CCHQ GS forms) and an
mbw_monitoring job handler for complex computations.

Replaces the custom_analysis SSE streaming approach in mbw_monitoring/.
"""

from commcare_connect.workflow.templates.mbw_monitoring.template import RENDER_CODE as V1_RENDER_CODE

# ---------------------------------------------------------------------------
# Build the V2 RENDER_CODE by surgically replacing the SSE data-loading layer
# in the V1 render code with pipeline + job handler logic.
#
# The v1 code loads data via EventSource (SSE) from a custom endpoint.
# The v2 code reads from the `pipelines` prop (auto-loaded by the workflow
# runner) and triggers `actions.startJob()` for complex computations.
#
# Everything else — tabs, modals, helpers, worker management — stays identical.
# ---------------------------------------------------------------------------

DEFINITION = {
    "name": "MBW Monitoring V2",
    "description": "Pipeline-based MBW monitoring with GPS analysis, follow-up rates, and FLW assessment",
    "version": 1,
    "templateType": "mbw_monitoring_v2",
    "statuses": [
        {"id": "in_progress", "label": "In Progress", "color": "blue"},
        {"id": "completed", "label": "Completed", "color": "green"},
    ],
    "config": {
        "showSummaryCards": False,
        "showFilters": False,
        "job_type": "mbw_monitoring",
    },
    "pipeline_sources": [],
}

# Pipeline schemas — these create pipeline definitions when the template is initialized
PIPELINE_SCHEMAS = [
    {
        "alias": "visits",
        "name": "MBW Visit Forms",
        "description": "Connect CSV visit data for MBW monitoring",
        "schema": {
            "data_source": {"type": "connect_csv"},
            "grouping_key": "username",
            "terminal_stage": "visit_level",
            "fields": [
                {"name": "gps_location", "path": "form.meta.location.#text", "aggregation": "first"},
                {"name": "case_id", "path": "form.case.@case_id", "aggregation": "first"},
                {"name": "mother_case_id", "path": "form.parents.parent.case.@case_id", "aggregation": "first"},
                {"name": "form_name", "path": "form.@name", "aggregation": "first"},
                {"name": "visit_datetime", "path": "form.meta.timeEnd", "aggregation": "first"},
                {
                    "name": "entity_id_deliver",
                    "paths": [
                        "form.mbw_visit.deliver.entity_id",
                        "form.visit_completion.mbw_visit.deliver.entity_id",
                    ],
                    "aggregation": "first",
                },
                {
                    "name": "entity_name",
                    "paths": [
                        "form.mbw_visit.deliver.entity_name",
                        "form.visit_completion.mbw_visit.deliver.entity_name",
                    ],
                    "aggregation": "first",
                },
                {
                    "name": "parity",
                    "path": "form.confirm_visit_information.parity__of_live_births_or_stillbirths_after_24_weeks",
                    "aggregation": "first",
                },
                {"name": "anc_completion_date", "path": "form.visit_completion.anc_completion_date", "aggregation": "first"},
                {"name": "pnc_completion_date", "path": "form.pnc_completion_date", "aggregation": "first"},
                {"name": "baby_dob", "path": "form.capture_the_following_birth_details.baby_dob", "aggregation": "first"},
                {
                    "name": "app_build_version",
                    "path": "form.meta.app_build_version",
                    "aggregation": "first",
                    "transform": "int",
                },
                {
                    "name": "bf_status",
                    "paths": [
                        "form.feeding_history.pnc_current_bf_status",
                        "form.feeding_history.oneweek_current_bf_status",
                        "form.feeding_history.onemonth_current_bf_status",
                        "form.feeding_history.threemonth_current_bf_status",
                        "form.feeding_history.sixmonth_current_bf_status",
                    ],
                    "aggregation": "first",
                },
            ],
        },
    },
    {
        "alias": "registrations",
        "name": "CCHQ Registration Forms",
        "description": "CCHQ registration forms for mother data",
        "schema": {
            "data_source": {
                "type": "cchq_forms",
                "form_name": "Register Mother",
                "app_id_source": "opportunity",
            },
            "grouping_key": "case_id",
            "terminal_stage": "visit_level",
            "fields": [
                {"name": "expected_visits", "path": "form.expected_visits", "aggregation": "first"},
                {"name": "mother_name", "path": "form.mother_name", "aggregation": "first"},
                {"name": "user_connect_id", "path": "form.user_connect_id", "aggregation": "first"},
                # var_visit_1..6: schedule blocks for follow-up analysis
                {"name": "var_visit_1_visit_type", "path": "form.var_visit_1.visit_type", "aggregation": "first"},
                {"name": "var_visit_1_mother_case_id", "path": "form.var_visit_1.mother_case_id", "aggregation": "first"},
                {"name": "var_visit_1_visit_date_scheduled", "path": "form.var_visit_1.visit_date_scheduled", "aggregation": "first"},
                {"name": "var_visit_1_visit_expiry_date", "path": "form.var_visit_1.visit_expiry_date", "aggregation": "first"},
                {"name": "var_visit_2_visit_type", "path": "form.var_visit_2.visit_type", "aggregation": "first"},
                {"name": "var_visit_2_mother_case_id", "path": "form.var_visit_2.mother_case_id", "aggregation": "first"},
                {"name": "var_visit_2_visit_date_scheduled", "path": "form.var_visit_2.visit_date_scheduled", "aggregation": "first"},
                {"name": "var_visit_2_visit_expiry_date", "path": "form.var_visit_2.visit_expiry_date", "aggregation": "first"},
                {"name": "var_visit_3_visit_type", "path": "form.var_visit_3.visit_type", "aggregation": "first"},
                {"name": "var_visit_3_mother_case_id", "path": "form.var_visit_3.mother_case_id", "aggregation": "first"},
                {"name": "var_visit_3_visit_date_scheduled", "path": "form.var_visit_3.visit_date_scheduled", "aggregation": "first"},
                {"name": "var_visit_3_visit_expiry_date", "path": "form.var_visit_3.visit_expiry_date", "aggregation": "first"},
                {"name": "var_visit_4_visit_type", "path": "form.var_visit_4.visit_type", "aggregation": "first"},
                {"name": "var_visit_4_mother_case_id", "path": "form.var_visit_4.mother_case_id", "aggregation": "first"},
                {"name": "var_visit_4_visit_date_scheduled", "path": "form.var_visit_4.visit_date_scheduled", "aggregation": "first"},
                {"name": "var_visit_4_visit_expiry_date", "path": "form.var_visit_4.visit_expiry_date", "aggregation": "first"},
                {"name": "var_visit_5_visit_type", "path": "form.var_visit_5.visit_type", "aggregation": "first"},
                {"name": "var_visit_5_mother_case_id", "path": "form.var_visit_5.mother_case_id", "aggregation": "first"},
                {"name": "var_visit_5_visit_date_scheduled", "path": "form.var_visit_5.visit_date_scheduled", "aggregation": "first"},
                {"name": "var_visit_5_visit_expiry_date", "path": "form.var_visit_5.visit_expiry_date", "aggregation": "first"},
                {"name": "var_visit_6_visit_type", "path": "form.var_visit_6.visit_type", "aggregation": "first"},
                {"name": "var_visit_6_mother_case_id", "path": "form.var_visit_6.mother_case_id", "aggregation": "first"},
                {"name": "var_visit_6_visit_date_scheduled", "path": "form.var_visit_6.visit_date_scheduled", "aggregation": "first"},
                {"name": "var_visit_6_visit_expiry_date", "path": "form.var_visit_6.visit_expiry_date", "aggregation": "first"},
                # Submitter username and mother demographics for metadata extraction
                {"name": "metadata_username", "path": "metadata.username", "aggregation": "first"},
                {"name": "mother_dob", "path": "form.mother_details.mother_dob", "aggregation": "first"},
                {"name": "mother_phone", "path": "form.mother_details.phone_number", "aggregation": "first"},
                # Eligibility flag for follow-up rate calculation (_is_eligible check)
                {"name": "eligible_full_intervention_bonus", "path": "form.eligible_full_intervention_bonus", "aggregation": "first"},
            ],
        },
    },
    {
        "alias": "gs_forms",
        "name": "CCHQ Gold Standard Forms",
        "description": "CCHQ Gold Standard visit checklist forms",
        "schema": {
            "data_source": {
                "type": "cchq_forms",
                "form_name": "Gold Standard Visit Checklist",
                "app_id_source": "opportunity",
                "gs_app_id": "2ca67a89dd8a2209d75ed5599b45a5d1",
            },
            "grouping_key": "case_id",
            "terminal_stage": "visit_level",
            "fields": [
                # form.checklist_percentage = GS score (0-100); form.load_flw_connect_id = assessed FLW
                # (matches v1 views.py:739-740 and DOCUMENTATION.md field table)
                {"name": "gs_score", "path": "form.checklist_percentage", "aggregation": "first"},
                {"name": "load_flw_connect_id", "path": "form.load_flw_connect_id", "aggregation": "first"},
                {"name": "assessor_name", "path": "form.assessor_name", "aggregation": "first"},
                {"name": "assessment_date", "path": "form.meta.timeEnd", "aggregation": "first"},
            ],
        },
    },
]

# ---------------------------------------------------------------------------
# Build the V2 RENDER_CODE via string replacement on V1_RENDER_CODE
# ---------------------------------------------------------------------------


def _replace_between_inclusive(code: str, start_marker: str, end_marker: str, replacement: str) -> str:
    """Replace text between two markers (inclusive of both)."""
    start = code.find(start_marker)
    end = code.find(end_marker, start + len(start_marker)) if start >= 0 else -1
    if start < 0 or end < 0:
        raise ValueError(f"Could not find markers: start={start_marker[:60]!r} end={end_marker[:60]!r}")
    return code[:start] + replacement + code[end + len(end_marker) :]


def _build_v2_render_code() -> str:
    """Build the V2 render code by replacing SSE data loading with pipeline + job handler.

    All replacements use _replace_between_inclusive with @v2-replace markers
    placed in V1's template.py. This makes V2 resilient to V1 code changes
    within marked sections.
    """
    code = V1_RENDER_CODE

    # =====================================================================
    # R1. Replace SSE state variables with pipeline/job state variables
    # Marker: // @v2-replace:sse-state:start/end in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "// @v2-replace:sse-state:start",
        "// @v2-replace:sse-state:end",
        "// @v2-replace:sse-state:start\n"
        "    var [dashData, setDashData] = React.useState(null);\n"
        "    var [jobMessages, setJobMessages] = React.useState([]);\n"
        "    var [jobError, setJobError] = React.useState(null);\n"
        "    var [jobRunning, setJobRunning] = React.useState(false);\n"
        "    var [analysisComplete, setAnalysisComplete] = React.useState(false);\n"
        "    var [oauthStatus, setOauthStatus] = React.useState(null);\n"
        "    var jobCleanupRef = React.useRef(null);\n"
        "    // No-ops: saveSnapshot() references these; harmless in V2\n"
        "    var setDataSource = function() {};\n"
        "    var setSnapshotTimestamp = function() {};\n"
        "    // @v2-replace:sse-state:end",
    )

    # =====================================================================
    # R2. Replace SSE loading useEffect with pipeline-aware job trigger
    # Marker: // @v2-replace:data-loading:start/end in template.py
    # =====================================================================
    _PIPELINE_LOADING = """    // @v2-replace:data-loading:start
    // =========================================================================
    // OAuth: Check auth status — only after pipelines are loaded.
    // Delaying the check prevents a CCHQ token-expired redirect from killing
    // an in-progress visits pipeline download (which can take several minutes).
    // Once pipelines are cached, any re-auth redirect causes only a quick reload.
    // =========================================================================
    React.useEffect(function() {
        if (step !== 'dashboard') return;
        if (!pipelinesReady) return;  // Wait for pipeline data before checking
        fetch('/custom_analysis/mbw_monitoring/api/oauth-status/?next=' + encodeURIComponent(window.location.pathname + window.location.search))
        .then(function(r) { return r.json(); })
        .then(function(status) {
            setOauthStatus(status);
        })
        .catch(function() {
            // Network error — leave oauthStatus null so UI doesn't block
        });
    }, [step, pipelinesReady]);

    // =========================================================================
    // Pipeline + Job: Detect loaded pipeline data and run analysis via job handler
    // =========================================================================

    // Helper: check if pipelines are ready (visits must have data; others just need to exist)
    var pipelinesReady = pipelines
        && pipelines.visits && pipelines.visits.rows && pipelines.visits.rows.length > 0
        && ['registrations', 'gs_forms'].every(function(key) {
            return !pipelines[key] || (pipelines[key].rows !== undefined);
        });

    // pipelinesPartial: at least one pipeline has SUCCESSFUL data (> 0 rows).
    // A pipeline alias present with 0 rows + error means it failed — not "partial".
    var pipelinesPartial = pipelines && (
        (pipelines.visits && pipelines.visits.rows && pipelines.visits.rows.length > 0)
        || (pipelines.registrations && pipelines.registrations.rows && pipelines.registrations.rows.length > 0)
        || (pipelines.gs_forms && pipelines.gs_forms.rows && pipelines.gs_forms.rows.length > 0)
    );

    // visitsFailed: visits key exists with 0 rows AND a server error marker.
    // Means the pipeline stream completed but visits could not be loaded.
    var visitsFailed = pipelines && pipelines.visits
        && pipelines.visits.rows && pipelines.visits.rows.length === 0
        && pipelines.visits.metadata && pipelines.visits.metadata.error;

    // Build FLW names from workers prop
    var flwNameMap = React.useMemo(function() {
        var m = {};
        (workers || []).forEach(function(w) {
            if (w.username) m[w.username.toLowerCase()] = w.name || w.username;
        });
        return m;
    }, [workers]);

    // Run analysis job when pipelines are ready
    var runAnalysis = React.useCallback(function() {
        if (!pipelinesReady || !actions || !actions.startJob) return;
        if (jobRunning) return;

        var sessionFlwsList = instance.state?.selected_workers || instance.state?.selected_flws
            || Object.keys(flwNameMap);

        setJobRunning(true);
        setJobError(null);
        setJobMessages(['Starting analysis...']);
        setDashData(null);
        setAnalysisComplete(false);

        actions.startJob(instance.id, {
            job_type: 'mbw_monitoring',
            pipeline_ids: {
                visits: (pipelines.visits && pipelines.visits.metadata && pipelines.visits.metadata.pipeline_id) || null,
                registrations: (pipelines.registrations && pipelines.registrations.metadata && pipelines.registrations.metadata.pipeline_id) || null,
                gs_forms: (pipelines.gs_forms && pipelines.gs_forms.metadata && pipelines.gs_forms.metadata.pipeline_id) || null,
            },
            active_usernames: sessionFlwsList,
            flw_names: flwNameMap,
            flw_statuses: instance.state?.flw_statuses || {},
            opportunity_id: instance.opportunity_id,
        }).then(function(resp) {
            if (!resp || !resp.success) {
                setJobRunning(false);
                setJobError(resp?.error || 'Failed to start analysis job');
                return;
            }
            var taskId = resp.task_id;
            if (!taskId) {
                setJobRunning(false);
                setJobError('No task ID returned from job');
                return;
            }

            setJobMessages(function(prev) { return prev.concat(['Job started (task: ' + taskId + ')']); });

            // Stream job progress
            var cleanup = actions.streamJobProgress(
                taskId,
                // onProgress
                function(data) {
                    if (data.message) {
                        setJobMessages(function(prev) { return prev.concat([data.message]); });
                    }
                },
                // onItemResult
                function(item) {
                    // Individual item results (not used for MBW monitoring)
                },
                // onComplete
                function(results) {
                  try {
                    setJobRunning(false);
                    setAnalysisComplete(true);

                    // Build dashData in the shape the tabs expect
                    var gpsData = results.gps_data || {};
                    var followupData = results.followup_data || {};
                    var qualityMetrics = results.quality_metrics || {};
                    var overviewSummary = results.overview_data || {};
                    var performanceData = results.performance_data || [];

                    // Build overview flw_summaries by merging data from multiple result sections
                    var activeUsernamesList = instance.state?.selected_workers || instance.state?.selected_flws
                        || Object.keys(flwNameMap);

                    // Build last_active lookup from workers prop (mirrors v1 SSE flw_last_active dict)
                    var lastActiveMap = {};
                    (workers || []).forEach(function(w) {
                        if (w.username && w.last_active) {
                            lastActiveMap[w.username.toLowerCase()] = w.last_active;
                        }
                    });

                    var overviewFlwSummaries = activeUsernamesList.map(function(username) {
                        var uLower = username.toLowerCase();
                        var displayName = flwNameMap[uLower] || username;

                        // Compute last_active_days / last_active_date from workers prop
                        var laStr = lastActiveMap[uLower];
                        var lastActiveDays = null;
                        var lastActiveDate = null;
                        if (laStr) {
                            var laDt = new Date(laStr);
                            if (!isNaN(laDt.getTime())) {
                                lastActiveDays = Math.max(0, Math.floor((Date.now() - laDt.getTime()) / 86400000));
                                lastActiveDate = laDt.toISOString().replace('T', ' ').slice(0, 16);
                            }
                        }

                        // From GPS data
                        var gpsFlw = (gpsData.flw_summaries || []).find(function(g) { return g.username === uLower; }) || {};
                        var medianMeters = (gpsData.median_meters_by_flw || {})[uLower];
                        var medianMinutes = (gpsData.median_minutes_by_flw || {})[uLower];

                        // From follow-up data
                        var fuFlw = (followupData.flw_summaries || []).find(function(f) { return f.username === uLower; }) || {};

                        // From quality metrics
                        var quality = qualityMetrics[uLower] || {};

                        // From overview summary
                        var motherCount = (overviewSummary.mother_counts || {})[uLower] || 0;
                        var ebfPct = (overviewSummary.ebf_pct_by_flw || {})[uLower];

                        // Build cases_still_eligible from drilldown
                        var drilldown = (followupData.flw_drilldown || {})[uLower] || [];
                        var eligibleMothers = drilldown.filter(function(m) { return m.eligible; });
                        var stillOnTrack = 0;
                        eligibleMothers.forEach(function(m) {
                            var completedCount = 0;
                            var missedCount = 0;
                            (m.visits || []).forEach(function(v) {
                                if (v.status && v.status.indexOf('Completed') === 0) completedCount++;
                                if (v.status === 'Missed') missedCount++;
                            });
                            if (completedCount >= 5 || missedCount <= 1) stillOnTrack++;
                        });
                        var totalEligible = eligibleMothers.length;

                        return Object.assign({
                            username: uLower,
                            display_name: displayName,
                            last_active_days: lastActiveDays,
                            last_active_date: lastActiveDate,
                            cases_registered: motherCount,
                            eligible_mothers: totalEligible,
                            first_gs_score: null,  // populated below from gs_forms pipeline
                            post_test_attempts: null,
                            followup_rate: fuFlw.completion_rate || 0,
                            ebf_pct: ebfPct != null ? ebfPct : null,
                            revisit_distance_km: gpsFlw.avg_case_distance_km != null ? Math.round(gpsFlw.avg_case_distance_km * 100) / 100 : null,
                            median_meters_per_visit: medianMeters != null ? medianMeters : null,
                            median_minutes_per_visit: medianMinutes != null ? medianMinutes : null,
                            cases_still_eligible: {
                                eligible: stillOnTrack,
                                total: totalEligible,
                                pct: totalEligible > 0 ? Math.round(stillOnTrack / totalEligible * 100) : 0,
                            },
                        }, quality);
                    });

                    // Enrich with GS scores from gs_forms pipeline data
                    var gsFormRows = (pipelines.gs_forms && pipelines.gs_forms.rows) || [];
                    var gsByFlw = {};
                    gsFormRows.forEach(function(row) {
                        var connectId = ((row.computed || row).load_flw_connect_id || '').toLowerCase();
                        var uLower = connectId.toLowerCase();
                        var score = parseFloat((row.computed || row).gs_score);
                        if (!isNaN(score)) {
                            if (!gsByFlw[uLower]) gsByFlw[uLower] = [];
                            gsByFlw[uLower].push({ score: score, date: (row.computed || row).assessment_date || '' });
                        }
                    });
                    overviewFlwSummaries.forEach(function(flw) {
                        var gsEntries = gsByFlw[flw.username] || [];
                        if (gsEntries.length > 0) {
                            // Use the oldest (first) GS score
                            gsEntries.sort(function(a, b) { return (a.date || '').localeCompare(b.date || ''); });
                            flw.first_gs_score = Math.round(gsEntries[0].score);
                        }
                    });

                    var builtDashData = {
                        success: true,
                        gps_data: gpsData,
                        followup_data: followupData,
                        overview_data: {
                            flw_summaries: overviewFlwSummaries,
                            visit_status_distribution: followupData.visit_status_distribution || {},
                        },
                        performance_data: performanceData,
                        active_usernames: activeUsernamesList.map(function(u) { return u.toLowerCase(); }).sort(),
                        flw_names: flwNameMap,
                        open_tasks: instance.state?.open_tasks || {},
                        open_task_usernames: Object.keys(instance.state?.open_tasks || {}),
                        monitoring_session: instance.state?.monitoring_session || null,
                    };

                    setDashData(builtDashData);

                    // Restore worker results from monitoring session if available
                    if (builtDashData.monitoring_session?.flw_results) {
                        setWorkerResults(builtDashData.monitoring_session.flw_results);
                    }
                  } catch(err) {
                    setJobRunning(false);
                    setAnalysisComplete(false);
                    setJobError('Failed to process results: ' + (err.message || String(err)));
                    console.error('[MBW] onComplete error:', err);
                  }
                },
                // onError
                function(error) {
                    setJobRunning(false);
                    setJobError(error || 'Analysis job failed');
                },
                // onCancelled
                function() {
                    setJobRunning(false);
                    setJobError('Analysis job was cancelled');
                }
            );

            jobCleanupRef.current = cleanup;
        }).catch(function(err) {
            setJobRunning(false);
            setJobError('Failed to start job: ' + (err.message || err));
        });
    }, [pipelinesReady, jobRunning, instance.id, instance.state, pipelines, flwNameMap, actions]);

    // Cleanup job stream on unmount
    React.useEffect(function() {
        return function() {
            if (jobCleanupRef.current) jobCleanupRef.current();
        };
    }, []);
    // @v2-replace:data-loading:end"""

    code = _replace_between_inclusive(
        code,
        "// @v2-replace:data-loading:start",
        "// @v2-replace:data-loading:end",
        _PIPELINE_LOADING,
    )

    # =====================================================================
    # R3. Replace sticky header dependency on sseComplete with analysisComplete
    # Marker: // @v2-replace:sticky-deps:start/end in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "// @v2-replace:sticky-deps:start",
        "// @v2-replace:sticky-deps:end",
        "// @v2-replace:sticky-deps:start\n"
        "    }, [activeTab, analysisComplete, showAggregateMap, expandedGps]);\n"
        "    // @v2-replace:sticky-deps:end",
    )

    # =====================================================================
    # R4. Replace Loading + Error UI with pipeline/job loading UI
    # Marker: // @v2-replace:loading-ui:start/end in template.py
    # =====================================================================
    _PIPELINE_LOADING_UI = """    // @v2-replace:loading-ui:start
    // ---- Pipeline loading / Job running / Error state ----
    if (!analysisComplete || !dashData) {
        var visitCount = (pipelines && pipelines.visits && pipelines.visits.rows) ? pipelines.visits.rows.length : 0;
        var regCount = (pipelines && pipelines.registrations && pipelines.registrations.rows) ? pipelines.registrations.rows.length : 0;
        var gsCount = (pipelines && pipelines.gs_forms && pipelines.gs_forms.rows) ? pipelines.gs_forms.rows.length : 0;

        return (
            <div className="space-y-4">
                <div className="bg-white rounded-lg shadow-sm p-6">
                    <h2 className="text-xl font-bold text-gray-900">{instance.state?.title || 'MBW Monitoring V2'}</h2>
                    <p className="text-gray-500 mt-1">Pipeline-based dashboard</p>
                </div>

                {/* Pipeline Status */}
                <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                    <h3 className="text-sm font-semibold text-gray-700 mb-3">Pipeline Data Sources</h3>
                    <div className="space-y-2">
                        {(function() {
                            // Helper: render one pipeline row with smart status detection.
                            // After the SSE stream closes, the server pre-populates all aliases
                            // with {rows:[], metadata:{error:"Not loaded"}}. So:
                            //   pipelines[alias] === undefined  → stream still in progress
                            //   pipelines[alias].metadata?.error → pipeline failed (0 rows + error)
                            //   pipelines[alias].rows.length > 0 → success
                            var pipelineRow = function(alias, label, required) {
                                var p = pipelines && pipelines[alias];
                                var count = p && p.rows ? p.rows.length : null;
                                var failed = count === 0 && p && p.metadata && p.metadata.error;
                                var icon, text;
                                if (count > 0) {
                                    icon = 'fa-circle-check text-green-500';
                                    text = count + ' rows';
                                } else if (failed) {
                                    icon = required ? 'fa-circle-exclamation text-red-500' : 'fa-circle-exclamation text-amber-500';
                                    text = required ? '0 rows (failed — reload page to retry)' : '0 rows (not found)';
                                } else {
                                    // count === null: alias not yet in pipelines → still loading
                                    icon = 'fa-spinner fa-spin text-blue-500';
                                    text = 'Loading...';
                                }
                                return (
                                    <div className="flex items-center gap-3" key={alias}>
                                        <i className={'fa-solid ' + icon}></i>
                                        <span className="text-sm text-gray-700">{label}</span>
                                        <span className="text-xs text-gray-500 ml-auto">{text}</span>
                                    </div>
                                );
                            };
                            return [
                                pipelineRow('visits', 'Visit Forms', true),
                                pipelineRow('registrations', 'Registration Forms', false),
                                pipelineRow('gs_forms', 'Gold Standard Forms', false),
                            ];
                        })()}
                    </div>
                </div>

                {/* Visit pipeline failure prompt */}
                {(function() {
                    var vp = pipelines && pipelines.visits;
                    var visitsFailed = vp && vp.rows && vp.rows.length === 0 && vp.metadata && vp.metadata.error;
                    if (!visitsFailed) return null;
                    return (
                        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                            <div className="flex items-center gap-2 text-red-800 mb-2">
                                <i className="fa-solid fa-circle-exclamation"></i>
                                <span className="font-medium">Visit Forms pipeline failed to load</span>
                            </div>
                            <p className="text-sm text-red-700 mb-3">
                                The visits pipeline returned 0 rows. This may be a temporary error or a large
                                dataset still being indexed. Reload the page to retry.
                            </p>
                            <button onClick={function() { window.location.reload(); }}
                                    className="px-4 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700">
                                <i className="fa-solid fa-rotate-right mr-1"></i> Reload &amp; Retry
                            </button>
                        </div>
                    );
                })()}

                {/* Error State */}
                {jobError && (
                    <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                        <div className="flex items-center gap-2 text-red-800">
                            <i className="fa-solid fa-circle-exclamation"></i>
                            <span className="font-medium">{jobError}</span>
                        </div>
                        <div className="mt-3">
                            <button onClick={function() { setJobError(null); runAnalysis(); }}
                                    disabled={!pipelinesReady}
                                    className="px-4 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 disabled:opacity-50">
                                Retry Analysis
                            </button>
                        </div>
                    </div>
                )}

                {/* Job Running State */}
                {jobRunning && (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                        <div className="flex items-center gap-3 mb-3">
                            <div className="animate-spin h-5 w-5 border-2 border-blue-600 border-t-transparent rounded-full"></div>
                            <span className="font-medium text-blue-800">Running analysis...</span>
                        </div>
                        <div className="space-y-1 text-sm text-blue-700 max-h-40 overflow-y-auto">
                            {jobMessages.map(function(msg, i) {
                                return <div key={i}>{msg}</div>;
                            })}
                        </div>
                    </div>
                )}

                {/* Run Analysis Button */}
                {!jobRunning && !jobError && pipelinesReady && (
                    <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                        <div className="flex items-center justify-between">
                            <div>
                                <span className="font-medium text-green-800">All pipelines loaded</span>
                                <p className="text-sm text-green-600 mt-1">
                                    {visitCount} visits, {regCount} registrations, {gsCount} GS forms loaded.
                                </p>
                            </div>
                            <button onClick={runAnalysis}
                                    className="px-6 py-2.5 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 shadow-sm">
                                <i className="fa-solid fa-play mr-2"></i> Run Analysis
                            </button>
                        </div>
                    </div>
                )}

                {/* Waiting for pipelines — only when at least one succeeded and visits hasn't failed */}
                {!jobRunning && !jobError && !pipelinesReady && pipelinesPartial && !visitsFailed && (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                        <div className="flex items-center gap-3">
                            <div className="animate-spin h-5 w-5 border-2 border-blue-600 border-t-transparent rounded-full"></div>
                            <span className="font-medium text-blue-800">Waiting for all pipeline data to load...</span>
                        </div>
                    </div>
                )}

                {!jobRunning && !jobError && !pipelinesPartial && (
                    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                        <div className="flex items-center gap-3">
                            <div className="animate-spin h-5 w-5 border-2 border-gray-400 border-t-transparent rounded-full"></div>
                            <span className="font-medium text-gray-600">Initializing pipeline data sources...</span>
                        </div>
                    </div>
                )}
            </div>
        );
    }
    // @v2-replace:loading-ui:end"""

    code = _replace_between_inclusive(
        code,
        "// @v2-replace:loading-ui:start",
        "// @v2-replace:loading-ui:end",
        _PIPELINE_LOADING_UI,
    )

    # =====================================================================
    # R5. Replace OAuth retry button (reload page instead of SSE refresh)
    # Marker: {/* @v2-replace:oauth-retry:start/end */} in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "{/* @v2-replace:oauth-retry:start */}",
        "{/* @v2-replace:oauth-retry:end */}",
        "{/* @v2-replace:oauth-retry:start */}\n"
        '                    <button onClick={function() { window.location.reload(); }}\n'
        '                            className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700">\n'
        '                        <i className="fa-solid fa-rotate-right mr-1"></i> Retry\n'
        "                    </button>\n"
        "                    {/* @v2-replace:oauth-retry:end */}",
    )

    # =====================================================================
    # R6. Replace resetFilters (remove SSE refresh logic)
    # Marker: // @v2-replace:reset-filters:start/end in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "// @v2-replace:reset-filters:start",
        "// @v2-replace:reset-filters:end",
        "// @v2-replace:reset-filters:start\n"
        "    var resetFilters = function() {\n"
        "        setFilterFlws([]);\n"
        "        setFilterMothers([]);\n"
        "        setAppVersionOp('gt');\n"
        "        setAppVersionVal('14');\n"
        "        setAppliedAppVersionOp('gt');\n"
        "        setAppliedAppVersionVal('14');\n"
        "    };\n"
        "    // @v2-replace:reset-filters:end",
    )

    # =====================================================================
    # R7. Replace cache indicator text
    # Marker: {/* @v2-replace:cache-indicator:start/end */} in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "{/* @v2-replace:cache-indicator:start */}",
        "{/* @v2-replace:cache-indicator:end */}",
        "{/* @v2-replace:cache-indicator:start */}\n"
        "                {dashData && (\n"
        '                    <div className="mt-2 text-xs text-gray-400">Data loaded via pipeline analysis</div>\n'
        "                )}\n"
        "                {/* @v2-replace:cache-indicator:end */}",
    )

    # =====================================================================
    # R8. Replace tab bar actions (snapshot badge + refresh → re-run analysis)
    # Marker: {/* @v2-replace:tab-bar-actions:start/end */} in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "{/* @v2-replace:tab-bar-actions:start */}",
        "{/* @v2-replace:tab-bar-actions:end */}",
        "{/* @v2-replace:tab-bar-actions:start */}\n"
        '                <div className="flex items-center gap-3 ml-auto">\n'
        "                    <button onClick={function() {\n"
        "                        setDashData(null);\n"
        "                        setAnalysisComplete(false);\n"
        "                        setJobMessages([]);\n"
        "                        setJobError(null);\n"
        "                    }} disabled={jobRunning}\n"
        "                    className={'inline-flex items-center gap-1 px-3 py-1.5 text-sm"
        " font-medium rounded-md border transition-colors ' +\n"
        "                        (analysisComplete && !jobRunning\n"
        "                            ? 'text-blue-700 bg-blue-50 border-blue-200 hover:bg-blue-100'\n"
        "                            : 'text-gray-400 bg-gray-50 border-gray-200 cursor-not-allowed')}>\n"
        "                        {'\\u21BB'} Re-run Analysis\n"
        "                    </button>\n"
        "                </div>\n"
        "                {/* @v2-replace:tab-bar-actions:end */}",
    )

    # =====================================================================
    # R9. Replace Apply button SSE refresh with no-op comment
    # Marker: // @v2-replace:apply-refresh:start/end in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "// @v2-replace:apply-refresh:start",
        "// @v2-replace:apply-refresh:end",
        "// @v2-replace:apply-refresh:start\n"
        "                                    // App version filter applied (no SSE refresh needed in v2)\n"
        "                                    // @v2-replace:apply-refresh:end",
    )

    # =====================================================================
    # R10. Replace drill-down snapshot conditional with simple message
    # Marker: {/* @v2-replace:snapshot-check:start/end */} in template.py
    # =====================================================================
    code = _replace_between_inclusive(
        code,
        "{/* @v2-replace:snapshot-check:start */}",
        "{/* @v2-replace:snapshot-check:end */}",
        "{/* @v2-replace:snapshot-check:start */}\n"
        "                                                                {'No due visits found for this FLW.'}\n"
        "                                                                {/* @v2-replace:snapshot-check:end */}",
    )

    # =====================================================================
    # Safety check: ensure no SSE state refs remain in the final code
    # =====================================================================
    _sse_terms = [
        "sseComplete",
        "sseError",
        "sseMessages",
        "sseAuthorizeUrl",
        "sseCleanupRef",
        "sseSectionsRef",
        "sseAuthRequired",
        "fromSnapshot",
        "refreshTrigger",
        "bustCacheRef",
        "setSseComplete",
        "setSseError",
        "setSseMessages",
        "setSseAuthorizeUrl",
        "setSseAuthRequired",
        "setFromSnapshot",
        "setRefreshTrigger",
        "EventSource",
    ]
    for term in _sse_terms:
        if term in code:
            import logging

            logging.getLogger(__name__).warning(
                "MBW V2 render code still contains SSE reference: %s", term
            )

    return code


RENDER_CODE = _build_v2_render_code()

TEMPLATE = {
    "key": "mbw_monitoring_v2",
    "name": "MBW Monitoring V2",
    "description": "Pipeline-based MBW monitoring with GPS analysis, follow-up rates, and FLW assessment",
    "icon": "fa-baby",
    "color": "pink",
    "definition": DEFINITION,
    "render_code": RENDER_CODE,
    "pipeline_schemas": PIPELINE_SCHEMAS,
}
