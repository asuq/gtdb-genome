"""Shared batching helpers for workflow execution."""

from __future__ import annotations

from collections.abc import Callable

from gtdb_genomes.workflow_execution_models import (
    AccessionPlan,
    DirectBatchPhaseResult,
)


type RequestPlanGroup = tuple[str, tuple[AccessionPlan, ...]]
type RequestPlanGroups = tuple[RequestPlanGroup, ...]


def group_plans_by_download_request_accession(
    plans: tuple[AccessionPlan, ...],
) -> RequestPlanGroups:
    """Group accession plans by request accession in first-seen order."""

    grouped_plans: dict[str, list[AccessionPlan]] = {}
    for plan in plans:
        grouped_plans.setdefault(plan.download_request_accession, []).append(plan)
    return tuple(
        (download_request_accession, tuple(group))
        for download_request_accession, group in grouped_plans.items()
    )


def split_request_plan_groups(
    plan_groups: RequestPlanGroups,
) -> tuple[RequestPlanGroups, ...]:
    """Bisect one request-group tuple into smaller retry subsets."""

    if len(plan_groups) <= 1:
        return (plan_groups,)
    midpoint = len(plan_groups) // 2
    return (
        plan_groups[:midpoint],
        plan_groups[midpoint:],
    )


def execute_decomposed_direct_phase(
    initial_groups: RequestPlanGroups,
    run_phase: Callable[[RequestPlanGroups], DirectBatchPhaseResult],
) -> DirectBatchPhaseResult:
    """Run one direct phase while recursively isolating unresolved groups."""

    if not initial_groups:
        return DirectBatchPhaseResult(
            executions={},
            unresolved_groups=(),
            shared_failures=(),
        )

    pending_batches = [initial_groups]
    executions = {}
    shared_failures = []
    unresolved_groups: list[RequestPlanGroup] = []

    while pending_batches:
        current_groups = pending_batches.pop(0)
        if not current_groups:
            continue
        phase_result = run_phase(current_groups)
        executions.update(phase_result.executions)
        shared_failures.extend(phase_result.shared_failures)
        if not phase_result.unresolved_groups:
            continue
        if len(phase_result.unresolved_groups) <= 1:
            unresolved_groups.extend(phase_result.unresolved_groups)
            continue
        pending_batches.extend(
            split_request_plan_groups(phase_result.unresolved_groups),
        )

    return DirectBatchPhaseResult(
        executions=executions,
        unresolved_groups=tuple(unresolved_groups),
        shared_failures=tuple(shared_failures),
    )
