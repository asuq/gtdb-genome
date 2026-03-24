"""Shared batching helpers for workflow execution."""

from __future__ import annotations

from gtdb_genomes.workflow_execution_models import AccessionPlan


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


def build_next_wave_batches(
    unresolved_groups: RequestPlanGroups,
) -> tuple[RequestPlanGroups, ...]:
    """Return the next-wave batches for one unresolved direct batch result."""

    if not unresolved_groups:
        return ()
    if len(unresolved_groups) <= 1:
        return (unresolved_groups,)
    return split_request_plan_groups(unresolved_groups)
