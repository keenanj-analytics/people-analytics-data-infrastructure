# Spec deviations

The `JustKaizen_AI_Data_Generation_Spec.md` is internally inconsistent in
places. Where the generation pipeline had to deviate, the deviation is
documented at the source (typically the relevant Python script's
docstring) and summarized below.

## Headcount reconciliation (Stage 1)

Section 5's archetype percentages don't algebraically reconcile with
`568 total / 380 active / 188 terminated`. With the rigid 75-person
Q1 2023 layoff plus the four 100%-terminated archetypes at their stated
proportions, the stated rates produce only ~291 active vs target 380.

**Resolution:** Steady Contributor inflated 25% → 45% (256 → after the
user-reviewed adjustment 214); Steady and Internal Mover at 100% active
(lost the 15% / 10% term per spec); the four 100%-terminated archetypes
resized to the user-approved counts (Early Churner 35, Top Performer
Flight Risk 25, Performance Managed Out 15, Manager Change Casualty 20).

Documented in `01_generate_employee_profiles.py` docstring.

## Supplemental terminated profiles (Stage 1.5)

To restore the spec's voluntary turnover semantics for Steady Contributor
(15% term) and Internal Mover (10% term), 36 supplemental terminated
profiles were appended (32 Steady + 4 Internal Mover, all hired
2021-2022, all Voluntary). Total goes from 568 → 604 / 380 / 224.

These 36 are the only profiles with a `manager_id` populated at Stage 1;
the other 568 had `manager_id` populated in Stage 2b's hierarchy
resolution.

## Section 3 sub-dept × level grid — 13 residual delta cells

The 2c step-2 alignment closes the IC1+39 / IC4-31 starting-level skew
via archetype-budget-respecting promotions and sub-dept rebalancing. 13
cells remain off the Section 3 + uplift target after the alignment runs:

- **Founder IC track at IC5 outside Engineering** — Section 3 only has
  IC5 cells in Engineering. CS / Product founder IC5 designations spill
  into non-Section-3 cells (e.g., CS Implementation IC5, Product Design
  IC5).
- **People L&D M1 vacancy** — Section 3 row sums imply 4 People M1;
  Section 2 says 3. 2a designated 3 per Section 2, leaving Section 3's
  L&D M1 unfilled.
- **Sales SDR IC1 shortage (-5)** — flex pool doesn't have 10
  IC1-starting Sales profiles after the no-demotion guard. Spillover
  lands in adjacent SDR / AE / SE cells.

Documented in `05_align_subdept_level_grid.py`.

## Performance Managed Out — 3 SR1 soft violations

Spec says Performance Managed Out profiles should show declining ratings
in their last 2-3 cycles. Section 7's "late" distribution still has a
15% Meets weight, so the random walk can occasionally produce a
non-declining last cycle. 3 of 15 PMO profiles hit this. Documented in
the validator output as a soft (informational) rule.

To strictly enforce decline, the late distribution would need to drop
the Meets weight to 0 — not done here because it would deviate from
Section 7's stated probabilities.

## Row count vs spec targets

| Table | Spec target | Actual | Note |
|---|---|---|---|
| `raw_employees` | ~568 | 604 | Spec uses ~ (approximate). Reconciliation math forced 604. |
| `raw_job_history` | 800-1,000 | 1,045 | Slightly over due to organic Manager Change events bridging structural at-hire vs current manager differences. |
| `raw_compensation` | 900-1,100 | 2,312 | Significantly over because the spec's row math appears to omit Annual Review records. With the per-Jan-15 review rule the count roughly doubles. Documented in `09_build_raw_compensation.py`. |
| `raw_performance` | 2,500-3,000 | 2,676 | Within range. |
| `raw_recruiting` | 8,000-10,000 | 9,348 | Within range. Funnel rates are inflated above Section 9 targets because the Applied-stage rejection pool was capped to keep total volume in the spec range. |
| `raw_engagement` | 3,500-4,000 | 3,024 | Slightly under. Sub-department rows (Section 12: "only when 5+ respondents") were not generated; including them would land in spec range. |
