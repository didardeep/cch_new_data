def _build_parameter_recommendations(problem_type, root_cause, trend_summary, nearest):
    text = f"{root_cause}\n{trend_summary}"
    flags = _infer_issue_flags(text)

    bw = nearest.get("bandwidth_mhz") if nearest else None
    gain = nearest.get("antenna_gain_dbi") if nearest else None
    eirp = nearest.get("rf_power_eirp_dbm") if nearest else None
    height = nearest.get("antenna_height_agl_m") if nearest else None
    tilt = nearest.get("e_tilt_degree") if nearest else None
    crs = nearest.get("crs_gain") if nearest else None

    recs = []

    if "off_air" in flags:
        recs.append(
            f"**Restore Site to ON AIR**: Resolve active alarms first and validate recovery against KPI trends; "
            f"then re-check RF parameters (EIRP {_value_or_na(eirp,' dBm')}, E-tilt {_value_or_na(tilt,'°')}) for post-recovery tuning."
        )

    if "congestion" in flags and bw is not None:
        target_bw = bw + (10 if bw <= 10 else 5)
        recs.append(
            f"**Increase Bandwidth**: Bandwidth is {bw} MHz; expand to {target_bw} MHz (if spectrum/license allows) to reduce PRB congestion and improve throughput."
        )
    elif "congestion" in flags:
        recs.append(
            "**Increase Bandwidth**: Bandwidth value missing; increase carrier bandwidth (e.g., +5 to +10 MHz) to reduce PRB congestion and improve throughput."
        )

    if "interference" in flags and tilt is not None and eirp is not None:
        target_tilt = _adjust_value(tilt, 1, min_val=0)
        target_eirp = _adjust_value(eirp, -1)
        recs.append(
            f"**Reduce Overshoot/Interference**: E-tilt is {tilt}° and EIRP is {eirp} dBm; "
            f"increase tilt to {target_tilt}° and lower EIRP to {target_eirp} dBm to reduce pilot pollution and stabilize SINR."
        )
    elif "coverage" in flags and tilt is not None and eirp is not None:
        target_tilt = _adjust_value(tilt, -1, min_val=0)
        target_eirp = _adjust_value(eirp, 1)
        recs.append(
            f"**Improve Coverage/RSS**: E-tilt is {tilt}° and EIRP is {eirp} dBm; "
            f"reduce tilt to {target_tilt}° and raise EIRP to {target_eirp} dBm to lift RSRP and improve coverage."
        )

    if "handover" in flags and crs is not None:
        target_crs = _adjust_value(crs, 3)
        recs.append(
            f"**Stabilize Handover**: CRS Gain is {crs}; raise to {target_crs} to improve reference signal quality and reduce RLF/call drops during mobility."
        )
    elif "access" in flags and gain is not None:
        target_gain = _adjust_value(gain, 1)
        recs.append(
            f"**Improve Call Accessibility**: Antenna Gain is {gain} dBi; increase to {target_gain} dBi (or swap to higher-gain antenna) to improve call setup KPIs."
        )

    if "latency" in flags and height is not None:
        target_height = _adjust_value(height, 2)
        recs.append(
            f"**Optimize Antenna Height**: Antenna height is {height} m AGL; adjust to {target_height} m to improve line-of-sight and reduce latency spikes."
        )

    # Ensure 4–5 points using unique parameter-based fill-up lines
    padding_pool = []
    if tilt is not None and eirp is not None:
        padding_pool.append(
            f"**RF Parameter Alignment**: Validate E-tilt ({tilt}°) and EIRP ({eirp} dBm) against the RCA-identified degradation window and confirm KPI recovery within 24 hours post-adjustment."
        )
    if bw is not None:
        padding_pool.append(
            f"**Capacity Confirmation**: Confirm bandwidth at {bw} MHz is adequate; if PRB utilization exceeds 70%, schedule a bandwidth expansion to {round(bw + 5, 1)} MHz."
        )
    if crs is not None:
        padding_pool.append(
            f"**CRS Gain Verification**: CRS Gain is currently {crs}; validate reference signal power across all sectors and adjust if RSRP drops below -105 dBm at cell edge."
        )
    if height is not None:
        padding_pool.append(
            f"**Antenna Height Review**: Antenna height is {height} m AGL; verify line-of-sight coverage and adjust mounting height if near-field obstruction is detected."
        )
    if gain is not None:
        padding_pool.append(
            f"**Antenna Gain Audit**: Antenna Gain is {gain} dBi; cross-check with drive test data and replace with higher-gain antenna if signal levels are below threshold."
        )
    # Generic always-available options
    padding_pool += [
        "**Drive Test Validation**: Conduct a drive test in the affected area post-parameter changes to confirm KPI recovery and customer experience improvement.",
        "**Neighbor Cell Audit**: Review neighbor cell list for missing or unoptimized neighbors that may cause handover failures and increase call drops.",
        "**Scheduler Tuning**: Adjust CQI-based scheduler parameters to prioritize users with degraded signal quality and improve cell-edge throughput by up to 15%.",
    ]

    for pad in padding_pool:
        if len(recs) >= 5:
            break
        if pad not in recs:
            recs.append(pad)

    return recs[:5]


def _recommendation_has_params(text: str):
    t = (text or "").lower()
    param_hits = 0
    for p in ["bandwidth", "antenna gain", "eirp", "rf power", "antenna height", "e-tilt", "tilt", "crs gain"]:
        if p in t:
            param_hits += 1
    has_numbers = bool(re.search(r"\d+(\.\d+)?", t))
    return param_hits >= 2 and has_numbers


def _filter_rca_lines(lines):
    generic_patterns = [
