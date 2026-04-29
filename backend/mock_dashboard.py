"""
mock_dashboard.py
=================
Hardcoded dashboard data for demo user ansh.grwl.22@gmail.com.
Returns realistic data based on 5 tickets so the Performance Command Center
looks populated. This file does NOT change any real logic.
"""

from datetime import datetime, timedelta

MOCK_EMAIL = "ansh.grwl.22@gmail.com"


def get_mock_dashboard(agent_name="Ansh", agent_location="Gurgaon", agent_domain="broadband"):
    """Return a complete dashboard payload based on 5 hardcoded tickets."""

    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    return {
        # ── Summary cards ──
        "summary": {
            "total_tickets": 5,
            "resolved": 3,
            "open": 2,
            "total_feedback": 3,
        },

        # ── KPI tiles ──
        "kpis": {
            "mttr": 4.2,
            "sla_compliance_rate": 80.0,
            "first_contact_resolution": 60.0,
            "csat": 4.1,
            "csat_pct": 66.7,
            "reopen_rate": 0.0,
            "hs_incident_resolution_time": 2.8,
            "hs_incident_response_time": 0.6,
            "complaint_resolution_time": 4.2,
            "rca_timely_completion": 100.0,
            "avg_aging_hours": 3.5,
        },

        # ── Monthly ticket flow (last 6 months) ──
        "monthly_trend": [
            {"month": (now - timedelta(days=150)).strftime("%b %Y"), "created": 0, "resolved": 0},
            {"month": (now - timedelta(days=120)).strftime("%b %Y"), "created": 0, "resolved": 0},
            {"month": (now - timedelta(days=90)).strftime("%b %Y"),  "created": 1, "resolved": 1},
            {"month": (now - timedelta(days=60)).strftime("%b %Y"),  "created": 1, "resolved": 1},
            {"month": (now - timedelta(days=30)).strftime("%b %Y"),  "created": 2, "resolved": 1},
            {"month": now.strftime("%b %Y"),                         "created": 1, "resolved": 0},
        ],

        # ── Priority distribution ──
        "priority_chart": [
            {"name": "critical", "value": 1},
            {"name": "high",     "value": 2},
            {"name": "medium",   "value": 1},
            {"name": "low",      "value": 1},
        ],

        # ── SLA compliance by priority ──
        "sla_priority_chart": [
            {"priority": "critical", "compliance": 100.0},
            {"priority": "high",     "compliance": 100.0},
            {"priority": "medium",   "compliance": 100.0},
            {"priority": "low",      "compliance": 0.0},
        ],

        # ── Customer sentiment ──
        "sentiment": [
            {"name": "Excellent", "value": 1},
            {"name": "Good",     "value": 1},
            {"name": "Neutral",  "value": 1},
            {"name": "Poor",     "value": 0},
            {"name": "Bad",      "value": 0},
        ],

        # ── Category resolution ──
        "category_resolution": [
            {"category": "Slow Speed / No Connectivity", "total": 2, "resolved": 1, "rate": 50.0, "sla_rate": 100.0},
            {"category": "Frequent Disconnections",      "total": 1, "resolved": 1, "rate": 100.0, "sla_rate": 100.0},
            {"category": "Router / Equipment Problems",  "total": 1, "resolved": 1, "rate": 100.0, "sla_rate": 100.0},
            {"category": "Billing & Plan Issues",        "total": 1, "resolved": 0, "rate": 0.0,   "sla_rate": 0.0},
        ],

        # ── Agent efficiency ──
        "efficiency_metrics": {
            "avg_msgs_per_ticket": 5.4,
            "agent_msg_pct": 38.0,
            "ai_msg_pct": 62.0,
            "avg_first_response_hrs": 0.5,
            "fastest_response_hrs": 0.1,
            "tickets_with_response": 5,
            "total_conversations": 5,
            "resolution_rate": 60.0,
        },

        # ── Customer tier distribution ──
        "customer_tiers": [
            {"tier": "Platinum", "total": 0, "resolved": 0, "rate": 0.0,   "avg_hours": 0.0},
            {"tier": "Gold",     "total": 1, "resolved": 1, "rate": 100.0, "avg_hours": 2.1},
            {"tier": "Silver",   "total": 2, "resolved": 1, "rate": 50.0,  "avg_hours": 4.5},
            {"tier": "Bronze",   "total": 2, "resolved": 1, "rate": 50.0,  "avg_hours": 5.8},
        ],

        # ── AI vs Human Agent ──
        "ai_vs_agent": {
            "total_conversations": 5,
            "ai_resolved": 2,
            "ai_resolution_rate": 40.0,
            "ai_avg_time": 1.5,
            "escalated_to_agent": 3,
            "escalation_rate": 60.0,
            "agent_resolved": 1,
            "agent_resolution_rate": 33.3,
            "agent_avg_time": 4.2,
        },

        # ── Weekly activity heatmap (7 days x 24 hours) ──
        "heatmap": [
            [0,0,0,0,0,0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
        ],
        "heatmap_resolved": [
            [0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
        ],

        # ── Performance score + radar ──
        "performance_score": 68.5,
        "badges": [
            {"tag": "Quick Responder", "icon": "zap"},
            {"tag": "SLA Compliant",   "icon": "shield"},
        ],
        "perf_radar": [
            {"axis": "Speed",          "value": 78.0, "detail": "Avg 4.2h MTTR"},
            {"axis": "Quality",        "value": 100.0, "detail": "0% reopen rate"},
            {"axis": "SLA",            "value": 80.0, "detail": "4/5 within SLA"},
            {"axis": "Satisfaction",   "value": 66.7, "detail": "4.1/5 avg rating"},
            {"axis": "Responsiveness", "value": 85.0, "detail": "Avg 0.5h first response"},
            {"axis": "Workload",       "value": 50.0, "detail": "5 tickets handled"},
        ],

        # ── Agent info ──
        "agent_name": agent_name,
        "agent_location": agent_location,
        "agent_domain": agent_domain,

        # ── Issue hotspots ──
        "issue_hotspots": [
            {"name": "Slow Speed / No Connectivity", "total": 2, "resolved": 1, "open": 1},
            {"name": "Frequent Disconnections",      "total": 1, "resolved": 1, "open": 0},
            {"name": "Router / Equipment Problems",  "total": 1, "resolved": 1, "open": 0},
            {"name": "Billing & Plan Issues",        "total": 1, "resolved": 0, "open": 1},
        ],

        # ── Zone-wise performance ──
        "zone_data": [
            {"zone": "Gurgaon",  "total": 3, "resolved": 2, "rate": 66.7},
            {"zone": "Delhi",    "total": 1, "resolved": 1, "rate": 100.0},
            {"zone": "Noida",    "total": 1, "resolved": 0, "rate": 0.0},
        ],
        "state_data": [
            {"state": "Haryana",       "total": 3, "resolved": 2, "rate": 66.7},
            {"state": "Delhi",         "total": 1, "resolved": 1, "rate": 100.0},
            {"state": "Uttar Pradesh", "total": 1, "resolved": 0, "rate": 0.0},
        ],
        "detected_country": "India",

        # ── SLA risk predictor ──
        "sla_risk": [
            {
                "ticket_id": 1001, "reference": "TKT-BB-001", "priority": "high",
                "pct_elapsed": 65.0, "remaining_hrs": 2.8, "overdue_hrs": 0,
                "risk": "warning", "category": "Slow Speed / No Connectivity",
                "subcategory": "Broadband", "status": "in_progress",
                "sla_hours": 8, "sla_deadline": (now + timedelta(hours=2, minutes=48)).isoformat(),
                "created_at": (now - timedelta(hours=5, minutes=12)).isoformat(),
            },
            {
                "ticket_id": 1002, "reference": "TKT-BB-002", "priority": "medium",
                "pct_elapsed": 30.0, "remaining_hrs": 16.8, "overdue_hrs": 0,
                "risk": "safe", "category": "Billing & Plan Issues",
                "subcategory": "Broadband", "status": "open",
                "sla_hours": 24, "sla_deadline": (now + timedelta(hours=16, minutes=48)).isoformat(),
                "created_at": (now - timedelta(hours=7, minutes=12)).isoformat(),
            },
        ],
        "sla_risk_summary": {"safe": 1, "warning": 1, "critical": 0, "breached": 0},
        "sla_health_pct": 100.0,
        "sla_priority_dist": [
            {"priority": "high",   "total": 1, "breached": 0, "critical": 0, "warning": 1, "safe": 0},
            {"priority": "medium", "total": 1, "breached": 0, "critical": 0, "warning": 0, "safe": 1},
        ],
        "sla_total_open": 2,

        # ── Category treemap ──
        "category_treemap": [
            {"name": "Slow Speed / No Connectivity", "size": 2, "resolved": 1, "rate": 50.0},
            {"name": "Frequent Disconnections",      "size": 1, "resolved": 1, "rate": 100.0},
            {"name": "Router / Equipment Problems",  "size": 1, "resolved": 1, "rate": 100.0},
            {"name": "Billing & Plan Issues",        "size": 1, "resolved": 0, "rate": 0.0},
        ],

        # ── Today's hourly activity ──
        "hourly_today": [
            {"hour": f"{h:02d}:00", "tickets": 1 if h in (9, 11, 14) else 0}
            for h in range(24)
        ],

        # ── AI insights ──
        "ai_insights": [
            {"type": "info",    "text": "Most complaints: Slow Speed / No Connectivity (2 tickets)"},
            {"type": "success", "text": "SLA compliance at 80% — 4 of 5 tickets within SLA"},
            {"type": "warning", "text": "1 ticket approaching SLA deadline — TKT-BB-001"},
            {"type": "info",    "text": "AI resolved 40% of conversations without escalation"},
        ],

        # ── Predictive forecast (next 7 days) ──
        "forecast": [
            {"day": (now + timedelta(days=i)).strftime("%a %d/%m"),
             "predicted": round(0.8 + (i % 3) * 0.3, 1), "capacity": 5}
            for i in range(1, 8)
        ],

        # ── Burndown ──
        "burndown": {
            "target_rate": 90.0,
            "current_resolved": 3,
            "needed": 2,
            "total": 5,
            "current_rate": 60.0,
        },

        # ── Recent feedbacks ──
        "recent_feedbacks": [
            {
                "rating": 5, "comment": "Issue fixed quickly, great support!",
                "customer": "Rahul S.", "session_id": "sess-101",
                "created_at": (now - timedelta(hours=3)).isoformat(),
                "subprocess": "Slow Speed / No Connectivity",
            },
            {
                "rating": 4, "comment": "Router replaced, working fine now.",
                "customer": "Priya M.", "session_id": "sess-102",
                "created_at": (now - timedelta(hours=8)).isoformat(),
                "subprocess": "Router / Equipment Problems",
            },
            {
                "rating": 3, "comment": "Disconnection issue took a while to resolve.",
                "customer": "Amit K.", "session_id": "sess-103",
                "created_at": (now - timedelta(days=1)).isoformat(),
                "subprocess": "Frequent Disconnections",
            },
        ],
    }
