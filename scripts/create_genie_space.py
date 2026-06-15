#!/usr/bin/env python3
"""Create the 'Ask the Portfolio' Genie space over the gold/silver marts. Reproducible.
Usage: python3 scripts/create_genie_space.py [profile] [warehouse_id] [catalog] [schema]
Prints the space_id on success. Uses the genie-rooms skill's GenieSpaceBuilder."""
import sys, json, subprocess, pathlib

prof = sys.argv[1] if len(sys.argv) > 1 else "DEV"
wh = sys.argv[2] if len(sys.argv) > 2 else "a3b61648ea4809e3"
cat = sys.argv[3] if len(sys.argv) > 3 else "lr_dev_aws_us_catalog"
sch = sys.argv[4] if len(sys.argv) > 4 else "bricksurance_re"

BUILDER = pathlib.Path.home() / ".vibe/marketplace/plugins/fe-internal-tools/skills/genie-rooms/resources"
sys.path.insert(0, str(BUILDER))
from genie_space_builder import GenieSpaceBuilder  # noqa

fqn = f"{cat}.{sch}"
space = GenieSpaceBuilder(
    title="Ask the Portfolio — Bricksurance Re",
    description="Natural-language analytics over the reinsurance portfolio: capacity vs appetite by peak zone, capital/solvency, submissions on the desk, and catastrophe-event response.",
    warehouse_id=wh,
)
space.set_instructions(
    "You answer questions about a treaty reinsurer's book. gold_portfolio_position has per peak-zone current "
    "1-in-200 PML vs appetite (utilisation_pct, headroom_eur, rag). gold_capital_position has the diversified "
    "BSCR, eligible own funds and solvency_ratio_pct. gold_event_response has the latest cat-event loss "
    "(gross/net, treaties responding, top cedant). silver_submissions is the renewal-desk pipeline (status, "
    "zone, structure, RoL). silver_inforce_treaties is the in-force book. Money is EUR; report PML/appetite in "
    "millions; utilisation and solvency as percentages."
)
for t in ["gold_portfolio_position", "gold_capital_position", "gold_event_response",
          "silver_submissions", "silver_inforce_treaties"]:
    space.add_table(f"{fqn}.{t}")
space.validate()

payload = {
    "title": "Ask the Portfolio — Bricksurance Re",
    "description": "Reinsurance portfolio analytics: capacity vs appetite, capital, submissions, cat-event response.",
    "parent_path": "/Workspace/Users/laurence.ryszka@databricks.com",
    "warehouse_id": wh,
    "serialized_space": space.to_json(),
}
open("/tmp/create_genie_space.json", "w").write(json.dumps(payload))
out = subprocess.run(["databricks", "api", "post", "/api/2.0/genie/spaces", "--profile", prof,
                      "--json", "@/tmp/create_genie_space.json"], capture_output=True, text=True)
print(out.stdout[:800] or out.stderr[:800])
try:
    print("SPACE_ID:", json.loads(out.stdout)["space_id"])
except Exception:
    pass
