"""Run with the installed native interpreter, passing a new task-owned directory."""
import importlib.util
import json
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
root.mkdir()  # Refuse existing state, including another owner's directory.
(root / "workspace").mkdir()
(root / "home").mkdir()
(root / "home/settings.yaml").write_text("bundle:\n  app: []\n")


def source(name):
    return Path(importlib.util.find_spec("amplifier_module_" + name.replace("-", "_")).origin).parent


bundle = root / "bundle.yaml"
bundle.write_text(f"""bundle:
  name: installed-operations-qualification
  version: 1.0.0
session:
  orchestrator:
    module: loop-live
    source: {source('loop-live')}
  context:
    module: context-simple
    source: {source('context-simple')}
providers:
  - module: provider-operations-fixture
    source: {source('provider-operations-fixture')}
    config: {{}}
tools:
  - module: tool-bash
    source: {source('tool-bash')}
    config: {{managed_processes: true, managed_stdin: true, safety_profile: unrestricted}}
""")
(root / "config.json").write_text(json.dumps({"home": str(root / "home"), "appHome": str(root / "app"),
                                           "bundle": str(bundle), "startupTimeout": 90}))
print(json.dumps({"config": str(root / "config.json"), "workspace": str(root / "workspace")}))
