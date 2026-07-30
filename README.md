# z4j-taskiqscheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiqscheduler.svg?v=1.8.0)](https://pypi.org/project/z4j-taskiqscheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiqscheduler.svg?v=1.8.0)](https://pypi.org/project/z4j-taskiqscheduler/)
[![License](https://img.shields.io/pypi/l/z4j-taskiqscheduler.svg?v=1.8.0)](https://github.com/z4jdev/z4j-taskiqscheduler/blob/main/LICENSE)

The taskiq-scheduler adapter for [z4j](https://z4j.com).

Surfaces taskiq-scheduler periodic jobs on the dashboard's Schedules
page, list, read, delete (where the schedule source supports it).

## Compatibility

- TaskIQ 0.11+ and <1
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| List schedules | every job registered with the taskiq-scheduler source |
| Read | by registered name |
| Delete | when the schedule source implements `delete_schedule` (dynamic sources); label-defined schedules need a code change |
| Boot inventory | full snapshot at agent connect; existing schedules show up without editing |

taskiq-scheduler schedules are typically defined declaratively (label
source, JSON file, or label decorators), so create / update are
intentionally out of scope, those need a deploy round-trip. taskiq
also has no enable/disable toggle or trigger-now primitive, so the
dashboard hides those actions for this adapter.

## Install

```bash
pip install z4j-taskiq z4j-taskiqscheduler
```

```python
from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from z4j_bare import install_agent
from z4j_taskiq import TaskiqEngineAdapter
from z4j_taskiqscheduler import TaskiqSchedulerAdapter

source = LabelScheduleSource(broker)
scheduler = TaskiqScheduler(broker=broker, sources=[source])

install_agent(
    engines=[TaskiqEngineAdapter(broker=broker)],
    schedulers=[TaskiqSchedulerAdapter(source=source)],
    brain_url="https://brain.example.com",
    token="z4j_agent_...",
    project_id="my-project",
)
```

## Pairs with

- [`z4j-taskiq`](https://github.com/z4jdev/z4j-taskiq), engine adapter

## Reliability

- No exception from the adapter ever propagates back into
  taskiq-scheduler or your task code.
- The adapter never rewrites schedule definitions; the only write
  it can perform is an operator-initiated delete, and only when the
  source itself supports it.

## Documentation

Full docs at [z4j.dev/schedulers/taskiq-scheduler/](https://z4j.dev/schedulers/taskiq-scheduler/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-taskiqscheduler/
- Issues: https://github.com/z4jdev/z4j-taskiqscheduler/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
