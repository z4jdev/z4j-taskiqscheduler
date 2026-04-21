# z4j-taskiqscheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiqscheduler.svg)](https://pypi.org/project/z4j-taskiqscheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiqscheduler.svg)](https://pypi.org/project/z4j-taskiqscheduler/)
[![License](https://img.shields.io/pypi/l/z4j-taskiqscheduler.svg)](https://github.com/z4jdev/z4j-taskiqscheduler/blob/main/LICENSE)


z4j scheduler adapter for taskiq's schedule sources
(`LabelScheduleSource`, custom backends).

```python
from taskiq.schedule_sources import LabelScheduleSource
from z4j_taskiq import TaskiqEngineAdapter
from z4j_taskiqscheduler import TaskiqSchedulerAdapter

source = LabelScheduleSource(broker)

# In your z4j-bare bootstrap:
from z4j_bare import install_agent
install_agent(
    engines=[TaskiqEngineAdapter(broker=broker)],
    schedulers=[TaskiqSchedulerAdapter(source=source)],
)
```

## Capabilities

- ✅ List + read schedules from any taskiq schedule source
- ✅ Delete (when source supports it)
- ❌ Create / update - taskiq sources don't expose a generic
  add-from-spec API; LabelScheduleSource is read-only at runtime
  by design.

Apache 2.0.

## License

Apache 2.0 - see [LICENSE](LICENSE). This package is deliberately permissively licensed so that proprietary Django / Flask / FastAPI applications can import it without any license concerns.

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Source: <https://github.com/z4jdev/z4j-taskiqscheduler>
- Issues: <https://github.com/z4jdev/z4j-taskiqscheduler/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
