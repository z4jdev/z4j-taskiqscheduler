"""z4j-taskiqscheduler - scheduler adapter for taskiq schedule sources."""

from __future__ import annotations

from z4j_taskiqscheduler.scheduler import TaskiqSchedulerAdapter

try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("z4j-taskiqscheduler")
except PackageNotFoundError:  # source checkout, no installed metadata
    from z4j_core.version import __version__  # type: ignore[no-redef]

__all__ = ["TaskiqSchedulerAdapter", "__version__"]
