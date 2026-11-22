"""addrlink: parse-free, inverted-index address matching.

Implements and extends Zhang, Churchill & Ng (arXiv:1708.01402). See docs/SPEC.md.
"""

__version__ = "0.1.0"

from .config import MatchConfig  # noqa: E402
from .api import Matcher, MatchResult  # noqa: E402

__all__ = ["Matcher", "MatchConfig", "MatchResult", "__version__"]
