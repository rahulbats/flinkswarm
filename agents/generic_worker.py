"""Backwards-compatible entrypoint. The implementation lives in the package.

    python agents/generic_worker.py        # same as: python -m flinkswarm.worker
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flinkswarm.worker import main  # noqa: E402

if __name__ == "__main__":
    main()
