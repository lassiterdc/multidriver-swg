"""Run the command as `python -m multidriver_swg.workflow`.

The module exists so the command can be started without a console script, which is the form the package's
own tests and any checkout-in-place invocation use. It calls `main()` with no argument, exactly as the
generated wrapper does, and exits with the band `main` returns.
"""

import sys

from multidriver_swg.workflow.cli import main

sys.exit(main())
