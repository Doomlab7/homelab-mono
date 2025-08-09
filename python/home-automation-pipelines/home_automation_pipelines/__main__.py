# SPDX-FileCopyrightText: 2023-present Pypeaday <pypeaday@pm.me>
#
# SPDX-License-Identifier: MIT
import sys

if __name__ == "__main__":
    from .cli import home_automation_pipelines

    sys.exit(home_automation_pipelines())
