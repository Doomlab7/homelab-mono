# SPDX-FileCopyrightText: 2023-present Pypeaday <pypeaday@pm.me>
#
# SPDX-License-Identifier: MIT
import click

from ..__about__ import __version__


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
)
@click.version_option(version=__version__, prog_name="Home Automation Pipelines")
@click.pass_context
def home_automation_pipelines(ctx: click.Context):
    click.echo("Hello world!")
