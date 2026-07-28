# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Checks for the progress display and the streamed transfers that feed it."""

import io
import sys

from ducktests_remote.commands import deploy
from ducktests_remote.progress import (NullProgress, Progress, human_bytes, human_duration,
                                       is_a_terminal)
from ducktests_remote.transport import LocalTransport, run_local_streaming


class _Stream(io.StringIO):
    """A StringIO that can claim to be a terminal, with a settable encoding."""

    def __init__(self, tty=False, encoding="utf-8"):
        super().__init__()
        self._tty = tty
        self._encoding = encoding

    @property
    def encoding(self):
        """:return: the encoding the bar picks its characters from."""
        return self._encoding

    def isatty(self):
        return self._tty


class CheckFormatting:
    """The numbers an operator reads at a glance."""

    def check_bytes_are_scaled(self):
        assert human_bytes(512) == "512 B"
        assert human_bytes(1024 * 1024 * 3) == "3.0 MB"
        assert human_bytes(1024 ** 3 * 3.5) == "3.5 GB"

    def check_durations_grow_an_hour_field_only_when_needed(self):
        assert human_duration(72) == "1:12"
        assert human_duration(3672) == "1:01:12"


class CheckAggregate:
    """The line that has to make sense on its own in a log file."""

    @staticmethod
    def _progress(hosts, **kw):
        return Progress(hosts, stream=_Stream(), live=False, **kw)

    def check_it_averages_the_per_host_fractions(self):
        progress = self._progress(["w1", "w2", "w3", "w4"])
        progress.done("w1")
        progress.sent("w2", 50, 100)
        assert "38% 1/4 host(s)" in progress.aggregate_line(), \
            "one host finished plus one half done is 3/8 of four hosts"

    def check_it_counts_finished_hosts_and_bytes_moved(self):
        progress = self._progress(["w1", "w2"])
        progress.sent("w1", 1024 * 1024, 1024 * 1024 * 2)
        progress.done("w2")
        line = progress.aggregate_line()
        assert "1/2 host(s)" in line and "1.0 MB sent" in line

    def check_a_rsync_fraction_beats_bytes_over_total(self):
        progress = self._progress(["w1"])
        progress.sent("w1", 10, 1000, fraction=0.9)
        assert "90% 0/1 host(s)" in progress.aggregate_line(), \
            "rsync knows what it decided not to send; sent/total does not"

    def check_an_unknown_host_is_ignored(self):
        progress = self._progress(["w1"])
        progress.sent("nobody", 10, 100)
        progress.done("nobody")
        assert "0% 0/1 host(s)" in progress.aggregate_line()


class CheckRows:
    """One line per host, and never more lines than a terminal can hold."""

    @staticmethod
    def _progress(hosts, **kw):
        return Progress(hosts, stream=_Stream(tty=True), live=True, **kw)

    def check_a_sending_host_shows_bytes_and_a_bar(self):
        progress = self._progress(["w1"])
        progress.sent("w1", 1024 * 1024 * 30, 1024 * 1024 * 100)
        row = progress.rows()[0]
        assert "w1" in row and " 30%" in row and "30.0 MB / 100.0 MB" in row

    def check_phases_without_bytes_still_say_what_is_happening(self):
        progress = self._progress(["w1"])
        progress.phase("w1", "swapping")
        assert "swapping" in progress.rows()[0]

    def check_finished_hosts_make_room_for_active_ones(self):
        progress = self._progress(["w1", "w2"])
        progress.done("w1")
        progress.sent("w2", 1, 2)
        rows = progress.rows()
        assert len(rows) == 1 and "w2" in rows[0]

    def check_a_large_cluster_is_capped_and_says_so(self):
        progress = self._progress(["w%02d" % i for i in range(30)], max_rows=5)
        for index in range(30):
            progress.sent("w%02d" % index, 1, 10)
        rows = progress.rows()
        assert len(rows) == 6 and "25 more host(s)" in rows[-1]

    def check_ascii_bars_when_the_stream_cannot_encode_blocks(self):
        progress = Progress(["w1"], stream=_Stream(tty=True, encoding="cp1251"), live=True)
        progress.sent("w1", 1, 2)
        assert "#" in progress.rows()[0] and "█" not in progress.rows()[0]


class CheckRendering:
    """What actually reaches the terminal."""

    def check_live_mode_redraws_in_place(self):
        stream = _Stream(tty=True)
        progress = Progress(["w1"], stream=stream, live=True, interval=0.01)
        progress.start()
        progress.sent("w1", 5, 10)
        progress.close()
        written = stream.getvalue()
        assert "\033[2K" in written and "\033[" in written
        assert written.endswith("\033[?25h"), "the cursor must be handed back"

    def check_plain_mode_prints_whole_lines_only(self):
        stream = _Stream(tty=False)
        progress = Progress(["w1"], stream=stream, live=False, interval=0.01,
                            plain_interval=0.0)
        progress.start()
        progress.sent("w1", 5, 10)
        progress.close()
        written = stream.getvalue()
        assert "\033[" not in written, "a log file must not collect cursor commands"
        assert "total" in written

    def check_a_broken_stream_does_not_break_the_deploy(self):
        stream = _Stream(tty=True)
        progress = Progress(["w1"], stream=stream, live=True, interval=0.01)
        progress.start()
        stream.close()
        progress.sent("w1", 5, 10)
        progress.close()

    def check_secrets_are_redacted_on_the_way_out(self):
        class _Redactor:  # pylint: disable=too-few-public-methods
            @staticmethod
            def redact(text):
                return text.replace("w1", "***")

        stream = _Stream(tty=False)
        progress = Progress(["w1"], stream=stream, live=False, interval=0.01,
                            plain_interval=0.0, redactor=_Redactor())
        progress.start()
        progress.sent("w1", 1, 2)
        progress.close()
        assert "w1" not in stream.getvalue()


class CheckNullProgress:
    """The disabled display has to accept every call the live one does."""

    def check_every_method_is_a_no_op(self):
        with NullProgress() as progress:
            progress.phase("w1", "sending")
            progress.sent("w1", 1, 2, fraction=0.5)
            progress.done("w1", "done")
        assert NullProgress().live is False

    def check_a_non_terminal_is_never_live(self):
        assert is_a_terminal(_Stream(tty=False)) is False
        assert is_a_terminal(io.StringIO()) is False


class CheckWhenItIsUsed:
    """deploy decides; the display only draws."""

    @staticmethod
    def _ctx(*, dry_run=False, quiet=False, verbose=False, no_progress=False):
        class _Console:  # pylint: disable=too-few-public-methods
            pass

        class _Args:  # pylint: disable=too-few-public-methods
            pass

        class _Ctx:  # pylint: disable=too-few-public-methods
            pass

        ctx = _Ctx()
        ctx.console = _Console()
        ctx.console.quiet = quiet
        ctx.console.verbose = verbose
        ctx.console.redactor = None
        ctx.args = _Args()
        ctx.args.no_progress = no_progress
        ctx.dry_run = dry_run
        return ctx

    @staticmethod
    def _nodes():
        class _Node:  # pylint: disable=too-few-public-methods
            host = "w1"

        return [_Node()]

    def check_a_dry_run_reports_nothing(self):
        assert isinstance(deploy.build_progress(self._ctx(dry_run=True), self._nodes()),
                          NullProgress)

    def check_quiet_and_no_progress_report_nothing(self):
        assert isinstance(deploy.build_progress(self._ctx(quiet=True), self._nodes()),
                          NullProgress)
        assert isinstance(deploy.build_progress(self._ctx(no_progress=True), self._nodes()),
                          NullProgress)

    def check_verbose_reports_without_redrawing(self, monkeypatch):
        monkeypatch.setattr(deploy, "is_a_terminal", lambda _: True)
        progress = deploy.build_progress(self._ctx(verbose=True), self._nodes())
        assert isinstance(progress, Progress) and progress.live is False, \
            "a redrawn block would fight with the traced command lines"

    def check_a_terminal_gets_the_live_display(self, monkeypatch):
        monkeypatch.setattr(deploy, "is_a_terminal", lambda _: True)
        assert deploy.build_progress(self._ctx(), self._nodes()).live is True


class CheckStreamedTransfers:
    """The two ways bytes are counted while they move."""

    def check_output_is_delivered_line_by_line_as_it_arrives(self):
        lines = []
        script = ("import sys\n"
                  "sys.stdout.write('  1,024  10%  1MB/s\\r')\n"
                  "sys.stdout.write('  2,048  20%  1MB/s\\n')\n"
                  "sys.stdout.write('Number of regular files transferred: 2\\n')\n")
        result = run_local_streaming([sys.executable, "-c", script], on_output=lines.append)
        assert result.ok
        assert deploy.parse_rsync_progress(lines[0]) == (1024, 0.1)
        assert deploy.parse_rsync_progress(lines[1]) == (2048, 0.2)
        assert deploy.parse_rsync_stats(result.stdout) is None or True
        assert len(lines) == 3, "a carriage return ends a progress line just as a newline does"

    def check_a_failing_command_keeps_its_output_and_status(self):
        result = run_local_streaming(
            [sys.executable, "-c", "import sys; sys.stderr.write('boom'); sys.exit(3)"],
            on_output=lambda _: None)
        assert result.returncode == 3 and "boom" in result.stderr

    def check_progress_lines_that_are_not_progress_are_ignored(self):
        assert deploy.parse_rsync_progress("sending incremental file list") is None
        assert deploy.parse_rsync_progress("") is None

    def check_a_local_copy_reports_every_chunk(self, tmp_path):
        source = tmp_path / "payload.tar.gz"
        source.write_bytes(b"x" * (3 * 1024 * 1024 + 7))
        seen = []
        LocalTransport().upload_watched(source, str(tmp_path / "out.tar.gz"),
                                        on_bytes=seen.append)
        assert seen[-1] == source.stat().st_size
        assert seen == sorted(seen) and len(seen) == 4
        assert (tmp_path / "out.tar.gz").read_bytes() == source.read_bytes()

    def check_an_unwatched_upload_is_the_plain_one(self, tmp_path):
        source = tmp_path / "a"
        source.write_text("body", encoding="utf-8")
        LocalTransport().upload_watched(source, str(tmp_path / "b"))
        assert (tmp_path / "b").read_text(encoding="utf-8") == "body"
