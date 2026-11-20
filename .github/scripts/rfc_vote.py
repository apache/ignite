# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read public Apache Pony Mail messages and verify evidence for one pull request."""

import http.client
import json
import re
from dataclasses import dataclass
from urllib.parse import unquote, urlencode, urlsplit
from urllib.request import Request, urlopen

from rfc_tally import unquoted_body, verify_tally


ARCHIVE = "https://lists.apache.org"
MAIL_ID = r"[a-zA-Z0-9_-]{1,128}"
PERMALINK = re.compile(
    rf"{re.escape(ARCHIVE)}/thread(?:\.html)?/({MAIL_ID})/?(?:\?dev(?:@|%40)ignite\.apache\.org)?"
)


class VoteError(ValueError):
    """The archive did not establish the requested vote evidence."""


class ManualReviewRequired(VoteError):
    """A maintainer must interpret the evidence for this PR and RFC."""


@dataclass
class VoteEvidence:
    opening_url: str
    result_url: str
    elapsed_hours: float
    versions: dict
    tally: str


def _load_json(url):
    # The only callers construct URLs from fixed ASF hosts and validated message IDs.
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "ignite-rfc-check"})
    try:
        with urlopen(request, timeout=15) as response:
            limit = 16 * 1024 * 1024
            data = response.read(limit + 1)
            if len(data) > limit:
                raise VoteError(f"Response from {urlsplit(url).hostname} exceeds 16 MiB.")
            return json.loads(data)
    except (OSError, http.client.HTTPException, ValueError, RecursionError) as exc:
        raise VoteError(f"Could not read evidence from {urlsplit(url).hostname}: {exc}") from exc


def _links(body):
    try:
        text = unquoted_body(body)
    except ValueError as exc:
        raise VoteError("Could not distinguish original links from quoted email.") from exc
    return {url.rstrip(".,;:!?") for url in re.findall(r"https://[^\s<>\[\]()\"']+", text)}


def _archive(endpoint, mid, **params):
    return _load_json(f"{ARCHIVE}/api/{endpoint}.lua?" + urlencode({"id": mid, **params}))


def _message(mid):
    mail = _archive("email", mid)
    if not isinstance(mail, dict) or mail.get("id") != mid:
        raise VoteError("Apache Lists did not return the requested message.")
    if mail.get("list") != "<dev.ignite.apache.org>" or mail.get("private") is not False:
        raise VoteError("The message must be public and archived on dev@ignite.apache.org.")
    if (not isinstance(mail.get("subject"), str) or not isinstance(mail.get("body"), str)
            or type(mail.get("epoch")) is not int):
        raise VoteError("The archived message is missing its subject, body or timestamp.")
    return mail


def _thread(mid):
    data = _archive("thread", mid, find_parent="true")
    root = data.get("thread") if isinstance(data, dict) else None
    pending = [root]
    ids = []
    while pending:
        node = pending.pop()
        if not isinstance(node, dict) or not isinstance(node.get("children"), list):
            raise VoteError("Apache Lists returned an incomplete vote thread.")
        node_id = node.get("id")
        if not isinstance(node_id, str) or not re.fullmatch(MAIL_ID, node_id) or node_id in ids:
            raise VoteError("Apache Lists returned an invalid message ID in the vote thread.")
        ids.append(node_id)
        # Pony Mail bounds both the reply search and recursion at 250 per thread branch.
        if len(ids) >= 250:
            raise VoteError("This thread reaches Apache Lists' retrieval limit of 250 messages.")
        pending.extend(reversed(node["children"]))
    if mid not in ids:
        raise VoteError("The archived thread does not contain the linked message.")
    return ids


def _is_opening(mail):
    subject = mail["subject"].strip().upper()
    return subject.startswith("[VOTE]") and "[RESULT]" not in subject


def _topic(mail):
    title = re.sub(r"^\s*(?:(?:\[(?:RESULT|VOTE)\]|Re:)\s*)+", "", mail["subject"], flags=re.I)
    return " ".join(title.casefold().split())


def _versions(links, paths):
    versions = {}
    for path in paths:
        references = {link for link in links if unquote(urlsplit(link).path).endswith("/" + path)}
        if not references:
            raise VoteError(f"The opening [VOTE] must link the proposed version of {path}.")
        shas = set()
        for link in references:
            url = urlsplit(link)
            match = re.fullmatch(r"/[^/]+/[^/]+/blob/([a-fA-F0-9]{40})/" + re.escape(path), unquote(url.path))
            if url.netloc.lower() != "github.com" or not match:
                break
            shas.add(match[1].lower())
        else:
            if len(shas) == 1:
                versions[path] = shas.pop()
    return versions


def verify_vote(result_url, pr_url, rfc_paths, *, minimum_hours=72):
    """Return archive evidence or raise VoteError. Versions maps file paths to voted commit SHAs.

    The caller compares each voted blob with the RFC at the current head. Other link formats
    or changed RFC text require a maintainer to confirm what the vote covered.
    """
    match = PERMALINK.fullmatch(result_url)
    if not match:
        raise VoteError("Link the exact [RESULT] message on https://lists.apache.org/thread/<id>.")
    result_id = match[1]
    result = _message(result_id)
    if not re.match(r"^\s*(?:\[VOTE\]\s*)?\[RESULT\]", result["subject"], re.I):
        raise VoteError("The linked message must announce [RESULT] in its own subject.")

    result_thread = _thread(result_id)
    separate_result = result_thread[0] == result_id
    if not separate_result:
        opening = _message(result_thread[0])
        if not _is_opening(opening):
            raise VoteError("The linked [RESULT] is a reply outside an opening [VOTE] thread.")
        vote_thread = result_thread
    else:
        links = {m[1] for url in _links(result["body"]) if (m := PERMALINK.fullmatch(url))}
        if len(links) != 1:
            raise VoteError("The separate [RESULT] must identify one opening [VOTE] permalink.")
        opening_id = links.pop()
        opening = _message(opening_id)
        vote_thread = _thread(opening_id)
        if vote_thread[0] != opening_id or not _is_opening(opening):
            raise VoteError("The [RESULT] must link the original opening [VOTE] message.")

    links = _links(opening["body"])
    if pr_url.rstrip("/") not in {link.rstrip("/") for link in links}:
        raise VoteError("The opening [VOTE] must link this pull request.")
    versions = _versions(links, rfc_paths)
    elapsed = result["epoch"] - opening["epoch"]
    if elapsed < minimum_hours * 3600:
        raise VoteError(f"Fewer than {minimum_hours} hours elapsed between [VOTE] and [RESULT].")

    reply_ids = dict.fromkeys(vote_thread + result_thread)
    replies = [_message(mid) for mid in reply_ids if mid not in (opening["id"], result_id)]
    try:
        tally = verify_tally(opening, replies, result, _load_json)
    except VoteError:
        raise
    except ValueError as exc:
        if str(exc).startswith("Manual review:"):
            raise ManualReviewRequired(str(exc)) from exc
        raise VoteError(str(exc)) from exc
    except (KeyError, TypeError, AttributeError) as exc:
        raise VoteError("Apache returned incomplete committer roster data.") from exc
    if separate_result and _topic(result) != _topic(opening):
        raise ManualReviewRequired(
            "Confirm that the separate [RESULT] and [VOTE] with different subjects refer to the same RFC."
        )

    return VoteEvidence(
        f"{ARCHIVE}/thread/{opening['id']}", f"{ARCHIVE}/thread/{result_id}",
        elapsed / 3600, versions, tally,
    )
