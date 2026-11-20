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

import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import rfc_vote


PR = "https://github.com/apache/ignite/pull/42"
RFC = "rfcs/IGNITE-42-example.md"
SHA = "c" * 40
BLOB = f"https://github.com/contributor/ignite/blob/{SHA}/{RFC}"
VOTE = rfc_vote.ARCHIVE + "/thread/open"
RESULT = rfc_vote.ARCHIVE + "/thread/result"


def node(mid, *children):
    """Build a Pony Mail thread node containing reply metadata."""
    return {"id": mid, "children": list(children)}


def mail(mid, subject, body, epoch, sender="organizer"):
    """Build an unquoted public dev-list email for the indicated archive ID."""
    return {
        "id": mid, "subject": subject, "body": body, "epoch": epoch,
        "private": False, "list": "<dev.ignite.apache.org>",
        "from_raw": f"{sender} <{sender}@apache.org>",
    }


class VoteTest(unittest.TestCase):
    def setUp(self):
        """Provide a PR-specific vote with three distinct committers and a separate result email."""
        self.mails = {
            "open": mail("open", "[VOTE] Example", f"Review {PR}\nVersion {BLOB}", 10),
            "result": mail("result", "[RESULT] [VOTE] Example", "The vote has passed.\n\n" + VOTE, 10 + 72 * 3600),
        }
        self.people = ["alice", "bob", "carol"]
        for i, uid in enumerate(self.people):
            self.mails[uid] = mail(uid, "Re: [VOTE] Example", "+1", 20 + i, uid)
        self.tree = {"open": node("open", node("alice", node("bob")), node("carol")), "result": node("result")}
        self.enterContext(patch("rfc_vote._load_json", side_effect=self.archive))

    def archive(self, url):
        """Serve archive and ASF roster records through the URLs used in production."""
        if url.endswith("public_ldap_projects.json"):
            return {"projects": {"ignite": {"members": self.people}}}
        if url.endswith("public_ldap_people.json"):
            return {"people": {uid: {"name": uid} for uid in self.people}}
        uri = urlsplit(url)
        self.assertEqual(uri.netloc, "lists.apache.org")
        query = parse_qs(uri.query)
        mid = query["id"][0]
        if uri.path == "/api/email.lua":
            return self.mails.get(mid)
        self.assertEqual(uri.path, "/api/thread.lua")
        self.assertEqual(query["find_parent"], ["true"])
        return {"thread": self.tree[mid]}

    def check(self, url=RESULT, *, reviewer_opening=None):
        return rfc_vote.verify_vote(url, PR, [RFC], reviewer_opening=reviewer_opening)

    def test_separate_result_and_version(self):
        """Given a separate RESULT, following its opening link returns the voted version."""
        self.mails["result"]["body"] = "The vote has passed.\n\n" + VOTE.replace("/thread/", "/thread.html/")
        self.assertEqual(self.check().versions, {RFC: SHA})

    def test_reviewer_can_identify_an_unlinked_opening(self):
        """Given a standalone RESULT with no vote link, a reviewed opening supplies the file and tally."""
        self.mails["result"]["body"] = "The vote has passed."
        with self.assertRaises(rfc_vote.OpeningReviewRequired):
            self.check()
        evidence = self.check(reviewer_opening=lambda: VOTE)
        self.assertEqual(evidence.versions, {RFC: SHA})
        self.assertEqual(evidence.review_note, "")
        self.assertIn("alice, bob, carol", evidence.tally)

    def test_reviewer_cannot_override_a_linked_opening(self):
        """Given links to another PR's vote and a discussion, the result belongs to the linked vote."""
        self.mails["open"]["body"] = f"https://github.com/apache/ignite/pull/99\n{BLOB}"
        self.mails["result"]["body"] += "\n" + VOTE.replace("/open", "/discussion")
        self.mails["discussion"] = mail("discussion", "[DISCUSS] Example", "A discussion.", 0)
        with self.assertRaisesRegex(rfc_vote.VoteError, "this pull request"):
            self.check(reviewer_opening=lambda: self.fail("The result already identifies a vote."))

    def test_reviewer_must_choose_among_linked_openings(self):
        """Given a result linking two votes, a reviewer can select one of those votes."""
        self.mails["result"]["body"] += "\n" + VOTE.replace("/open", "/older")
        self.mails["older"] = mail("older", "[VOTE] Earlier proposal", "Earlier proposal.", 0)
        with self.assertRaises(rfc_vote.OpeningReviewRequired):
            self.check(reviewer_opening=lambda: VOTE.replace("/open", "/other"))
        self.assertEqual(self.check(reviewer_opening=lambda: VOTE).opening_url, VOTE)

    def test_unreadable_result_reference_requires_archive_evidence(self):
        """Given a missing archived reference, a review cannot substitute another opening."""
        self.mails["result"]["body"] += "\n" + VOTE.replace("/open", "/missing")
        with self.assertRaisesRegex(rfc_vote.VoteError, "requested message"):
            self.check(reviewer_opening=lambda: self.fail("The archive response was incomplete."))

    def test_reviewed_opening_cannot_clear_a_failed_vote(self):
        """Given an unlinked opening, a reviewer-supplied link still rejects an early or failed result."""
        for body, epoch, reason in [
            ("The vote has passed.", 72 * 3600, "Fewer than 72"),
            ("The vote failed.", 10 + 72 * 3600, "says the vote failed"),
        ]:
            with self.subTest(reason=reason), patch.dict(self.mails["result"], body=body, epoch=epoch):
                with self.assertRaisesRegex(rfc_vote.VoteError, reason):
                    self.check(reviewer_opening=lambda: VOTE)

    def test_result_inside_vote_thread(self):
        """Given a RESULT replying to a ballot, its exact permalink identifies the opening."""
        self.tree["open"]["children"][0]["children"].append(node("result"))
        self.tree["result"] = self.tree["open"]
        self.mails["result"]["body"] = "The vote has passed."
        self.assertEqual(self.check(RESULT + "?dev@ignite.apache.org").opening_url, VOTE)

    def test_result_subject_on_exact_message(self):
        """Given a later reply or a quoted subject, checking its permalink rejects it."""
        for subject in ("Re: [RESULT] [VOTE] Example", "Fwd: [RESULT]", "[VOTE] Example"):
            with self.subTest(subject=subject):
                self.mails["result"]["subject"] = subject
                with self.assertRaisesRegex(rfc_vote.VoteError, "own subject"):
                    self.check()

    def test_unrelated_pr(self):
        """Given a foreign PR and a quoted reference to this one, evidence fails."""
        self.mails["open"]["body"] = f"{PR}0\n> {PR}\n{BLOB}"
        with self.assertRaisesRegex(rfc_vote.VoteError, "this pull request"):
            self.check()

    def test_wrong_rfc_path(self):
        """Given an opening linking another RFC in this PR, the requested RFC fails."""
        self.mails["open"]["body"] = f"{PR}\n{BLOB}.old"
        with self.assertRaisesRegex(rfc_vote.VoteError, RFC):
            self.check()

    def test_unpinned_version(self):
        """Given a branch link, archive verification leaves the blob comparison to a maintainer."""
        self.mails["open"]["body"] = f"{PR}\n{BLOB.replace(SHA, 'branch')}"
        self.assertEqual(self.check().versions, {})

    def test_early_close(self):
        """Given an early result, checking the two archive timestamps rejects it."""
        self.mails["result"]["epoch"] -= 1
        with self.assertRaisesRegex(rfc_vote.VoteError, "Fewer than 72") as caught:
            self.check()
        self.assertNotIsInstance(caught.exception, rfc_vote.ManualReviewRequired)

    def test_foreign_or_private_message(self):
        """Given an unrelated list or a private reply, checking all nested ballots fails."""
        for fields in ({"list": "<dev.other.apache.org>"}, {"private": True}):
            with self.subTest(fields=fields), patch.dict(self.mails["bob"], fields):
                with self.assertRaisesRegex(rfc_vote.VoteError, "public and archived"):
                    self.check()

    def test_archive_missing_a_reply(self):
        """Given a reply in the index, failing to load its full text blocks verification."""
        del self.mails["bob"]
        with self.assertRaisesRegex(rfc_vote.VoteError, "requested message"):
            self.check()

    def test_foreign_link(self):
        """Given a lookalike or malformed archive link, the check rejects it before fetching."""
        for url in (RESULT + "#other", RESULT + "/more", RESULT.replace(".org/", ".org.example/"),
                    RESULT.replace("https:", "http:")):
            with self.subTest(url=url), self.assertRaisesRegex(rfc_vote.VoteError, "exact"):
                self.check(url)

    def test_result_and_vote_name_different_proposals(self):
        """Given a standalone RESULT naming another proposal, its opening link needs confirmation."""
        self.mails["result"]["subject"] = "[RESULT] [VOTE] A different RFC"
        self.assertIn("different subjects", self.check().review_note)

    def test_ambiguous_ballot_requires_review(self):
        """Given free-form vote text, checking complete evidence requests maintainer interpretation."""
        self.mails["bob"]["body"] = "+1 if the migration plan is confirmed."
        evidence = self.check()
        self.assertIn("Manual review", evidence.review_note)
        self.assertEqual(evidence.versions, {RFC: SHA})


if __name__ == "__main__":
    unittest.main()
