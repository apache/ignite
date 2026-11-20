# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"). You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest

from count_votes import DAY, verify_tally


NOTICE = ("This RFC is limited to compatible additions. "
          "It will be approved if no objections are raised during the first seven days of the vote.")
PASSED = "The vote has passed."


def mail(address, body, epoch):
    return {"from_raw": address, "epoch": epoch, "body": body}


def directory(url):
    """Serve project and person records with the same fields as Whimsy."""
    if url.endswith("public_ldap_projects.json"):
        return {"projects": {"ignite": {"members": ["ada", "ben", "cid"], "owners": ["ada"]}}}
    if url.endswith("public_ldap_people.json"):
        return {"people": {uid: {"name": name} for uid, name in
                           [("ada", "Ada One"), ("ben", "Ben Two"), ("cid", "Cid Three"), ("else", "Else Four")]}}
    raise AssertionError(url)


def tally(votes, opening="[ ] +1 Approve\n[ ] -1 Disapprove", result=PASSED, duration=3 * DAY):
    """Check archive messages with a fixed publication time and the test directory."""
    return verify_tally(mail("ada@apache.org", opening, 0),
                        [mail(address, body, 100) for address, body in votes],
                        mail("ada@apache.org", result, duration), directory)


class CountVotesTest(unittest.TestCase):
    def test_author_and_non_pmc_committers_count(self):
        """Given an author's vote and two member replies, closing the vote counts all three."""
        result = ("Hi all,\n\nThe vote has passed with 3 +1 votes from Ignite committers:\n\n"
                  "- Ben Two\n- Cid Three\n\nThere were no 0 or -1 votes.\n\nThanks everyone for reviewing and voting!")
        votes = [("Ben Two <ben@apache.org>", "My +1 as well\n\nOn Tue, Aug 4, Ada <ada@apache.org>\nwrote:\n> -1"),
                 ("Cid Three <cid@apache.org>", "+1 (binding)\n\n> previous vote\n\n-- \nBest wishes,\nCid Three")]
        self.assertIn("ada, ben, cid", tally(votes, opening="My vote is +1.", result=result))

    def test_silent_approval_requires_a_scope_check(self):
        """A seven-day vote still needs a reviewer to verify that the proposal qualifies."""
        for opening in (NOTICE, NOTICE + " We request permission to skip deprecation for the old option."):
            with self.subTest(opening=opening), self.assertRaisesRegex(ValueError, "^Manual review:.*only adds compatible"):
                tally([], opening=opening, duration=7 * DAY)

    def test_three_committer_votes_do_not_need_the_seven_day_rule(self):
        """Three explicit committer votes establish approval even when the opening also offers seven days."""
        votes = [(uid + "@apache.org", "+1") for uid in ("ada", "ben", "cid")]
        self.assertIn("ada, ben, cid", tally(votes, opening=NOTICE, duration=7 * DAY))

    def test_early_seven_day_close(self):
        """Given a compatible addition, closing one second early blocks the shortcut even with discussion."""
        with self.assertRaisesRegex(ValueError, "^The vote closed before seven days"):
            tally([("ben@apache.org", "Question about the schedule")], opening=NOTICE, duration=7 * DAY - 1)

    def test_minimum_duration(self):
        """Given three votes, closing one second before 72 hours fails."""
        with self.assertRaisesRegex(ValueError, "^The RESULT was published before"):
            tally([(f"{uid}@apache.org", "+1") for uid in ("ada", "ben", "cid")], duration=3 * DAY - 1)

    def test_failed_result_precedes_ambiguous_reply(self):
        """Given discussion, a negative RESULT still blocks the check."""
        with self.assertRaisesRegex(ValueError, "^The published RESULT says"):
            tally([("ada@apache.org", "Please clarify")], result="Hi all,\n\nThe vote did not pass.")

    def test_negative_reply_precedes_ambiguous_reply(self):
        """Given unclear prose followed by a -1, a positive RESULT cannot clear the objection."""
        with self.assertRaisesRegex(ValueError, "^The archive contains an explicit -1"):
            tally([("ada@apache.org", "Please clarify"), ("ben@apache.org", "-1 because the old API breaks.")],
                  opening=NOTICE, duration=7 * DAY)

    def test_withdrawal_needs_human_review(self):
        """Given a -1 followed by its author's withdrawal, closing requires a reader to confirm resolution."""
        replies = [mail("ben@apache.org", "-1", 100), mail("ben@apache.org", "I no longer object.", 200)]
        replies.extend(mail(uid + "@apache.org", "+1", 300) for uid in ("ada", "ben", "cid"))
        with self.assertRaisesRegex(ValueError, "^Manual review:"):
            verify_tally(mail("ada@apache.org", NOTICE, 0), replies, mail("ada@apache.org", PASSED, 7 * DAY), directory)

    def test_ballot_must_be_the_authors_own_and_unconditional(self):
        """Given two clear votes, a third forwarded, conditional or amended vote needs review."""
        for text in ("----- Forwarded message -----\n+1", "+1 if fixed", "+1\n> prior\nI withdraw it"):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "^Manual review:"):
                tally([("ada@apache.org", "+1"), ("ben@apache.org", "+1"), ("cid@apache.org", text)])

    def test_author_vote_is_not_implicit(self):
        """Given just an unchecked ballot and two positive replies, closing finds too few votes."""
        with self.assertRaisesRegex(ValueError, "^The archive has fewer than three"):
            tally([("ben@apache.org", "+1"), ("cid@apache.org", "+1")])

    def test_discussion_and_quotes_cannot_supply_a_missing_vote(self):
        """Two votes and a question or a quotation still fall short of three explicit votes."""
        for text in ("When would we release this?", "> +1"):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "^The archive has fewer than three"):
                tally([("ada@apache.org", "+1"), ("ben@apache.org", "+1"), ("cid@apache.org", text)])

    def test_bare_ballot_in_the_opening_can_be_verified(self):
        """A reviewer can verify whether the author's standalone ballot is their own vote."""
        with self.assertRaisesRegex(ValueError, "^Manual review:.*opening email"):
            tally([("ben@apache.org", "+1"), ("cid@apache.org", "+1")], opening="Please review the proposal.\n\n+1 (binding)")

    def test_repeated_vote(self):
        """When a committer changes their vote and quorum is possible, a reviewer checks their final vote."""
        with self.assertRaisesRegex(ValueError, "^Manual review: one sender"):
            tally([("Ben <ben@apache.org>", "+1"), ("B <ben@apache.org>", "0"), ("cid@apache.org", "+1"),
                   ("ada@apache.org", "+1")])

    def test_result_must_agree_with_ballots(self):
        """Given three archived votes, a conflicting total or qualification in the RESULT needs review."""
        for text in ("The vote has passed with 4 +1 votes.", "The vote has passed. Only part A is approved.",
                     "If the vote has passed, we may merge.", "> The vote passed."):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "^Manual review:"):
                tally([(f"{uid}@apache.org", "+1") for uid in ("ada", "ben", "cid")], result=text)

    def test_self_declared_binding_does_not_establish_membership(self):
        """Given a third vote from another project's committer, the published ballot falls short."""
        with self.assertRaisesRegex(ValueError, "^Fewer than three distinct"):
            tally([("ada@apache.org", "+1"), ("ben@apache.org", "+1"), ("else@apache.org", "+1 (binding)")])

    def test_directory_name_needs_confirmation(self):
        """Given a third vote with a directory name alone, closing identifies the candidate for review."""
        for address in ("Cid Three via dev <dev@ignite.apache.org>", "Cid Three <cid@example.org>"):
            with self.subTest(address=address), self.assertRaisesRegex(ValueError, "^Manual review:.*accounts to check: cid"):
                tally([("ada@apache.org", "+1"), ("ben@apache.org", "+1"), (address, "+1")])

    def test_natural_language_notice_can_be_reviewed(self):
        """Given a week of lazy consensus in other words, closing sends that announcement to a reviewer."""
        with self.assertRaisesRegex(ValueError, "^Manual review:.*seven days without objections"):
            tally([], opening="These are additive changes. I propose lazy consensus for a week.", duration=7 * DAY)

    def test_incompatible_change_cannot_use_silence(self):
        """Given an explicit breaking change, a week without replies cannot approve the RFC."""
        with self.assertRaisesRegex(ValueError, "^The opening excludes approval by silence"):
            tally([], opening="This change includes a breaking API change. I propose lazy consensus for a week.",
                  duration=7 * DAY)

    def test_generic_policy_reference_does_not_announce_this_vote(self):
        """Given only a general rule, seven days passing sends the specific vote to review."""
        with self.assertRaisesRegex(ValueError, "^Manual review:"):
            tally([], opening="For compatible additions, seven days without objections can suffice.", duration=7 * DAY)


if __name__ == "__main__":
    unittest.main()
