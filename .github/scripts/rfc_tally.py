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

import re
from collections import defaultdict
from email.utils import parseaddr

WHIMSY = "https://whimsy.apache.org/public/"
DAY = 86400
FORWARDED = re.compile(r"(?im)^[ \t]*(?:-+\s*(?:forwarded|original) message|begin forwarded message:|from:)")


def manual(reason):
    raise ValueError(f"Manual review: {reason}")


def unquoted_body(body):
    if not isinstance(body, str) or not body.strip():
        manual("the archive has no readable message body")
    text = body.replace("\r\n", "\n")
    # Drop attribution only when followed by an actual quoted block. Keep inline replies.
    text = re.sub(
        r"(?im)^On [^<>\n]+(?:\n[^<>\n]*)?<[^<>\n]+@[^<>\n]+>[ \t]*(?:\n[ \t]*)?"
        r"wrote:[ \t]*\n(?:[ \t]*\n)*(?=[ \t]*>)", "", text,
    )
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith(">"))
    if FORWARDED.search(text):
        manual("a forwarded message needs a reader to identify who said what")
    return text.strip()


def sender(message):
    name, address = parseaddr(message.get("from_raw") or message.get("from", ""))
    return name.removesuffix(" via dev").strip().casefold(), address.casefold()


def sender_key(message):
    name, address = sender(message)
    if address in {"dev@ignite.apache.org", "de...@ignite.apache.org"} or "..." in address:
        return name, address
    return "", address


def signed_text(message):
    text = unquoted_body(message["body"])
    # Public mail sometimes places the author's ordinary name signature below a quoted reply.
    parts = re.split(r"(?m)^-- ?$", text, maxsplit=1)
    if len(parts) == 2:
        name, _ = sender(message)
        signature = [line.strip().casefold() for line in parts[1].splitlines() if line.strip()]
        if signature and signature[-1] == name and all(
            line in {"best wishes,", "best regards,", "regards,", "thanks,", "cheers,"}
            for line in signature[:-1]
        ):
            return parts[0].strip()
    return text


BALLOT = re.compile(
    r"(?:(?:my vote is|i vote|my)\s+)?(?P<vote>\+1|0)"
    r"(?:\s+as well|\s+from me)?(?:\s*\((?P<kind>binding|non[- ]binding)\))?[.!]?",
    re.I,
)
LIMITED_SCOPE = r"(?:this|the) (?:rfc|proposal|change) (?:is limited to|contains only|only includes|covers only) compatible additions"
SILENCE = (
    r"(?:(?:this|the) (?:rfc|proposal)|it) (?:will (?:also )?be |is (?:also )?(?:considered )?)approved "
    r"if (?:no objections? (?:are raised|are received)|there (?:are|is) no objections?) "
    r"(?:during|within|in) (?:the (?:first|next) )?(?:seven|7) days(?: of (?:the|this) vote)?"
)
OUTCOME = re.compile(
    r"(?:(?:the |this )?vote (?:has )?passed|(?:the |this )?(?:rfc|proposal) (?:is|was|has been) approved|approved|passed)"
    r"(?: with (?P<count>\d+) \+1(?:s| votes?)?(?P<committers> from (?:different |distinct )?ignite committers)?"
    r"(?P<no_votes> and no (?:0 or )?-1 votes?)?)?[.!:]?", re.I,
)


def result_evidence(result, positive_names):
    """Read common RESULT summaries. Unrecognized prose can qualify or reverse the decision."""
    text = signed_text(result)
    cleaned = []
    for line in text.splitlines():
        line = line.strip()
        if re.fullmatch(r"(?:\[\d+\]\s*)?https?://\S+", line, re.I):
            continue
        if re.fullmatch(r"(?:vot(?:e|ing) thread|vote):\s*(?:\[\d+\]|https?://\S+)", line, re.I):
            continue
        if re.fullmatch(r"[-*]\s+.+", line) and line[1:].strip().casefold() in positive_names:
            continue
        cleaned.append(line)
    outcomes = []
    no_neutral = False
    for paragraph in re.split(r"\n\s*\n", "\n".join(cleaned)):
        paragraph = " ".join(paragraph.split())
        for sentence in re.split(r"(?<=[.!])\s+", paragraph):
            if not sentence:
                continue
            match = OUTCOME.fullmatch(sentence)
            if match:
                outcomes.append(match)
                no_neutral |= "no 0" in (match["no_votes"] or "").lower()
            elif re.fullmatch(r"(?:there were |we received )?no (?:0 or )?-1 votes?[.!]?", sentence, re.I):
                no_neutral |= "no 0" in sentence.lower()
            elif not re.fullmatch(
                r"(?:hi|hello)(?: all| everyone| folks)?[,!]?|"
                r"thanks(?: everyone| all)?(?: for (?:reviewing and )?voting)?[.!]?|"
                r"no objections (?:were raised|were received)(?: (?:in|during|within) "
                r"(?:the (?:first|last) )?(?:seven|7) days(?: of (?:the|this) vote)?)?[.!]?", sentence, re.I,
            ):
                manual("read the published RESULT for a decision, qualifications or a revised tally")
    if len(outcomes) != 1:
        manual("the RESULT needs one clear affirmative decision")
    return outcomes[0], no_neutral


def committer_tally(ballots, load_json):
    try:
        roster = load_json(WHIMSY + "public_ldap_projects.json")["projects"]["ignite"]["members"]
        people = load_json(WHIMSY + "public_ldap_people.json")["people"]
        if not isinstance(roster, list) or not roster or not people:
            raise ValueError("The Apache Ignite committer directory is empty or incomplete.")
        members = set(roster)
        names = defaultdict(set)
        for uid, person in people.items():
            if person.get("name"):
                names[person["name"].casefold()].add(uid)
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("The Apache committer directory has an unexpected format.") from exc

    confirmed = set()
    candidates = set()
    uncertain = 0
    for message, _ in ballots:
        name, address = sender(message)
        local, _, domain = address.partition("@")
        if domain == "apache.org" and re.fullmatch(r"[a-z][a-z0-9_-]*", local):
            if name in names and local not in names[name]:
                manual("the Apache address and display name identify different accounts")
            if local in members and local in people and not people[local].get("noLogin"):
                confirmed.add(local)
        else:
            uncertain += 1
            # A display name can suggest a candidate for a reviewer. It does not prove account ownership.
            possible = names[name] | ({name} if name in people else set())
            if len(possible) == 1:
                candidates.update(possible & members)
    return confirmed, uncertain, candidates


def read_replies(replies, start, finish):
    ballots = []
    problems = []
    has_negative_vote = False
    possible_positives = 0
    for reply in replies:
        try:
            text = " ".join(signed_text(reply).split())
        except ValueError as exc:
            problems.append(str(exc).removeprefix("Manual review: "))
            continue
        if re.match(r"(?:my vote is |i vote |my )?(?:\[x\]\s*)?-1\b", text, re.I):
            has_negative_vote = True
        match = BALLOT.fullmatch(text)
        if not start <= reply["epoch"] <= finish:
            problems.append("a reply outside the voting period needs review")
        elif not match:
            possible_positives += "+1" in text
            problems.append("the vote thread contains an objection, discussion, or an unclear ballot")
        else:
            ballots.append((reply, match))
    return ballots, problems, possible_positives, has_negative_vote


def verify_tally(opening, replies, result, load_json):
    duration = result["epoch"] - opening["epoch"]
    if duration < 3 * DAY:
        raise ValueError("The RESULT was published before the vote had been open for 72 hours.")
    ballots, problems, possible_positives, has_negative_vote = read_replies(replies, opening["epoch"], result["epoch"])
    own_result = FORWARDED.split(result["body"], maxsplit=1)[0]
    result_text = " ".join(unquoted_body(own_result).split()) if own_result.strip() else ""
    if re.search(r"\b(?:vote|rfc|proposal|motion) (?:has |has been |is |was |did )?"
                 r"(?:failed|rejected|withdrawn|cancelled|canceled|not (?:been )?(?:accepted|approved|pass(?:ed)?))\b",
                 result_text, re.I):
        raise ValueError("The published RESULT says the vote failed, was withdrawn or was rejected.")

    # An unchecked ballot in the announcement describes choices, including the negative vote.
    announcement = re.sub(r"(?im)^[ \t]*\[ \].*$", "", unquoted_body(opening["body"]))
    prose = "\n".join(" ".join(block.split()).lower() for block in re.split(r"\n\s*\n", announcement))
    compatible = re.search(r"(?:^|[.!?]\s+)" + LIMITED_SCOPE + r"[.!](?:\s|$)", prose, re.M)
    silence = re.search(r"(?:^|[.!?]\s+)" + SILENCE + r"[.!](?:\s|$)", prose, re.M)
    seven_day_notice = re.search(r"\b(?:lazy[ -]consensus|week|(?:seven|7)[ -]+(?:calendar )?days?|168[ -]+hours?)\b", prose)
    incompatible = re.search(
        r"\b(?:this|the) (?:rfc|proposal|change) (?:is|includes|contains|introduces) "
        r"(?:an? |only )?(?:breaking|incompatible|backwards?[- ]incompatible|not limited to compatible additions)\b"
        r"|\b(?:lazy consensus|the seven[- ]day rule) (?:does not apply|will not be used)\b", prose,
    )
    if re.search(r"\b(?:veto|concerns?|object(?:ion)?s?|withdraw\w*|cancel\w*|restart\w*)\b|(?<!\w)-1\b", re.sub(SILENCE, "", prose)):
        problems.append("check the opening message for objections, a restart or changes to the terms")
    for line in announcement.splitlines():
        if re.match(r"\s*(?:my (?:vote|[+-]\d)|i vote)\b", line, re.I):
            if re.match(r"\s*(?:my vote is|my|i vote)\s+-1\b", line, re.I):
                has_negative_vote = True
            match = BALLOT.fullmatch(line.strip())
            if match:
                ballots.append((opening, match))
            else:
                possible_positives += "+1" in line
                problems.append("the author's vote in the opening message needs interpretation")

    positive = [(message, match) for message, match in ballots if match["vote"] == "+1"]
    eligible = [(message, match) for message, match in positive if not (match["kind"] or "").lower().startswith("non")]
    if has_negative_vote:
        if len(eligible) + possible_positives < 3:
            raise ValueError("The archive contains an explicit -1 and fewer than three eligible +1 ballots.")
        manual("confirm that technical objections were resolved and any valid code veto was withdrawn")
    if incompatible and len(eligible) + possible_positives < 3:
        raise ValueError("The opening excludes approval by silence and the archive has fewer than three clear +1 ballots.")
    if duration < 7 * DAY and len(eligible) + possible_positives < 3 and seven_day_notice:
        raise ValueError("The vote closed before seven days with fewer than three clear eligible +1 ballots.")
    if problems:
        manual(problems[0])
    senders = {sender_key(message) for message, _ in ballots}
    if len(senders) != len(ballots):
        manual("one sender posted more than one ballot")

    outcome, no_neutral = result_evidence(result, {sender(message)[0] for message, _ in positive})
    if no_neutral and len(positive) != len(ballots):
        manual("the RESULT reports no neutral votes but the archive contains one")
    if outcome["count"] and not outcome["committers"] and int(outcome["count"]) != len(positive):
        manual("the number of +1 votes in the RESULT differs from the archived ballots")

    if duration >= 7 * DAY and compatible and silence and not incompatible and not outcome["committers"]:
        return "The opening announced approval for compatible additions after seven days without objections. The archived replies contain no objection."
    if len(eligible) < 3:
        if duration >= 7 * DAY and seven_day_notice and not incompatible:
            manual("confirm that the opening announced seven days without objections for compatible additions")
        raise ValueError("The archive has fewer than three eligible +1 ballots.")

    confirmed, uncertain, candidates = committer_tally(eligible, load_json)
    if len(confirmed) + uncertain < 3:
        raise ValueError("Fewer than three distinct Ignite committers cast eligible +1 ballots.")
    if len(confirmed) < 3 or outcome["committers"] and int(outcome["count"]) != len(confirmed):
        hint = " Possible ASF accounts to check: " + ", ".join(sorted(candidates)) + "." if candidates else ""
        manual("confirm the voters against the Ignite committer roster." + hint)
    return "Archived +1 votes matched these current Ignite committer IDs: " + ", ".join(sorted(confirmed))
