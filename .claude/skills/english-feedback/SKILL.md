---
name: english-feedback
description: The user is practicing English. On EVERY user message, scan their own wording for English grammar, spelling, word-choice, punctuation, or phrasing mistakes and, if any exist, append a short correction note at the end of the reply. If there are no mistakes, print nothing — never add an empty "nothing to correct" note. Applies to all turns automatically, not only when explicitly asked. Do the user's actual task first; the note is an addition, never a replacement.
---

# English feedback

The user wants to improve their English. On every message they send, review the
**user's own prose** (not code, logs, file contents, or quoted text) for mistakes.

## How to give feedback
- Answer the user's real request first, as normal. Then add the note.
- Put the note at the very end under a clear heading, e.g. `---` then `**✍️ English**`.
- Only raise it when there is a genuine error or clearly unnatural phrasing. **If there is nothing to correct, print nothing at all** — no heading, no "nothing to correct", no "looks good", no acknowledgement of any kind. Silence is the correct output. Do not invent nitpicks, and do not comment every turn just to comment.
- For each issue: quote the original, give the correction, and add a brief reason. Keep it short.
- Cover: grammar, spelling (incl. homophones like write/right, their/there), articles (a/an/the), verb tense/agreement, prepositions, word order, capitalization of proper nouns (e.g. "English"), and awkward phrasing → a more natural alternative.
- Be encouraging and concise. Group multiple small issues into one compact list.
- Never block or delay the technical answer for this. If the turn is urgent or the user says to stop, skip the note.

## Format example
```
---
**✍️ English**
- "right a note" → "write a note" (homophone: *right* vs *write*).
- "english" → "English" (language names are capitalized).
- "Save this to skill" → "Save this to a skill" (missing article *a*).
```

## Turning it off
The user can say "stop the English notes" (or similar) to pause this. Honor that until they re-enable it.
