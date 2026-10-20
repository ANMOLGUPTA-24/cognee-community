"""In-memory Codeforces API served through ``httpx.MockTransport``.

Responses follow the shapes the live API returns (recorded from
``user.info`` / ``user.status`` / ``user.rating`` on 2026-10-16), including
the ``{"status": "FAILED", "comment": ...}`` envelope for errors.
"""

import json
from urllib.parse import urlparse

import httpx

DAY = 86_400
T0 = 1_760_000_000


class FakeCodeforces:
    """A tiny Codeforces: users, their submissions and rating changes."""

    def __init__(self):
        self.users: dict[str, dict] = {}
        self.submissions: dict[str, list[dict]] = {}
        self.ratings: dict[str, list[dict]] = {}
        self.calls: list[tuple[str, dict]] = []
        # Responses served before the normal ones (to simulate rate limiting).
        self.queued: list[httpx.Response] = []
        self._next_id = 1000

    # -- fixtures ------------------------------------------------------------

    def add_user(self, handle: str, **fields) -> None:
        self.users[handle.lower()] = {
            "handle": handle,
            "registrationTimeSeconds": T0 - 100 * DAY,
            "lastOnlineTimeSeconds": T0,
            "contribution": 0,
            **fields,
        }
        self.submissions.setdefault(handle.lower(), [])
        self.ratings.setdefault(handle.lower(), [])

    def submit(
        self,
        handle: str,
        problem: dict,
        verdict: str = "OK",
        *,
        passed: int = 10,
        day: int = 0,
        participant: str = "PRACTICE",
        language: str = "C++17 (GCC 7-32)",
    ) -> dict:
        self._next_id += 1
        submission = {
            "id": self._next_id,
            "contestId": problem.get("contestId"),
            "creationTimeSeconds": T0 + day * DAY + self._next_id,
            "relativeTimeSeconds": 2147483647,
            "problem": problem,
            "author": {
                "contestId": problem.get("contestId"),
                "members": [{"handle": handle}],
                "participantType": participant,
                "ghost": False,
            },
            "programmingLanguage": language,
            "verdict": verdict,
            "testset": "TESTS",
            "passedTestCount": passed,
            "timeConsumedMillis": 46,
            "memoryConsumedBytes": 102_400,
        }
        # The live API returns submissions newest first.
        self.submissions[handle.lower()].insert(0, submission)
        return submission

    def rate(self, handle: str, contest_id: int, name: str, rank: int, old: int, new: int):
        self.ratings[handle.lower()].append(
            {
                "contestId": contest_id,
                "contestName": name,
                "handle": handle,
                "rank": rank,
                "ratingUpdateTimeSeconds": T0 + contest_id,
                "oldRating": old,
                "newRating": new,
            }
        )

    # -- transport -----------------------------------------------------------

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self._handle))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        method = urlparse(str(request.url)).path.rsplit("/", 1)[-1]
        params = dict(request.url.params)
        self.calls.append((method, params))
        if self.queued:
            return self.queued.pop(0)
        if method == "user.info":
            found = []
            for handle in params["handles"].split(";"):
                user = self.users.get(handle.lower())
                if user is None:
                    return failed(f"handles: User with handle {handle} not found", 400)
                found.append(user)
            return ok(found)
        handle = params.get("handle", "")
        if handle.lower() not in self.users:
            return failed(f"handle: User with handle {handle} not found", 400)
        if method == "user.status":
            return ok(self.submissions[handle.lower()])
        if method == "user.rating":
            return ok(self.ratings[handle.lower()])
        return failed(f"Unknown method {method}", 400)


def problem(contest_id, index, name, rating=800, tags=("math",)) -> dict:
    return {
        "contestId": contest_id,
        "index": index,
        "name": name,
        "type": "PROGRAMMING",
        "rating": rating,
        "tags": list(tags),
    }


def ok(result) -> httpx.Response:
    return httpx.Response(200, content=json.dumps({"status": "OK", "result": result}))


def failed(comment: str, status: int = 400) -> httpx.Response:
    return httpx.Response(status, content=json.dumps({"status": "FAILED", "comment": comment}))
