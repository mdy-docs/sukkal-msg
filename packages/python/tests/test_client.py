"""The client, against a real broker."""
from __future__ import annotations

import http.client
import threading

import pytest

from bjmsg import BjmsgError, Client
from bjmsg.binjson import decode

from .conftest import wait_for


def test_publish_returns_the_assigned_index(client):
    a = client.publish("t.pub", "first")
    b = client.publish("t.pub", {"n": 2})
    assert a["subject"] == "t.pub"
    assert a["index"] == 1
    assert b["index"] == 2


def test_round_trips_every_binjson_shape(client):
    got = []
    client.subscribe("t.shapes", lambda m: got.append(m.value), tail=True)

    sent = ["a string", 42, -1.5, True, False, None,
            {"nested": {"deep": [1, 2, 3]}},
            ["mixed", 1, None, {"k": "v"}]]
    for value in sent:
        client.publish("t.shapes", value)

    wait_for(lambda: len(got) == len(sent), 5,
             lambda: f"got {len(got)} of {len(sent)}: {got}")
    assert got == sent


def test_messages_arrive_pushed_in_order_with_metadata(client):
    seen = []
    client.subscribe("t.order", seen.append, tail=True)

    for i in range(1, 21):
        client.publish("t.order", i)
    wait_for(lambda: len(seen) == 20)

    assert [m.value for m in seen] == list(range(1, 21))
    assert [m.index for m in seen] == list(range(1, 21))
    assert seen[0].subject == "t.order"
    assert seen[0].headers is None


def test_delivery_is_prompt_not_polled(client):
    arrived = threading.Event()
    client.subscribe("t.fast", lambda m: arrived.set(), tail=True)

    import time
    time.sleep(0.1)
    sent = time.time()
    client.publish("t.fast", "now")
    assert arrived.wait(2), "nothing arrived"
    elapsed = (time.time() - sent) * 1000
    assert elapsed < 250, f"took {elapsed:.0f}ms — that smells like polling"


def test_headers_survive_as_an_envelope(client):
    got = []
    client.subscribe("t.hdr", got.append, tail=True)

    client.publish("t.hdr", "body", headers={"trace": "abc", "n": 7})
    wait_for(lambda: len(got) == 1)

    assert got[0].value == "body"
    assert got[0].headers == {"trace": "abc", "n": 7}


def test_wildcards_match_token_wise_and_carry_the_subject(client):
    got = []
    client.subscribe("eu.>", lambda m: got.append(f"{m.subject}={m.value}"),
                     tail=True)

    client.publish("eu.de", "hallo")
    client.publish("eu.fr.paris", "bonjour")
    client.publish("us.en", "hi")

    wait_for(lambda: len(got) == 2)
    import time
    time.sleep(0.2)                       # and no third
    assert sorted(got) == ["eu.de=hallo", "eu.fr.paris=bonjour"]


def test_a_named_consumer_resumes_where_it_left_off(client):
    client.publish("t.durable", "one")
    client.publish("t.durable", "two")

    first = []
    sub = client.subscribe("t.durable", lambda m: first.append(m.value),
                           consumer="reader")
    wait_for(lambda: len(first) == 2)
    sub.close()

    client.publish("t.durable", "three")

    second = []
    sub2 = client.subscribe("t.durable", lambda m: second.append(m.value),
                            consumer="reader")
    wait_for(lambda: len(second) == 1)
    assert second == ["three"]
    sub2.close()

    assert client.consumers("t.durable") == [
        {"consumer": "reader", "acked": 3, "lag": 0}
    ]


def test_a_throwaway_subscription_takes_its_position_with_it(client):
    client.publish("t.ephemeral", "x")
    sub = client.subscribe("t.ephemeral", lambda m: None, tail=True)
    sub.close()
    assert client.consumers("t.ephemeral") == []
    assert client.pushes() == []


def test_a_raising_handler_refuses_the_message_and_it_comes_back(client):
    state = {"attempts": 0, "pass": False}

    def handler(msg):
        state["attempts"] += 1
        if not state["pass"]:
            raise RuntimeError("not yet")
        assert msg.value == "work"

    client.subscribe("t.retry", handler, consumer="flaky")
    client.publish("t.retry", "work")

    wait_for(lambda: state["attempts"] >= 2, 5,
             lambda: f"only {state['attempts']} attempt(s)")
    # Refused, so nothing is acknowledged and the message is still owed.
    rows = client.consumers("t.retry")
    assert (rows[0]["acked"] if rows else 0) == 0

    state["pass"] = True
    wait_for(lambda: client.consumers("t.retry")[0]["acked"] == 1, 10,
             lambda: f"never acked after {state['attempts']} attempt(s)")


def test_idempotent_publish_collapses_a_repeat(client):
    a = client.publish("t.idem", "once", id="key-1")
    b = client.publish("t.idem", "once", id="key-1")
    assert a["duplicate"] is False
    assert b["duplicate"] is True
    assert b["index"] == a["index"]
    assert client.info("t.idem")["messages"] == 1


def test_queue_groups_run_each_job_once_across_workers(client, make_client):
    ran = []
    lock = threading.Lock()

    def record(who):
        def handler(job):
            with lock:
                ran.append((who, job.value))
        return handler

    make_client().work("t.jobs", record("a"), group="crew")
    make_client().work("t.jobs", record("b"), group="crew")

    for i in range(1, 13):
        client.publish("t.jobs", i)

    wait_for(lambda: len(ran) == 12, 10, lambda: f"only {len(ran)} of 12 ran")
    import time
    time.sleep(0.2)

    assert len(ran) == 12, "no job ran twice"
    assert sorted(v for _, v in ran) == list(range(1, 13))
    assert {who for who, _ in ran} == {"a", "b"}, "both workers took a share"


def test_a_failing_job_comes_back_and_dead_letters(client, make_client):
    client.configure_queue("t.poison", "p", max_attempts=2, backoff_ms=1,
                           lease_ms=2000)
    client.publish("t.poison", "bad")

    tries = {"n": 0}

    def explode(job):
        tries["n"] += 1
        raise RuntimeError("nope")

    make_client().work("t.poison", explode, group="p")

    wait_for(lambda: len(client.dead("t.poison")) == 1, 25,
             lambda: f"{tries['n']} attempt(s), nothing dead-lettered")
    assert tries["n"] == 2

    envelope = decode(client.dead("t.poison")[0]["payload"])
    assert envelope["subject"] == "t.poison"
    assert envelope["group"] == "p"


def test_an_empty_dead_letter_channel_is_empty_not_missing(client):
    client.publish("t.nodead", "fine")
    assert client.dead("t.nodead") == []


def test_request_reply_matches_on_correlation(client, make_client):
    make_client().reply("t.rpc", lambda m: str(m.value).upper())

    assert client.request("t.rpc", "hello") == "HELLO"

    # Concurrent requesters share one reply subject; each must get its
    # own answer back.
    answers = {}
    def ask(word):
        answers[word] = client.request("t.rpc", word, timeout=10)

    threads = [threading.Thread(target=ask, args=(w,))
               for w in ("one", "two", "three", "four")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)
    assert answers == {"one": "ONE", "two": "TWO",
                       "three": "THREE", "four": "FOUR"}


def test_request_times_out_when_nobody_answers(client):
    with pytest.raises(BjmsgError, match="within 0.3s"):
        client.request("t.silent", "anyone?", timeout=0.3)


def test_pipe_transforms_one_subject_into_another(client, make_client):
    out = []
    for word in ("alpha", "beta"):
        client.publish("t.raw", word)

    stage = make_client()
    stage.pipe("t.raw", lambda m: str(m.value).upper(),
               to="t.upper", consumer="stage1")
    client.subscribe("t.upper", lambda m: out.append(m.value))

    client.publish("t.raw", "gamma")
    wait_for(lambda: len(out) == 3, 5, lambda: str(out))
    assert out == ["ALPHA", "BETA", "GAMMA"]

    # Restarting the stage must not republish what it already did.
    stage.close()
    again = make_client()
    again.pipe("t.raw", lambda m: str(m.value).upper(),
               to="t.upper", consumer="stage1")
    client.publish("t.raw", "delta")
    wait_for(lambda: len(out) == 4)
    import time
    time.sleep(0.3)
    assert out == ["ALPHA", "BETA", "GAMMA", "DELTA"]


def test_a_dropped_message_still_acknowledges_the_input(client, make_client):
    client.publish("t.filter", "keep")
    client.publish("t.filter", "drop")
    client.publish("t.filter", "keep2")

    make_client().pipe("t.filter",
                       lambda m: None if m.value == "drop" else m.value,
                       to="t.kept", consumer="filt")

    wait_for(lambda: client.info("t.kept")["messages"] == 2, 5)
    wait_for(lambda: client.consumers("t.filter")[0]["acked"] == 3, 5)


def test_errors_carry_the_brokers_own_explanation(client):
    with pytest.raises(BjmsgError) as caught:
        client.info("no.such.subject")
    assert caught.value.status == 404

    with pytest.raises(BjmsgError, match="invalid subject"):
        client.publish("not a valid subject", "x")


def test_retention_and_trimming(client):
    for i in range(1, 11):
        client.publish("t.trim", i)
    assert client.trim("t.trim", keep=3)["removed"] == 7
    assert client.info("t.trim")["messages"] == 3

    client.set_policy("t.trim", max_messages=2)
    assert client.policy("t.trim")["max_messages"] == 2


def test_one_client_many_subscriptions_one_flask_app(client):
    seen = {"a": 0, "b": 0, "c": 0}
    for name in seen:
        client.subscribe(f"multi.{name}",
                         lambda m, n=name: seen.__setitem__(n, seen[n] + 1),
                         tail=True)

    for name in seen:
        client.publish(f"multi.{name}", 1)
    wait_for(lambda: all(seen.values()))

    assert len(client.pushes()) == 3
    assert len(client.receiver) == 3


def test_a_delivery_without_the_token_is_refused(client):
    client.subscribe("t.auth", lambda m: None, consumer="guarded")
    callback = next(p["callback"] for p in client.pushes()
                    if p["consumer"] == "guarded")

    from urllib.parse import urlsplit
    parts = urlsplit(callback)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=5)
    conn.request("POST", parts.path, body=b"\x05\x01\x00\x00\x00x",
                 headers={"Content-Type": "application/binjson"})
    assert conn.getresponse().status == 401
    conn.close()


def test_a_subscription_is_a_context_manager(client):
    got = []
    with client.subscribe("t.ctx", lambda m: got.append(m.value), tail=True):
        client.publish("t.ctx", "inside")
        wait_for(lambda: got == ["inside"])
    assert client.pushes() == []
