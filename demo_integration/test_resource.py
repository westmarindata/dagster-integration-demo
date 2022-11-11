def test_resource():
    from .resources import MyResource

    r = MyResource("abc-123")
    resp = r.run_and_poll("proj-123")
    assert resp
