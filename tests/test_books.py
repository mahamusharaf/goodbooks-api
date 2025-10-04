import pytest
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)

def test_list_books(client):
    res = client.get("/books?page=1&page_size=3")
    assert res.status_code == 200
    data = res.json()
    assert "items" in data
    assert data["page"] == 1

def test_create_rating(client):
    payload = {"user_id": 9999, "book_id": 1, "rating": 5}
    res = client.post("/ratings", json=payload)
    assert res.status_code in (200,201)
    assert "message" in res.json()