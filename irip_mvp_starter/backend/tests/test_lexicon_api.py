from app.db.repository import ReviewRepository


def test_list_lexicon_entries_from_seed(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")

    entries = repo.list_lexicon_entries(limit=10)

    assert len(entries) > 0
    assert "term" in entries[0]
    assert "normalized_term" in entries[0]
    assert "confidence" in entries[0]


def test_list_lexicon_entries_with_search(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")

    entries = repo.list_lexicon_entries(search="mast", limit=10)

    assert any(item["term"] == "mast" for item in entries)