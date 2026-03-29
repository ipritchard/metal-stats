"""Tests for Phase 0 band deduplication."""

from pathlib import Path

from scraper.pipeline import phase0_dedup

CSV_PATH = Path("data/bandlist.csv")


def test_dedup_returns_fewer_rows_than_csv() -> None:
    """Deduplicated bands should be fewer than album-level rows."""
    import pandas as pd

    raw = pd.read_csv(CSV_PATH)
    bands = phase0_dedup(CSV_PATH)
    assert len(bands) < len(raw)
    assert len(bands) > 0


def test_dedup_columns() -> None:
    """Output should have expected columns."""
    bands = phase0_dedup(CSV_PATH)
    assert "Band" in bands.columns
    assert "Band Origin" in bands.columns
    assert "album_count" in bands.columns
    assert "best_score" in bands.columns


def test_dedup_no_duplicate_bands() -> None:
    """Each band name should appear exactly once."""
    bands = phase0_dedup(CSV_PATH)
    assert bands["Band"].is_unique


def test_dedup_album_counts_positive() -> None:
    """Every band should have at least one album."""
    bands = phase0_dedup(CSV_PATH)
    assert (bands["album_count"] >= 1).all()
