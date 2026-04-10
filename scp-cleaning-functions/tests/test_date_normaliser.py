"""Tests for date_normaliser module."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.date_normaliser import normalise_date


def test_normalise_date_iso_format():
    """Test ISO format date (already clean)."""
    result, confidence = normalise_date('2025-01-15')
    
    assert result == '2025-01-15'
    assert confidence == 1.0


def test_normalise_date_dd_mm_yyyy():
    """Test DD/MM/YYYY format (AU standard)."""
    result, confidence = normalise_date('15/01/2025', locale_hint='AU')
    
    assert result == '2025-01-15'
    assert confidence > 0.0


def test_normalise_date_d_mon_yy():
    """Test D-Mon-YY format."""
    result, confidence = normalise_date('15-Jan-25', locale_hint='AU')
    
    assert result is not None
    assert '2025' in result
    assert confidence > 0.0


def test_normalise_date_yyyymmdd():
    """Test YYYYMMDD format (no separators)."""
    result, confidence = normalise_date('20250115')
    
    assert result == '2025-01-15'
    assert confidence == 1.0


def test_normalise_date_with_time():
    """Test date with time component (should strip time)."""
    result, confidence = normalise_date('2025-01-15 14:30:00')
    
    assert result == '2025-01-15'
    assert confidence > 0.0


def test_normalise_date_ambiguous():
    """Test ambiguous date (03/04/2025 could be Mar 4 or Apr 3)."""
    result, confidence = normalise_date('03/04/2025', locale_hint='AU')
    
    assert result is not None
    assert confidence <= 1.0


def test_normalise_date_missing_year():
    """Test date with missing year (should return None)."""
    result, confidence = normalise_date('15/01')
    
    assert result is None
    assert confidence == 0.0


def test_normalise_date_invalid():
    """Test invalid date string."""
    result, confidence = normalise_date('not a date')
    
    assert result is None
    assert confidence == 0.0


def test_normalise_date_excel_serial():
    """Test Excel serial date."""
    result, confidence = normalise_date(45678)
    
    assert result is not None
    assert confidence == 1.0
