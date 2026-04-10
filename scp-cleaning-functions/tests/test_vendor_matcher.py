"""Tests for vendor_matcher module."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.vendor_matcher import VendorMatcher


def test_vendor_matcher_exact_match():
    """Test exact vendor name matching."""
    vendor_master = [
        {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'},
        {'supplier_name': 'Komatsu Australia Pty Ltd', 'supplier_id': 'SUP-00002'}
    ]
    
    matcher = VendorMatcher(vendor_master)
    
    matched, confidence, candidates = matcher.match('Caterpillar Inc')
    
    assert matched is not None
    assert matched['supplier_id'] == 'SUP-00001'
    assert confidence >= 0.90


def test_vendor_matcher_fuzzy_match():
    """Test fuzzy vendor name matching with typo."""
    vendor_master = [
        {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'}
    ]
    
    matcher = VendorMatcher(vendor_master)
    
    matched, confidence, candidates = matcher.match('Caterpilalr Inc')
    
    assert matched is not None or confidence > 0.7
    assert len(candidates) > 0


def test_vendor_matcher_dictionary_lookup():
    """Test vendor dictionary instant resolution."""
    vendor_master = [
        {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'}
    ]
    
    vendor_dictionary = {
        'CAT INC': 'Caterpillar Inc'
    }
    
    matcher = VendorMatcher(vendor_master, vendor_dictionary)
    
    matched, confidence, candidates = matcher.match('CAT INC')
    
    assert matched is not None
    assert confidence == 1.0


def test_vendor_matcher_unknown():
    """Test handling of unknown vendor."""
    vendor_master = [
        {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'}
    ]
    
    matcher = VendorMatcher(vendor_master)
    
    matched, confidence, candidates = matcher.match('Completely Unknown Vendor XYZ')
    
    assert matched is None
    assert confidence < 0.90
